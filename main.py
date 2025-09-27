import os
import re
import io
import copy
import base64
import binascii
import tempfile
import zipfile
import shutil
import subprocess
import threading
import xml.etree.ElementTree as ET
from decimal import Decimal
from lxml import etree as LET
from jinja2 import Environment, DebugUndefined
from typing import Dict, Tuple, List, Optional, Set
import urllib.parse

import requests
from requests.adapters import HTTPAdapter

from fastapi import FastAPI, UploadFile, File, Form, Header
from fastapi.responses import JSONResponse

DEFAULT_AUTH_API_BASE_URL = "https://auth-677366504119.asia-northeast1.run.app"

_AUTH_SESSION_LOCAL = threading.local()


def _get_auth_session() -> requests.Session:
    session = getattr(_AUTH_SESSION_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=8, pool_maxsize=8)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _AUTH_SESSION_LOCAL.session = session
    return session

app = FastAPI(title="Doc/Excel → PDF & JPEG API (stable)")

# ------------ light helpers ------------
def file_ext_lower(name: str) -> str:
    return os.path.splitext(name)[1].lower()

def data_uri(mime: str, data: bytes) -> str:
    import base64 as _b64
    b64 = _b64.b64encode(data).decode("ascii")
    return f"data:{mime};base64,{b64}"

def run(cmd: List[str], cwd: Optional[str] = None):
    proc = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\n"
            f"STDOUT:\n{proc.stdout.decode('utf-8', 'ignore')}\n"
            f"STDERR:\n{proc.stderr.decode('utf-8', 'ignore')}"
        )
    return proc

def ok(d): return JSONResponse(status_code=200, content=d)
def err(m, status=400): return JSONResponse(status_code=status, content={"error": str(m)})


def _auth_api_timeout() -> float:
    try:
        return float(os.environ.get("AUTH_API_TIMEOUT", "5"))
    except ValueError:
        return 5.0


def validate_auth_id(auth_id: str) -> bool:
    base_url = (os.environ.get("AUTH_API_BASE_URL") or DEFAULT_AUTH_API_BASE_URL or "").rstrip("/")
    if not base_url:
        raise RuntimeError("AUTH_API_BASE_URL is not configured")
    if not auth_id:
        return False

    url = f"{base_url}/auth-ids/verify"
    payload = {"auth_id": auth_id}
    session = _get_auth_session()
    try:
        resp = session.post(url, json=payload, timeout=_auth_api_timeout())
    except requests.RequestException as exc:
        raise RuntimeError(f"auth_id validation request failed: {exc}")

    if resp.status_code in {401, 404}:
        return False
    if resp.status_code >= 400:
        raise RuntimeError(f"auth_id validation failed with status {resp.status_code}")

    try:
        data = resp.json()
    except ValueError as exc:
        raise RuntimeError("auth_id validation returned invalid JSON") from exc

    return bool(data.get("is_valid"))

# ------------ tag & number parsing ------------
VAR_NAME = r"[A-Za-z_][A-Za-z0-9_]*"
VAR_PATH = r"[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*"
IMG_KEY_PATTERN = re.compile(rf"^\{{\[(?P<var>{VAR_NAME})\](?::(?P<size>[^}}]+))?\}}$")
TXT_KEY_PATTERN = re.compile(rf"^\{{(?P<var>{VAR_NAME})\}}$")
TEXT_TOKEN_PATTERN = re.compile(rf"\{{\s*(?P<var>{VAR_NAME})\s*\}}")
LOOP_KEY_PATTERN = re.compile(rf"^\{{(?P<group>{VAR_NAME}):loop:(?P<field>{VAR_NAME})\}}$")
IMG_TAG_PATTERN = re.compile(rf"\{{\[(?P<var>{VAR_NAME})\](?::(?P<size>[^}}]+))?\}}")
WORD_INLINE_PATTERN = re.compile(
    rf"(?<!\{{)\{{\s*(?:\[\s*(?P<img>{VAR_NAME})\s*\](?::(?P<size>[^}}]+))?|(?P<txt>{VAR_PATH}))\s*\}}(?!\}})"
)
MM_RE      = re.compile(r'^\s*(\d+(?:\.\d+)?)\s*mm\s*$', re.IGNORECASE)
NUM_PLAIN  = re.compile(r'^\s*-?\d+(?:\.\d+)?\s*$')
NUM_COMMA  = re.compile(r'^\s*-?\d{1,3}(?:,\d{3})+(?:\.\d+)?\s*$')
NUM_PCT    = re.compile(r'^\s*-?\d+(?:\.\d+)?\s*%\s*$')

def parse_size_mm(s: Optional[str]) -> Optional[float]:
    if not s: return None
    m = MM_RE.match(s.strip()); return float(m.group(1)) if m else None

def parse_image_tag(text: Optional[str]) -> Tuple[Optional[str], Optional[float]]:
    if not text: return None, None
    m = IMG_TAG_PATTERN.fullmatch(text.strip())
    if not m: return None, None
    return m.group("var"), parse_size_mm(m.group("size") or "")

def parse_numberlike(s: str) -> Tuple[Optional[float], Optional[str]]:
    if s is None: return None, None
    st = s.strip()
    if NUM_PCT.match(st):
        num = st.replace("%", "").strip()
        try:
            v = float(num.replace(",", "")) / 100.0
            fmt = "0.00%" if "." in num else "0%"
            return v, fmt
        except: return None, None
    if NUM_COMMA.match(st) or NUM_PLAIN.match(st):
        try:
            c = st.replace(",", "") if "," in st else st
            return (float(c), None) if "." in c else (int(c), None)
        except: return None, None
    return None, None

def _format_formula_value(value) -> Tuple[Optional[str], Optional[str]]:
    """Return the serialized value and Excel type hint for a formula result."""
    if value is None:
        return None, None

    type_hint: Optional[str] = None

    if isinstance(value, Decimal):
        if value == value.to_integral():
            value = int(value)
        else:
            value = float(value)

    if isinstance(value, bool):
        return ("1" if value else "0"), "b"

    if isinstance(value, (int, float)):
        return str(value), None

    text = str(value)
    type_hint = "str"
    return text, type_hint

def _with_newlines(v: str) -> str:
    return (v or "").replace("<br>", "\n")

def parse_mapping_text(raw: str) -> Tuple[Dict[str, str], Dict[str, Dict], Dict[str, List[Dict[str, str]]]]:
    """
    {a}:X,{b}:Y でも 改行でもOK。URL内カンマ保護。
    {[img]:50mm}:URL / {[img]}:URL
    """
    text_map: Dict[str, str] = {}
    image_map: Dict[str, Dict] = {}
    loop_map: Dict[str, List[Dict[str, str]]] = {}
    if not raw: return text_map, image_map, loop_map

    SAFE = "\u241B"  # protect ://
    protected = [line.replace("://", SAFE) for line in raw.splitlines()]
    joined = "\n".join(protected)

    items: List[str] = []
    for line in joined.splitlines():
        if not line.strip(): continue
        for seg in line.split(","):
            seg = seg.strip()
            if seg: items.append(seg.replace(SAFE, "://"))

    loop_values: Dict[str, Dict[str, List[str]]] = {}

    for seg in items:
        if "}" not in seg:
            continue
        close = seg.find("}")
        key = seg[:close+1].strip()
        value = seg[close+1:].lstrip(":").strip()
        if not key:
            continue

        value_processed = _with_newlines(value)

        m_img = IMG_KEY_PATTERN.match(key)
        if m_img:
            v = m_img.group("var"); mm = parse_size_mm(m_img.group("size") or "")
            image_map[v] = {"url": value, "mm": mm}; continue

        m_loop = LOOP_KEY_PATTERN.match(key)
        if m_loop:
            group = m_loop.group("group")
            field = m_loop.group("field")
            segments = value_processed.splitlines() or [""]
            store = loop_values.setdefault(group, {}).setdefault(field, [])
            store.extend(segments)
            continue

        m_txt = TXT_KEY_PATTERN.match(key)
        if m_txt:
            v = m_txt.group("var"); text_map[v] = value_processed; continue

    for group, fields in loop_values.items():
        lengths = [len(vals) for vals in fields.values() if vals]
        max_len = max(lengths) if lengths else 0
        rows: List[Dict[str, str]] = []
        for idx in range(max_len):
            row: Dict[str, str] = {}
            for field, vals in fields.items():
                row[field] = vals[idx] if idx < len(vals) else ""
            if row:
                rows.append(row)
        if rows:
            loop_map[group] = rows

    return text_map, image_map, loop_map

def _apply_text_tokens(text: Optional[str], text_map: Dict[str, str]) -> Optional[str]:
    if text is None or not text_map:
        return text

    def _replace(match: re.Match[str]) -> str:
        var = match.group("var")
        if var in text_map:
            return text_map[var]
        return match.group(0)

    return TEXT_TOKEN_PATTERN.sub(_replace, text)

def mm_to_pixels(mm: float, dpi: int = 96) -> int:
    return int(round(mm / 25.4 * dpi))

def parse_pages_arg(pages: str, total_pages: int) -> List[int]:
    pages = (pages or "1").strip()
    out: List[int] = []
    for part in pages.split(","):
        part = part.strip()
        if not part: continue
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                s = max(1, int(a)); e = min(total_pages, int(b))
                if s <= e: out.extend(range(s, e+1))
            except: pass
        else:
            try:
                p = int(part)
                if 1 <= p <= total_pages: out.append(p)
            except: pass
    return sorted(list(dict.fromkeys(out))) or [1]

# ------------ Word (.docx) ------------
WORD_XML_TARGETS = ("word/document.xml","word/footnotes.xml","word/endnotes.xml","word/comments.xml")
W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
S_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
R_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
WP_NS = "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
A_NS = "http://schemas.openxmlformats.org/drawingml/2006/main"
PIC_NS = "http://schemas.openxmlformats.org/drawingml/2006/picture"
XML_NS = "http://www.w3.org/XML/1998/namespace"
XMLNS_NS = "http://www.w3.org/2000/xmlns/"
CONTENT_TYPES_NS = "http://schemas.openxmlformats.org/package/2006/content-types"
EMU_PER_INCH = 914400
XDR_NS = "http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing"
EMU_PER_PIXEL = 9525
CALC_CHAIN_REL_TYPE = f"{R_NS}/calcChain"
CALC_CHAIN_PART = "/xl/calcChain.xml"

DOC_MIME_MAP = {
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

# Ensure common OpenXML namespaces retain stable prefixes when serializing.
ET.register_namespace("r", R_NS)
ET.register_namespace("xdr", XDR_NS)
ET.register_namespace("a", A_NS)
ET.register_namespace("s", S_NS)
CELL_REF_RE = re.compile(r"^([A-Za-z]+)(\d+)$")


def _xml_collect_namespaces(path: str) -> Dict[str, str]:
    namespaces: Dict[str, str] = {}
    if not os.path.exists(path):
        return namespaces
    try:
        for _, (prefix, uri) in ET.iterparse(path, events=("start-ns",)):
            if uri == XMLNS_NS:
                continue
            if prefix in namespaces:
                continue
            namespaces[prefix or ""] = uri
    except Exception:
        return namespaces
    return namespaces


def _xml_parse_tree(path: str) -> Tuple[ET.ElementTree, Dict[str, str]]:
    namespaces = _xml_collect_namespaces(path)
    tree = ET.parse(path)
    return tree, namespaces


def _xml_write_tree(
    tree: ET.ElementTree,
    path: str,
    default_namespace: Optional[str] = None,
    namespace_map: Optional[Dict[str, str]] = None,
):
    kwargs = {"encoding": "utf-8", "xml_declaration": True}
    root: Optional[ET.Element] = None
    try:
        root = tree.getroot()
    except Exception:
        root = None

    if namespace_map and root is not None:
        for prefix, uri in namespace_map.items():
            if prefix in {"xml", "xmlns"}:
                continue
            if prefix:
                attr = f"{{{XMLNS_NS}}}{prefix}"
                if root.get(attr) != uri:
                    root.set(attr, uri)
                try:
                    ET.register_namespace(prefix, uri)
                except ValueError:
                    pass
        if not default_namespace and "" in namespace_map:
            default_namespace = namespace_map.get("")

    if default_namespace:
        missing: List[ET.Element] = []
        if root is not None:
            for node in root.iter():
                tag = getattr(node, "tag", None)
                if not isinstance(tag, str):
                    continue
                if not tag.startswith("{"):
                    missing.append(node)

        if missing:
            for node in missing:
                node.tag = f"{{{default_namespace}}}{node.tag}"
            kwargs["default_namespace"] = default_namespace
    buffer = io.BytesIO()
    try:
        tree.write(buffer, **kwargs)
    except ValueError as exc:
        if "non-qualified names" in str(exc) and kwargs.pop("default_namespace", None):
            buffer = io.BytesIO()
            tree.write(buffer, **kwargs)
        else:
            raise

    xml_text = buffer.getvalue().decode("utf-8")

    if namespace_map and root is not None:
        insertions: List[str] = []
        root_start = xml_text.find("<")
        while root_start != -1 and root_start + 1 < len(xml_text) and xml_text[root_start + 1] in {"?", "!"}:
            root_start = xml_text.find("<", root_start + 1)
        root_end = xml_text.find(">", root_start if root_start != -1 else 0)
        if root_start != -1 and root_end != -1:
            root_tag = xml_text[root_start:root_end]
            alias_pattern = re.compile(rf"\sxmlns:(ns\d+)=\"{re.escape(XMLNS_NS)}\"")
            aliases = set(alias_pattern.findall(root_tag))
            for alias in aliases:
                root_tag = re.sub(rf"\sxmlns:{alias}=\"{re.escape(XMLNS_NS)}\"", "", root_tag)
                root_tag = re.sub(rf"(\s){alias}:", r"\1xmlns:", root_tag)
            if aliases:
                xml_text = xml_text[:root_start] + root_tag + xml_text[root_end:]
                root_end = xml_text.find(">", root_start)
                root_tag = xml_text[root_start:root_end]

            decl_pattern = re.compile(r"\sxmlns(?::([A-Za-z0-9_.\-]+))?=\"([^\"]+)\"")
            seen_decl: Set[Tuple[str, str]] = set()

            def _dedup_decl(match: re.Match[str]) -> str:
                prefix = match.group(1) or ""
                uri = match.group(2)
                key = (prefix, uri)
                if key in seen_decl:
                    return ""
                seen_decl.add(key)
                return match.group(0)

            root_tag = decl_pattern.sub(_dedup_decl, root_tag)
            if seen_decl:
                xml_text = xml_text[:root_start] + root_tag + xml_text[root_end:]
                root_end = xml_text.find(">", root_start)
                root_tag = xml_text[root_start:root_end]

            existing_decls: Dict[str, str] = {}
            for match in re.findall(r"xmlns(?::([A-Za-z0-9_.\-]+))?=\"([^\"]+)\"", root_tag):
                prefix, uri = match
                existing_decls[prefix or ""] = uri

            default_uri = namespace_map.get("")
            if default_uri and existing_decls.get("", "") != default_uri:
                insertions.append(f' xmlns="{default_uri}"')
            for prefix, uri in namespace_map.items():
                if prefix in {"", "xml", "xmlns"}:
                    continue
                if existing_decls.get(prefix) == uri:
                    continue
                insertions.append(f" xmlns:{prefix}=\"{uri}\"")
            if insertions:
                insert_pos = root_end
                if xml_text[insert_pos - 1] == "/":
                    insert_pos -= 1
                xml_text = xml_text[:insert_pos] + "".join(insertions) + xml_text[insert_pos:]

    with open(path, "w", encoding="utf-8") as f:
        f.write(xml_text)

def _word_set_text(node: LET._Element, text: str):
    run = node.getparent()
    if run is None or run.tag != f"{{{W_NS}}}r":
        node.text = text
        return

    children = list(run)
    try:
        idx = children.index(node)
    except ValueError:
        idx = -1

    # remove text/break nodes after the current text node so we can rebuild
    if idx >= 0:
        for child in children[idx + 1:]:
            if child.tag in {f"{{{W_NS}}}t", f"{{{W_NS}}}br"}:
                run.remove(child)

    parts = text.split("\n")
    first = parts[0] if parts else ""
    node.text = first
    if first.strip() != first or "\n" in first or first == "":
        node.set(f"{{{XML_NS}}}space", "preserve")
    else:
        node.attrib.pop(f"{{{XML_NS}}}space", None)

    for part in parts[1:]:
        br = LET.Element(f"{{{W_NS}}}br")
        run.append(br)
        t = LET.Element(f"{{{W_NS}}}t")
        t.set(f"{{{XML_NS}}}space", "preserve")
        t.text = part
        run.append(t)

def _word_snapshot(root) -> Tuple[List[Tuple[LET._Element, int, int]], str]:
    nodes: List[Tuple[LET._Element, int, int]] = []
    cursor = 0
    parts: List[str] = []
    for t in root.iter(f"{{{W_NS}}}t"):
        text = t.text or ""
        start = cursor
        cursor += len(text)
        nodes.append((t, start, cursor))
        parts.append(text)
    return nodes, "".join(parts)

def _word_splice_text(nodes: List[Tuple[LET._Element, int, int]], start: int, end: int, replacement: str, preserve: bool = False):
    inserted = False
    for node, node_start, node_end in nodes:
        if node_end <= start or node_start >= end:
            continue
        text = node.text or ""
        local_start = max(0, start - node_start)
        local_end = min(len(text), end - node_start)
        before = text[:local_start]
        after = text[local_end:]
        new_value = before + replacement + after
        if not inserted:
            if preserve:
                _word_set_text(node, new_value)
            else:
                node.text = new_value
            inserted = True
        else:
            remainder = before + after
            if preserve:
                _word_set_text(node, remainder)
            else:
                node.text = remainder

def _word_convert_placeholders(
    root,
    size_hints: Dict[str, Optional[float]],
    loop_map: Dict[str, List[Dict[str, str]]],
):
    loop_vars: Dict[str, str] = {}
    loop_counter = [0]

    def _loop_var(group: str) -> str:
        if group not in loop_vars:
            loop_counter[0] += 1
            loop_vars[group] = f"__loop_{loop_counter[0]}_{group}"
        return loop_vars[group]

    def _placeholder_to_jinja(expr: str) -> str:
        expr_clean = (expr or "").strip()
        if not expr_clean:
            return ""
        parts = [p.strip() for p in expr_clean.split(":") if p.strip()]
        if not parts:
            return ""
        if len(parts) >= 2 and parts[1] == "loop":
            group = parts[0]
            var_name = _loop_var(group)
            if len(parts) == 2:
                return f"{{% for {var_name} in loops.get('{group}', []) %}}"
            field = parts[2] if len(parts) > 2 else ""
            if not field:
                return ""
            return f"{{{{ {var_name}.get('{field}', '') }}}}"
        group = parts[0]
        if group in loop_vars or group in loop_map:
            var_name = _loop_var(group)
            field = parts[1] if len(parts) > 1 else ""
            if not field:
                return f"{{{{ {var_name} }}}}"
            return f"{{{{ {var_name}.get('{field}', '') }}}}"
        return f"{{{{ {expr_clean} }}}}"

    while True:
        nodes, full_text = _word_snapshot(root)
        if not nodes:
            break
        match = WORD_INLINE_PATTERN.search(full_text)
        if not match:
            break
        var = match.group("img") or match.group("txt")
        if not var:
            break
        if match.group("img"):
            size = parse_size_mm(match.group("size") or "")
            if size is not None:
                size_hints.setdefault(var, size)
        replacement = _placeholder_to_jinja(match.group("txt")) if match.group("txt") else f"{{{{ {var} }}}}"
        _word_splice_text(nodes, match.start(), match.end(), replacement)

    while True:
        nodes, full_text = _word_snapshot(root)
        if not nodes:
            break
        idx = full_text.find("#end")
        if idx == -1:
            break
        _word_splice_text(nodes, idx, idx + 4, "{% endfor %}")

WORD_JINJA_PATTERN = re.compile(r"\{\{\s*(?P<var>%s)\s*\}\}" % VAR_NAME)

def _word_apply_text_map(root, text_map: Dict[str, str]):
    if not text_map:
        return
    while True:
        nodes, full_text = _word_snapshot(root)
        if not nodes:
            break
        target = None
        for m in WORD_JINJA_PATTERN.finditer(full_text):
            var = m.group("var")
            if var in text_map:
                target = (m.start(), m.end(), text_map[var])
                break
        if not target:
            break
        start, end, replacement = target
        _word_splice_text(nodes, start, end, replacement, preserve=True)

def _word_part_rels_path(xml_path: str) -> str:
    base = os.path.basename(xml_path)
    rels_dir = os.path.join(os.path.dirname(xml_path), "_rels")
    os.makedirs(rels_dir, exist_ok=True)
    return os.path.join(rels_dir, base + ".rels")

def _word_load_rels_tree(path: str) -> LET._ElementTree:
    if os.path.exists(path):
        return LET.parse(path)
    root = LET.Element(f"{{{REL_NS}}}Relationships")
    return LET.ElementTree(root)

def _word_max_docpr_id(root) -> int:
    max_id = 0
    for el in root.iter(f"{{{WP_NS}}}docPr"):
        try:
            max_id = max(max_id, int(el.get("id", "0")))
        except Exception:
            continue
    return max_id

def _word_next_docpr(counter: List[int]) -> int:
    counter[0] += 1
    return counter[0]

def _word_add_image_relationship(rels_root: LET._Element, target: str) -> Tuple[str, LET._Element]:
    rid_max = 0
    for rel in rels_root.findall(f"{{{REL_NS}}}Relationship"):
        rid = rel.get("Id", "")
        if rid.startswith("rId") and rid[3:].isdigit():
            rid_max = max(rid_max, int(rid[3:]))
    new_id = f"rId{rid_max + 1}"
    rel = LET.SubElement(rels_root, f"{{{REL_NS}}}Relationship")
    rel.set("Id", new_id)
    rel.set("Type", f"{R_NS}/image")
    rel.set("Target", target)
    return new_id, rel

def _word_make_inline_drawing(rid: str, docpr_id: int, cx: int, cy: int) -> LET._Element:
    drawing = LET.Element(f"{{{W_NS}}}drawing")
    inline = LET.SubElement(drawing, f"{{{WP_NS}}}inline")
    for key in ("distT", "distB", "distL", "distR"):
        inline.set(key, "0")
    extent = LET.SubElement(inline, f"{{{WP_NS}}}extent")
    extent.set("cx", str(max(cx, 1)))
    extent.set("cy", str(max(cy, 1)))
    effect = LET.SubElement(inline, f"{{{WP_NS}}}effectExtent")
    for key in ("l", "t", "r", "b"):
        effect.set(key, "0")
    docpr = LET.SubElement(inline, f"{{{WP_NS}}}docPr")
    docpr.set("id", str(docpr_id))
    docpr.set("name", f"Picture {docpr_id}")
    cNvGraphic = LET.SubElement(inline, f"{{{WP_NS}}}cNvGraphicFramePr")
    locks = LET.SubElement(cNvGraphic, f"{{{A_NS}}}graphicFrameLocks")
    locks.set("noChangeAspect", "1")
    graphic = LET.SubElement(inline, f"{{{A_NS}}}graphic")
    graphic_data = LET.SubElement(graphic, f"{{{A_NS}}}graphicData")
    graphic_data.set("uri", PIC_NS)
    pic = LET.SubElement(graphic_data, f"{{{PIC_NS}}}pic")
    nv_pic = LET.SubElement(pic, f"{{{PIC_NS}}}nvPicPr")
    cNvPr = LET.SubElement(nv_pic, f"{{{PIC_NS}}}cNvPr")
    cNvPr.set("id", "0")
    cNvPr.set("name", f"Picture {docpr_id}")
    cNvPicPr = LET.SubElement(nv_pic, f"{{{PIC_NS}}}cNvPicPr")
    pic_locks = LET.SubElement(cNvPicPr, f"{{{A_NS}}}picLocks")
    pic_locks.set("noChangeAspect", "1")
    pic_locks.set("noChangeArrowheads", "1")
    blip_fill = LET.SubElement(pic, f"{{{PIC_NS}}}blipFill")
    blip = LET.SubElement(blip_fill, f"{{{A_NS}}}blip")
    blip.set(f"{{{R_NS}}}embed", rid)
    stretch = LET.SubElement(blip_fill, f"{{{A_NS}}}stretch")
    LET.SubElement(stretch, f"{{{A_NS}}}fillRect")
    sp_pr = LET.SubElement(pic, f"{{{PIC_NS}}}spPr")
    xfrm = LET.SubElement(sp_pr, f"{{{A_NS}}}xfrm")
    off = LET.SubElement(xfrm, f"{{{A_NS}}}off")
    off.set("x", "0")
    off.set("y", "0")
    ext = LET.SubElement(xfrm, f"{{{A_NS}}}ext")
    ext.set("cx", str(max(cx, 1)))
    ext.set("cy", str(max(cy, 1)))
    prst = LET.SubElement(sp_pr, f"{{{A_NS}}}prstGeom")
    prst.set("prst", "rect")
    LET.SubElement(prst, f"{{{A_NS}}}avLst")
    return drawing

def _word_content_xmls(extracted_dir: str) -> List[str]:
    targets = list(WORD_XML_TARGETS)
    wdir = os.path.join(extracted_dir, "word")
    if os.path.isdir(wdir):
        for fn in os.listdir(wdir):
            if fn.startswith("header") and fn.endswith(".xml"): targets.append(f"word/{fn}")
            if fn.startswith("footer") and fn.endswith(".xml"): targets.append(f"word/{fn}")
    return [os.path.join(extracted_dir, p) for p in targets if os.path.exists(os.path.join(extracted_dir, p))]

def docx_convert_tags_to_jinja(
    in_docx: str,
    out_docx: str,
    loop_map: Optional[Dict[str, List[Dict[str, str]]]] = None,
) -> Dict[str, Optional[float]]:
    # {var}/{[var]} → {{ var }} へ。英数字+下線のタグのみ変換（Jinja誤爆防止）
    tmpdir = tempfile.mkdtemp()
    size_hints: Dict[str, Optional[float]] = {}
    try:
        with zipfile.ZipFile(in_docx, 'r') as zin:
            zin.extractall(tmpdir)
        for p in _word_content_xmls(tmpdir):
            parser = LET.XMLParser(remove_blank_text=False)
            tree = LET.parse(p, parser)
            root = tree.getroot()
            _word_convert_placeholders(root, size_hints, loop_map or {})
            tree.write(p, encoding="utf-8", xml_declaration=True)

        with zipfile.ZipFile(out_docx, 'w', zipfile.ZIP_DEFLATED) as zout:
            for root, _, files in os.walk(tmpdir):
                for fn in files:
                    full = os.path.join(root, fn)
                    zout.write(full, os.path.relpath(full, tmpdir))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    return size_hints

def _word_replace_placeholder_with_drawing(root, nodes: List[Tuple[LET._Element, int, int]], start: int, end: int, drawing: LET._Element) -> bool:

    if not nodes:
        return False
    affected_runs: List[LET._Element] = []
    for node, node_start, node_end in nodes:
        if node_end <= start or node_start >= end:
            continue
        run = node.getparent()
        if run is None or run.tag != f"{{{W_NS}}}r":
            continue
        if run not in affected_runs:
            affected_runs.append(run)
        text = node.text or ""
        local_start = max(0, start - node_start)
        local_end = min(len(text), end - node_start)
        new_text = text[:local_start] + text[local_end:]
        if new_text:
            _word_set_text(node, new_text)
        else:
            run.remove(node)
    if not affected_runs:
        return False
    last_run = affected_runs[-1]
    parent = last_run.getparent()
    if parent is None:
        return False
    insert_idx = parent.index(last_run) + 1
    new_run = LET.Element(f"{{{W_NS}}}r")
    rpr = affected_runs[0].find(f"{{{W_NS}}}rPr")
    if rpr is not None:
        new_run.append(LET.fromstring(LET.tostring(rpr)))
    new_run.append(drawing)
    parent.insert(insert_idx, new_run)
    for run in affected_runs:
        removable = True
        for child in run:
            if child.tag not in {f"{{{W_NS}}}rPr"}:
                removable = False
                break
        if removable and (run.text is None or not run.text.strip()):
            run_parent = run.getparent()
            if run_parent is not None:
                run_parent.remove(run)
    return True

def _word_prepare_image_assets(image_blobs: Dict[str, Dict[str, Optional[float]]]) -> Dict[str, Dict[str, object]]:
    if not image_blobs:
        return {}
    from PIL import Image as PILImage

    assets: Dict[str, Dict[str, object]] = {}
    for idx, (var, info) in enumerate(image_blobs.items(), start=1):
        data = info.get("bytes") if isinstance(info, dict) else None
        if not data:
            continue
        mm = None
        if isinstance(info, dict):
            mm = info.get("mm")
        try:
            img = PILImage.open(io.BytesIO(data)).convert("RGBA")
        except Exception:
            continue
        if mm:
            px = mm_to_pixels(mm, dpi=96)
            if px > 0:
                w, h = img.size
                if w:
                    new_h = int(round(h * (px / w))) if w else h
                    new_h = max(new_h, 1)
                    img = img.resize((max(px, 1), new_h), PILImage.LANCZOS)
        width_px, height_px = img.size
        if width_px <= 0 or height_px <= 0:
            continue
        bio = io.BytesIO()
        img.save(bio, format="PNG")
        safe = re.sub(r"[^A-Za-z0-9]+", "_", var).strip("_") or "image"
        filename = f"auto_{idx:04d}_{safe}.png"
        cx = int(round(max(width_px, 1) / 96 * EMU_PER_INCH))
        cy = int(round(max(height_px, 1) / 96 * EMU_PER_INCH))
        assets[var] = {
            "bytes": bio.getvalue(),
            "filename": filename,
            "target": f"media/{filename}",
            "cx": max(cx, 1),
            "cy": max(cy, 1),
        }
    return assets

def _word_apply_images(root, rels_tree: LET._ElementTree, assets: Dict[str, Dict[str, object]]) -> Tuple[bool, List[str]]:
    if not assets:
        return False, []
    rels_root = rels_tree.getroot()
    docpr_counter = [_word_max_docpr_id(root)]
    changed = False
    used: List[str] = []
    for var, asset in assets.items():
        pattern = re.compile(r"\{\{\s*%s\s*\}\}" % re.escape(var))
        while True:
            nodes, full_text = _word_snapshot(root)
            if not nodes:
                break
            m = pattern.search(full_text)
            if not m:
                break
            rid, rel_elem = _word_add_image_relationship(rels_root, asset["target"])
            docpr_id = _word_next_docpr(docpr_counter)
            drawing = _word_make_inline_drawing(rid, docpr_id, int(asset.get("cx", 1)), int(asset.get("cy", 1)))
            if _word_replace_placeholder_with_drawing(root, nodes, m.start(), m.end(), drawing):
                changed = True
                used.append(var)
            else:
                rels_root.remove(rel_elem)
                docpr_counter[0] -= 1
                break
    return changed, used

def _word_postprocess_docx(docx_path: str, text_map: Dict[str, str], assets: Dict[str, Dict[str, object]]):
    tmpdir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(docx_path, 'r') as zin:
            zin.extractall(tmpdir)

        used_vars: List[str] = []
        for p in _word_content_xmls(tmpdir):
            parser = LET.XMLParser(remove_blank_text=False)
            tree = LET.parse(p, parser)
            root = tree.getroot()
            _word_apply_text_map(root, text_map)
            if assets:
                rels_path = _word_part_rels_path(p)
                rels_tree = _word_load_rels_tree(rels_path)
                changed, used = _word_apply_images(root, rels_tree, assets)
                if changed:
                    rels_tree.write(rels_path, encoding="utf-8", xml_declaration=True)
                if used:
                    used_vars.extend(used)
            tree.write(p, encoding="utf-8", xml_declaration=True)

        if assets and used_vars:
            media_dir = os.path.join(tmpdir, "word", "media")
            os.makedirs(media_dir, exist_ok=True)
            for var in set(used_vars):
                asset = assets.get(var)
                if not asset:
                    continue
                with open(os.path.join(media_dir, asset["filename"]), "wb") as f:
                    f.write(asset["bytes"])

        with zipfile.ZipFile(docx_path, 'w', zipfile.ZIP_DEFLATED) as zout:
            for root_dir, _, files in os.walk(tmpdir):
                for fn in files:
                    full = os.path.join(root_dir, fn)
                    zout.write(full, os.path.relpath(full, tmpdir))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

def docx_render(
    in_docx: str,
    out_docx: str,
    text_map: Dict[str, str],
    image_map: Dict[str, Dict],
    loop_map: Optional[Dict[str, List[Dict[str, str]]]] = None,
):
    from docxtpl import DocxTemplate, InlineImage
    from docx.shared import Mm

    tmp = in_docx + ".jinja.docx"
    resolved_loops: Dict[str, List[Dict[str, str]]] = loop_map or {}

    size_hints = docx_convert_tags_to_jinja(in_docx, tmp, resolved_loops)

    doc = DocxTemplate(tmp)
    ctx: Dict[str, object] = {}
    for k, v in text_map.items():
        ctx[k] = v
    ctx.setdefault("loops", resolved_loops)
    image_blobs: Dict[str, Dict[str, Optional[float]]] = {}
    for k, meta in image_map.items():
        r = requests.get(meta["url"], timeout=20); r.raise_for_status()
        content = r.content
        bio = io.BytesIO(content)
        mm = meta.get("mm") or size_hints.get(k)
        image_blobs[k] = {"bytes": content, "mm": mm}
        ctx[k] = InlineImage(doc, bio, width=Mm(mm)) if mm else InlineImage(doc, bio)
    jinja_env = Environment(autoescape=False, undefined=DebugUndefined)
    doc.render(ctx, jinja_env=jinja_env)
    doc.save(out_docx)
    os.remove(tmp)

    assets = _word_prepare_image_assets(image_blobs)
    _word_postprocess_docx(out_docx, text_map, assets)

# ------------ Excel (.xlsx) ------------
def _xlsx_cell_to_coords(cell_ref: str) -> Tuple[Optional[int], Optional[int]]:
    m = CELL_REF_RE.match(cell_ref or "")
    if not m:
        return None, None
    col_letters, row_str = m.groups()
    col_idx = 0
    for ch in col_letters.upper():
        if not ('A' <= ch <= 'Z'):
            return None, None
        col_idx = col_idx * 26 + (ord(ch) - ord('A') + 1)
    try:
        row_idx = int(row_str)
    except ValueError:
        return None, None
    return col_idx - 1, row_idx - 1


def _xlsx_next_rel_id(root: ET.Element) -> str:
    max_id = 0
    for rel in root.findall(f"{{{REL_NS}}}Relationship"):
        rid = rel.get("Id", "")
        if rid.startswith("rId") and rid[3:].isdigit():
            max_id = max(max_id, int(rid[3:]))
    return f"rId{max_id + 1}"


def _xlsx_max_docpr_id(root: ET.Element) -> int:
    max_id = 0
    for el in root.iter():
        val = el.get("id")
        if val and val.isdigit():
            max_id = max(max_id, int(val))
    return max_id


def _xlsx_find_drawing_placeholders(drawings_dir: str, drawing_to_sheet: Dict[str, List[Dict[str, object]]]) -> List[Dict[str, object]]:
    placements: List[Dict[str, object]] = []
    if not os.path.isdir(drawings_dir):
        return placements

    ns = {"xdr": XDR_NS, "a": A_NS}

    for drawing_name, sheet_infos in drawing_to_sheet.items():
        path = os.path.join(drawings_dir, drawing_name)
        if not os.path.exists(path):
            continue
        try:
            tree = ET.parse(path)
        except Exception:
            continue

        root = tree.getroot()
        anchors = list(root)
        if not anchors:
            continue

        sheet_info = sheet_infos[0] if sheet_infos else {}

        for idx, anchor in enumerate(anchors):
            if anchor.tag not in {
                f"{{{XDR_NS}}}oneCellAnchor",
                f"{{{XDR_NS}}}twoCellAnchor",
                f"{{{XDR_NS}}}absoluteAnchor",
            }:
                continue

            sp = anchor.find("xdr:sp", ns)
            if sp is None:
                continue

            text_fragments: List[str] = []
            for t_node in sp.findall(".//a:t", ns):
                if t_node.text:
                    text_fragments.append(t_node.text)
            combined_text = "".join(text_fragments).strip()
            target_var, size_hint = parse_image_tag(combined_text)

            if not target_var:
                continue

            c_nv_pr = sp.find("xdr:nvSpPr/xdr:cNvPr", ns)
            docpr_id = c_nv_pr.get("id") if c_nv_pr is not None else None
            shape_name = c_nv_pr.get("name") if c_nv_pr is not None else None

            off_x = off_y = cx = cy = None
            xfrm = sp.find("xdr:spPr/a:xfrm", ns)
            if xfrm is not None:
                off = xfrm.find("a:off", ns)
                ext = xfrm.find("a:ext", ns)
                if off is not None:
                    off_x = off.get("x")
                    off_y = off.get("y")
                if ext is not None:
                    cx = ext.get("cx")
                    cy = ext.get("cy")

            placements.append({
                "source": "drawing",
                "drawing_name": drawing_name,
                "anchor_index": idx,
                "var": target_var,
                "size_hint": size_hint,
                "sheet_file": sheet_info.get("sheet_file"),
                "sheet_name": sheet_info.get("sheet_name"),
                "sheet_index": sheet_info.get("sheet_index"),
                "docpr_id": docpr_id,
                "shape_name": shape_name,
                "off_x": off_x,
                "off_y": off_y,
                "cx": cx,
                "cy": cy,
            })

    return placements


def _xlsx_set_anchor_size(anchor_el: Optional[ET.Element], cx: int, cy: int):
    if anchor_el is None:
        return

    ns = {"xdr": XDR_NS}
    tag = anchor_el.tag
    if tag in {f"{{{XDR_NS}}}oneCellAnchor", f"{{{XDR_NS}}}absoluteAnchor"}:
        ext_el = anchor_el.find("xdr:ext", ns)
        if ext_el is None:
            ext_el = ET.SubElement(anchor_el, f"{{{XDR_NS}}}ext")
        ext_el.set("cx", str(cx))
        ext_el.set("cy", str(cy))


def _xlsx_cell_text(
    cell: ET.Element,
    ns: Dict[str, str],
    shared_strings: List[str],
) -> str:
    t_attr = cell.get("t")
    if t_attr == "s":
        v_node = cell.find("s:v", ns)
        if v_node is None or v_node.text is None:
            return ""
        try:
            idx = int(v_node.text)
        except (TypeError, ValueError):
            return ""
        if 0 <= idx < len(shared_strings):
            return shared_strings[idx] or ""
        return ""
    if t_attr == "inlineStr":
        parts: List[str] = []
        for t_node in cell.findall("s:is/s:t", ns):
            parts.append(t_node.text or "")
        return "".join(parts)
    v_node = cell.find("s:v", ns)
    if v_node is not None and v_node.text is not None:
        return v_node.text
    return ""


def _xlsx_set_inline_text(cell: ET.Element, ns: Dict[str, str], text: str):
    for child in list(cell):
        if child.tag == f"{{{S_NS}}}f":
            continue
        cell.remove(child)
    cell.set("t", "inlineStr")
    is_node = ET.SubElement(cell, f"{{{S_NS}}}is")
    t_node = ET.SubElement(is_node, f"{{{S_NS}}}t")
    if text and (text.strip() != text or "\n" in text):
        t_node.set(f"{{{XML_NS}}}space", "preserve")
    t_node.text = text or ""


def _xlsx_clear_cell_value(cell: ET.Element):
    for child in list(cell):
        if child.tag == f"{{{S_NS}}}f":
            continue
        cell.remove(child)
    if "t" in cell.attrib:
        del cell.attrib["t"]


def _xlsx_find_loop_group_in_text(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.search(rf"\{{\s*(?P<group>{VAR_NAME})\s*:\s*loop\s*\}}", text)
    return match.group("group") if match else None


def _xlsx_cell_has_loop_token(text: str, group: str) -> bool:
    if not text or not group:
        return False
    pattern = rf"\{{\s*{re.escape(group)}\s*:\s*loop(?:\s*:\s*{VAR_NAME})?\s*\}}"
    return re.search(pattern, text) is not None


def _xlsx_apply_loop_text(
    text: str,
    group: str,
    entry: Dict[str, str],
    text_map: Dict[str, str],
) -> str:
    result = text.replace(f"{{{group}:loop}}", "")
    result = result.replace("#end", "")
    for field, value in entry.items():
        for token in (
            f"{{{group}:{field}}}",
            f"{{{group}:loop:{field}}}",
        ):
            result = result.replace(token, value or "")
    result = _apply_text_tokens(result, text_map)
    return result


def _xlsx_reindex_rows(sheet_data: ET.Element, ns: Dict[str, str]):
    for row_idx, row_el in enumerate(sheet_data.findall("s:row", ns), start=1):
        row_el.set("r", str(row_idx))
        for cell in row_el.findall("s:c", ns):
            ref = cell.get("r")
            if not ref:
                continue
            m = CELL_REF_RE.match(ref)
            if not m:
                continue
            col_letters = m.group(1)
            cell.set("r", f"{col_letters}{row_idx}")


def _xlsx_expand_loops(
    sheet_root: ET.Element,
    shared_strings: List[str],
    loop_map: Dict[str, List[Dict[str, str]]],
    text_map: Dict[str, str],
):
    if not loop_map:
        return

    ns = {"s": S_NS}
    sheet_data = sheet_root.find("s:sheetData", ns)
    if sheet_data is None:
        return

    rows = list(sheet_data.findall("s:row", ns))
    idx = 0
    while idx < len(rows):
        row = rows[idx]
        group: Optional[str] = None
        start_cell_text: Optional[str] = None
        for cell in row.findall("s:c", ns):
            raw_text = _xlsx_cell_text(cell, ns, shared_strings) or ""
            text = raw_text.strip()
            if not text:
                continue
            detected_group = _xlsx_find_loop_group_in_text(text)
            if detected_group:
                if group and detected_group != group:
                    raise ValueError(
                        "Multiple loop groups in a single row are not supported"
                    )
                group = detected_group
                start_cell_text = raw_text
        if not group:
            idx += 1
            continue

        start_pattern = re.compile(
            rf"^\s*\{{\s*{re.escape(group)}\s*:\s*loop\s*\}}\s*$"
        )
        strict_applied = False
        if start_cell_text and start_pattern.match(start_cell_text):
            group_field_pattern = re.compile(
                rf"\{{\s*{re.escape(group)}\s*:\s*{VAR_NAME}\s*\}}"
            )
            loop_field_pattern = re.compile(
                rf"\{{\s*{re.escape(group)}\s*:\s*loop\s*:\s*{VAR_NAME}\s*\}}"
            )
            end_idx = idx + 1
            end_row: Optional[ET.Element] = None
            while end_idx < len(rows):
                candidate_row = rows[end_idx]
                found_end = False
                for cell in candidate_row.findall("s:c", ns):
                    raw_text = _xlsx_cell_text(cell, ns, shared_strings) or ""
                    text = raw_text.strip()
                    if not text:
                        continue
                    if text == "#end":
                        if not re.fullmatch(r"\s*#end\s*", raw_text):
                            break
                        found_end = True
                        break
                if found_end:
                    end_row = candidate_row
                    break
                end_idx += 1

            if end_row is not None:
                block_rows = rows[idx + 1 : end_idx]
                template_bases = [copy.deepcopy(r) for r in block_rows]
                if template_bases:
                    for remove_row in [row] + block_rows + [end_row]:
                        sheet_data.remove(remove_row)

                    entries = loop_map.get(group, [])
                    insert_pos = idx

                    if entries:
                        for entry_idx, entry in enumerate(entries):
                            for tmpl in template_bases:
                                clone = copy.deepcopy(tmpl)
                                for cell in clone.findall("s:c", ns):
                                    original_text = _xlsx_cell_text(
                                        cell, ns, shared_strings
                                    )
                                    if original_text is None:
                                        original_text = ""
                                    has_group_token = bool(
                                        loop_field_pattern.search(original_text)
                                        or group_field_pattern.search(original_text)
                                        or _xlsx_cell_has_loop_token(
                                            original_text, group
                                        )
                                    )
                                    replaced = _xlsx_apply_loop_text(
                                        original_text, group, entry, text_map
                                    )
                                    if replaced != original_text or has_group_token:
                                        _xlsx_set_inline_text(cell, ns, replaced)
                                if clone.findall("s:c", ns):
                                    sheet_data.insert(insert_pos, clone)
                                    insert_pos += 1

                    rows = list(sheet_data.findall("s:row", ns))
                    idx = insert_pos
                    strict_applied = True
        if strict_applied:
            continue

        legacy_end_idx = idx
        while legacy_end_idx < len(rows):
            candidate_row = rows[legacy_end_idx]
            has_group_token = False
            for cell in candidate_row.findall("s:c", ns):
                raw_text = _xlsx_cell_text(cell, ns, shared_strings) or ""
                text = raw_text.strip()
                if not text:
                    continue
                if text == "#end":
                    if not re.fullmatch(r"\s*#end\s*", raw_text):
                        raise ValueError(
                            f"Loop end marker for '{group}' must be '#end' only"
                        )
                    found_end = True
                    break
            if not has_group_token:
                if legacy_end_idx == idx:
                    has_group_token = True
                else:
                    break
            legacy_end_idx += 1

        block_rows = rows[idx:legacy_end_idx]
        if not block_rows:
            idx += 1
            continue

        template_bases = [copy.deepcopy(r) for r in block_rows]
        cleaned_templates: List[ET.Element] = []
        for base in template_bases:
            for cell in list(base.findall("s:c", ns)):
                cell_text = (_xlsx_cell_text(cell, ns, shared_strings) or "").strip()
                if not cell_text:
                    continue
                if cell_text == "#end":
                    base.remove(cell)
            if base.findall("s:c", ns):
                cleaned_templates.append(base)
        template_bases = cleaned_templates or template_bases

        for original in block_rows:
            sheet_data.remove(original)

        entries = loop_map.get(group, [])
        insert_pos = idx

        if entries:
            for entry_idx, entry in enumerate(entries):
                for tmpl in template_bases:
                    clone = copy.deepcopy(tmpl)
                    for cell in list(clone.findall("s:c", ns)):
                        original_text = _xlsx_cell_text(cell, ns, shared_strings)
                        if original_text is None:
                            continue
                        has_group_token = _xlsx_cell_has_loop_token(
                            original_text, group
                        )
                        replaced = _xlsx_apply_loop_text(
                            original_text, group, entry, text_map
                        )
                        if entry_idx > 0 and not has_group_token:
                            clone.remove(cell)
                            continue
                        if (
                            replaced != original_text
                            or has_group_token
                            or "#end" in original_text
                        ):
                            _xlsx_set_inline_text(cell, ns, replaced)
                    if clone.findall("s:c", ns):
                        sheet_data.insert(insert_pos, clone)
                        insert_pos += 1

        rows = list(sheet_data.findall("s:row", ns))
        idx = insert_pos

    _xlsx_reindex_rows(sheet_data, ns)


def _decode_base64_payload(payload: str) -> Optional[bytes]:
    data = (payload or "").strip()
    if not data:
        return None
    padding = len(data) % 4
    if padding:
        data += "=" * (4 - padding)
    try:
        return base64.b64decode(data, validate=False)
    except (binascii.Error, ValueError):
        return None


def _xlsx_fetch_image_bytes(source: str) -> Optional[bytes]:
    if not source:
        return None

    text = source.strip()
    if not text:
        return None

    if text.startswith("data:"):
        header, _, payload = text.partition(",")
        if not payload:
            return None
        if ";base64" in header:
            return _decode_base64_payload(payload)
        return urllib.parse.unquote_to_bytes(payload)

    if text.startswith("base64:"):
        _, _, payload = text.partition(":")
        return _decode_base64_payload(payload)

    if text.startswith("base64,"):
        return _decode_base64_payload(text[7:])

    try:
        resp = requests.get(text, timeout=20)
        resp.raise_for_status()
        return resp.content
    except Exception:
        return None


def _xlsx_update_content_types(extracted_dir: str, media_exts: Set[str], drawing_files: Set[str]):
    if not media_exts and not drawing_files:
        return

    path = os.path.join(extracted_dir, "[Content_Types].xml")
    if not os.path.exists(path):
        return

    try:
        tree, nsmap = _xml_parse_tree(path)
    except Exception:
        return

    root = tree.getroot()
    if root is None:
        return

    changed = False
    ns = {"ct": CONTENT_TYPES_NS}
    defaults = {}
    for node in root.findall("ct:Default", ns):
        ext = (node.get("Extension") or "").lower()
        if ext:
            defaults[ext] = node

    media_map = {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
    }
    for ext in sorted(media_exts):
        lower = ext.lower()
        if lower in defaults:
            continue
        content_type = media_map.get(lower, f"image/{lower}")
        node = ET.SubElement(root, f"{{{CONTENT_TYPES_NS}}}Default")
        node.set("Extension", lower)
        node.set("ContentType", content_type)
        changed = True

    overrides = {}
    for node in root.findall("ct:Override", ns):
        part = node.get("PartName") or ""
        if part:
            overrides[part] = node

    drawing_content_type = "application/vnd.openxmlformats-officedocument.drawing+xml"
    for drawing_name in sorted(drawing_files):
        part = f"/xl/drawings/{drawing_name}"
        if part in overrides:
            continue
        node = ET.SubElement(root, f"{{{CONTENT_TYPES_NS}}}Override")
        node.set("PartName", part)
        node.set("ContentType", drawing_content_type)
        changed = True

    if changed:
        _xml_write_tree(tree, path, default_namespace=CONTENT_TYPES_NS, namespace_map=nsmap)


def _xlsx_ensure_dir(path: str):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def _xlsx_sheet_map(extracted_dir: str) -> Dict[str, Tuple[Optional[str], Optional[int]]]:
    mapping: Dict[str, Tuple[Optional[str], Optional[int]]] = {}
    workbook_xml = os.path.join(extracted_dir, "xl", "workbook.xml")
    rels_xml = os.path.join(extracted_dir, "xl", "_rels", "workbook.xml.rels")
    if not os.path.exists(workbook_xml):
        return mapping

    rid_to_target: Dict[str, str] = {}
    if os.path.exists(rels_xml):
        tree = ET.parse(rels_xml); root = tree.getroot()
        for rel in root.findall(f".//{{{REL_NS}}}Relationship"):
            if rel.get("Type", "").endswith("/worksheet"):
                rid = rel.get("Id"); target = rel.get("Target")
                if rid and target:
                    rid_to_target[rid] = target.replace('\\', '/')

    ns = {"s": S_NS, "r": R_NS}
    tree = ET.parse(workbook_xml); root = tree.getroot()
    sheets = root.find("s:sheets", ns)
    if sheets is None:
        return mapping
    for idx, sheet in enumerate(sheets.findall("s:sheet", ns)):
        name = sheet.get("name")
        rid = sheet.get(f"{{{R_NS}}}id")
        target = rid_to_target.get(rid or "")
        if target:
            rel_path = os.path.normpath(os.path.join("xl", target))
            basename = os.path.basename(rel_path)
            mapping[basename] = (name, idx)
    return mapping

def xlsx_force_full_recalc(extracted_dir: str):
    p = os.path.join(extracted_dir, "xl", "workbook.xml")
    if not os.path.exists(p): return
    ns = {"s": S_NS}
    tree, workbook_nsmap = _xml_parse_tree(p)
    root = tree.getroot()
    calcPr = root.find("s:calcPr", ns)
    if calcPr is None:
        calcPr = ET.SubElement(root, f"{{{ns['s']}}}calcPr")
    if not calcPr.get("calcId"):
        # Excel expects a calcId even when forcing a recalculation; reuse the
        # default value generated by openpyxl when the workbook lacks one.
        calcPr.set("calcId", "124519")
    calcPr.set("calcMode", "auto")
    calcPr.set("fullCalcOnLoad", "1")
    calcPr.set("calcOnSave", "1")
    calcPr.set("forceFullCalc", "1")
    chain = os.path.join(extracted_dir, "xl", "calcChain.xml")
    if os.path.exists(chain):
        try: os.remove(chain)
        except: pass
    rels_path = os.path.join(extracted_dir, "xl", "_rels", "workbook.xml.rels")
    if os.path.exists(rels_path):
        rels_tree, rels_nsmap = _xml_parse_tree(rels_path)
        rels_root = rels_tree.getroot()
        removed = False
        for rel in list(rels_root.findall(f".//{{{REL_NS}}}Relationship")):
            target = (rel.get("Target") or "").replace("\\", "/")
            rel_type = rel.get("Type") or ""
            if target.endswith("calcChain.xml") or rel_type == CALC_CHAIN_REL_TYPE:
                rels_root.remove(rel)
                removed = True
        if removed:
            default_rels_ns = rels_nsmap.get("") if rels_nsmap else REL_NS
            _xml_write_tree(
                rels_tree,
                rels_path,
                default_namespace=default_rels_ns,
                namespace_map=rels_nsmap,
            )
    ct_path = os.path.join(extracted_dir, "[Content_Types].xml")
    if os.path.exists(ct_path):
        ct_tree, ct_nsmap = _xml_parse_tree(ct_path)
        ct_root = ct_tree.getroot()
        ns_ct = {"ct": CONTENT_TYPES_NS}
        removed = False
        for node in list(ct_root.findall("ct:Override", ns_ct)):
            part = (node.get("PartName") or "").replace("\\", "/")
            if part.lower() == CALC_CHAIN_PART.lower():
                ct_root.remove(node)
                removed = True
        if removed:
            _xml_write_tree(
                ct_tree,
                ct_path,
                default_namespace=CONTENT_TYPES_NS,
                namespace_map=ct_nsmap,
            )
    _xml_write_tree(tree, p, default_namespace=workbook_nsmap.get("") if workbook_nsmap else S_NS, namespace_map=workbook_nsmap)

def _xlsx_escape_sheet_name(name: str) -> str:
    if not name:
        return ""
    if re.search(r"[\s'!]", name):
        return "'" + name.replace("'", "''") + "'"
    return name

def xlsx_update_formula_caches(xlsx_path: str, formula_cells: List[Dict]):
    if not formula_cells:
        return
    try:
        from xlcalculator import ModelCompiler, Evaluator
    except Exception:
        return

    try:
        compiler = ModelCompiler()
        model = compiler.read_and_parse_archive(xlsx_path)
        evaluator = Evaluator(model)
    except Exception:
        return

    computed: Dict[Tuple[str, str], Dict[str, Optional[str]]] = {}
    fallbacks: Dict[Tuple[str, str], Dict[str, Optional[str]]] = {}
    needs_fallback_write = False
    sheet_names_by_index: List[str] = []
    try:
        # model.cells keys look like "Sheet!A1"; derive sheet list lazily
        seen = []
        for key in model.cells.keys():
            sheet_part = key.split("!", 1)[0]
            if sheet_part not in seen:
                seen.append(sheet_part)
        sheet_names_by_index = seen
    except Exception:
        sheet_names_by_index = []

    for info in formula_cells:
        cell_ref = info.get("cell_ref")
        sheet_file = info.get("sheet_file")
        sheet_name = info.get("sheet_name")
        sheet_index = info.get("sheet_index")
        if not cell_ref or not sheet_file:
            continue
        if not sheet_name and sheet_index is not None and 0 <= sheet_index < len(sheet_names_by_index):
            sheet_name = sheet_names_by_index[sheet_index]
        if not sheet_name:
            continue
        key = (sheet_file, cell_ref)
        original_value = info.get("original_value")
        original_type = info.get("original_type")
        fallbacks[key] = {
            "value": original_value,
            "type": original_type,
        }
        if original_value is not None:
            needs_fallback_write = True
        address = f"{_xlsx_escape_sheet_name(sheet_name)}!{cell_ref}"
        try:
            value = evaluator.evaluate(address)
        except Exception:
            continue
        if hasattr(value, "value"):
            value = value.value
        formatted, type_hint = _format_formula_value(value)
        if formatted is None:
            continue
        computed[key] = {"value": formatted, "type": type_hint}

    if not computed and not needs_fallback_write:
        return

    tmpdir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(xlsx_path, 'r') as zin:
            zin.extractall(tmpdir)
        ns = {"s": S_NS}
        updated: Dict[str, Tuple[ET.ElementTree, Dict[str, str]]] = {}
        for info in formula_cells:
            sheet_file = info.get("sheet_file")
            cell_ref = info.get("cell_ref")
            if not sheet_file or not cell_ref:
                continue
            key = (sheet_file, cell_ref)
            sheet_path = os.path.join(tmpdir, "xl", "worksheets", sheet_file)
            if not os.path.exists(sheet_path):
                continue
            if sheet_file not in updated:
                tree, nsmap = _xml_parse_tree(sheet_path)
                updated[sheet_file] = (tree, nsmap)
            tree, nsmap = updated[sheet_file]
            root = tree.getroot()
            cell = root.find(f".//s:c[@r='{cell_ref}']", ns)
            if cell is None:
                continue
            v_node = cell.find("s:v", ns)
            if key in computed:
                if v_node is None:
                    v_node = ET.SubElement(cell, f"{{{ns['s']}}}v")
                result = computed[key]
                v_node.text = result.get("value") or ""
                type_hint = result.get("type")
                if type_hint == "b":
                    cell.set("t", "b")
                elif type_hint == "str":
                    cell.set("t", "str")
                else:
                    cell.attrib.pop("t", None)
            else:
                fallback = fallbacks.get(key, {})
                original_value = fallback.get("value")
                original_type = fallback.get("type")
                if original_value is not None:
                    if v_node is None:
                        v_node = ET.SubElement(cell, f"{{{ns['s']}}}v")
                    v_node.text = original_value
                    if original_type:
                        cell.set("t", original_type)
                    else:
                        cell.attrib.pop("t", None)
        for sheet_file, (tree, nsmap) in updated.items():
            sheet_path = os.path.join(tmpdir, "xl", "worksheets", sheet_file)
            _xml_write_tree(tree, sheet_path, namespace_map=nsmap)
        with zipfile.ZipFile(xlsx_path, 'w', zipfile.ZIP_DEFLATED) as zout:
            for root_dir, _, files in os.walk(tmpdir):
                for fn in files:
                    full = os.path.join(root_dir, fn)
                    zout.write(full, os.path.relpath(full, tmpdir))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

def xlsx_patch_and_place(
    src_xlsx: str,
    dst_xlsx: str,
    text_map: Dict[str, str],
    image_map: Dict[str, Dict],
    loop_map: Optional[Dict[str, List[Dict[str, str]]]] = None,
):
    """
    1) XML直編集で {var} を置換（完全一致は数値化、<br>→\n）、{[img]} はセルから除去し placements に記録
    2) drawing XML を直接編集して placements に新規画像挿入（既存図形/グラフは維持）
    3) fullCalcOnLoad=1 で再計算
    """
    ns = {"s": S_NS}
    tmpdir = tempfile.mkdtemp()
    placements: List[Dict[str, object]] = []
    formula_cells: List[Dict[str, object]] = []
    sheet_drawings: Dict[str, List[Dict[str, object]]] = {}
    shared_strings_values: List[str] = []
    resolved_loops = loop_map or {}

    try:
        with zipfile.ZipFile(src_xlsx, 'r') as zin:
            zin.extractall(tmpdir)

        # sharedStrings
        sst_path = os.path.join(tmpdir, "xl", "sharedStrings.xml")
        numeric_candidates: Dict[int, Tuple[bool, Optional[float]]] = {}
        img_sst_idx: Dict[int, Tuple[str, Optional[float]]] = {}

        if os.path.exists(sst_path):
            tree, sst_nsmap = _xml_parse_tree(sst_path)
            root = tree.getroot(); idx = -1
            for si in root.findall("s:si", ns):
                idx += 1
                t_nodes = si.findall("s:t", ns)
                if t_nodes:
                    original = "".join([t.text or "" for t in t_nodes])
                else:
                    original = "".join([(r.find("s:t", ns).text or "") for r in si.findall("s:r", ns) if r.find("s:t", ns) is not None])

                # 画像タグ？
                var, size_hint = parse_image_tag(original or "")
                if var:
                    img_sst_idx[idx] = (var, size_hint)
                    for r in list(si): si.remove(r)
                    t = ET.SubElement(si, f"{{{ns['s']}}}t"); t.text = ""
                    shared_strings_values.append("")
                    continue

                # テキスト置換
                replaced = _apply_text_tokens(original, text_map)

                # 書き戻し
                for r in list(si): si.remove(r)
                t = ET.SubElement(si, f"{{{ns['s']}}}t"); t.text = replaced
                shared_strings_values.append(replaced or "")

                # 完全一致 = 数値候補
                txt_match = TXT_KEY_PATTERN.match(original or "")
                if txt_match:
                    var_name = txt_match.group("var")
                    mapped = text_map.get(var_name)
                    if mapped is not None:
                        num, _ = parse_numberlike(mapped)
                        numeric_candidates[idx] = ((num is not None), num)

            _xml_write_tree(tree, sst_path, namespace_map=sst_nsmap)

        # worksheets
        ws_dir = os.path.join(tmpdir, "xl", "worksheets")
        sheet_map = _xlsx_sheet_map(tmpdir)
        if os.path.isdir(ws_dir):
            for fn in os.listdir(ws_dir):
                if not fn.endswith(".xml"): continue
                p = os.path.join(ws_dir, fn)
                tree, sheet_nsmap = _xml_parse_tree(p)
                root = tree.getroot()

                if resolved_loops:
                    _xlsx_expand_loops(root, shared_strings_values, resolved_loops, text_map)

                sheet_name, sheet_index = sheet_map.get(fn, (None, None))
                if sheet_index is None:
                    m = re.findall(r'\d+', fn)
                    sheet_index = int(m[0]) - 1 if m else 0

                rels_path = os.path.join(tmpdir, "xl", "worksheets", "_rels", f"{fn}.rels")
                if os.path.exists(rels_path):
                    try:
                        rels_tree = ET.parse(rels_path)
                        rels_root = rels_tree.getroot()
                        for rel in rels_root.findall(f"{{{REL_NS}}}Relationship"):
                            if rel.get("Type") == f"{R_NS}/drawing":
                                target = (rel.get("Target") or "").replace('\\', '/')
                                drawing_name = os.path.basename(target)
                                if drawing_name:
                                    sheet_drawings.setdefault(drawing_name, []).append({
                                        "sheet_file": fn,
                                        "sheet_name": sheet_name,
                                        "sheet_index": sheet_index,
                                    })
                    except Exception:
                        pass

                for c in root.findall(".//s:c", ns):
                    t_attr = c.get("t")
                    v_node = c.find("s:v", ns)
                    is_node = c.find("s:is", ns)
                    f_node = c.find("s:f", ns)
                    r_attr = c.get("r") or ""

                    if f_node is not None:
                        formula_cells.append({
                            "sheet_file": fn,
                            "sheet_name": sheet_name,
                            "sheet_index": sheet_index,
                            "cell_ref": r_attr,
                            "original_value": v_node.text if v_node is not None else None,
                            "original_type": t_attr if t_attr is not None else None,
                        })

                    # 数式セルはキャッシュ値を削除し LibreOffice での再計算を確実化
                    if f_node is not None and v_node is not None:
                        try: c.remove(v_node)
                        except: pass
                        v_node = None

                    # shared string
                    if t_attr == "s" and v_node is not None and v_node.text:
                        try: sst_idx = int(v_node.text)
                        except: sst_idx = None
                        if sst_idx is not None and sst_idx in img_sst_idx:
                            # 画像座標として記録してセルは空に
                            var, size_hint = img_sst_idx[sst_idx]
                            placements.append({
                                "sheet_file": fn,
                                "sheet_name": sheet_name,
                                "sheet_index": sheet_index,
                                "cell_ref": r_attr,
                                "var": var,
                                "size_hint": size_hint,
                            })
                            c.attrib.pop("t", None)
                            c.remove(v_node)
                            continue
                        if sst_idx is not None and sst_idx in numeric_candidates:
                            is_num, num_val = numeric_candidates[sst_idx]
                            if is_num and num_val is not None:
                                c.set("t", "n")
                                v_node.text = str(num_val)

                    # inlineStr
                    if t_attr == "inlineStr" and is_node is not None:
                        t_inline = is_node.find("s:t", ns)
                        if t_inline is not None and t_inline.text is not None:
                            txt = t_inline.text
                            var, size_hint = parse_image_tag(txt or "")
                            if var:
                                placements.append({
                                    "sheet_file": fn,
                                    "sheet_name": sheet_name,
                                    "sheet_index": sheet_index,
                                    "cell_ref": r_attr,
                                    "var": var,
                                    "size_hint": size_hint,
                                })

                                c.attrib.pop("t", None)
                                try: c.remove(is_node)
                                except: pass
                                if v_node is not None:
                                    c.remove(v_node)
                                continue
                            # テキスト置換／数値化
                            tag_match = TXT_KEY_PATTERN.match(txt or "")
                            if tag_match:
                                mapped = text_map.get(tag_match.group("var"))
                                if mapped is not None:
                                    num, _ = parse_numberlike(mapped)
                                    if num is not None:
                                        c.set("t", "n")
                                        try: c.remove(is_node)
                                        except: pass
                                        if v_node is None: v_node = ET.SubElement(c, f"{{{ns['s']}}}v")
                                        v_node.text = str(num)
                                        continue
                            replaced = _apply_text_tokens(txt, text_map)
                            t_inline.text = replaced

                _xml_write_tree(tree, p, namespace_map=sheet_nsmap)

        drawings_dir = os.path.join(tmpdir, "xl", "drawings")
        if image_map:
            drawing_placeholders = _xlsx_find_drawing_placeholders(drawings_dir, sheet_drawings)
            if drawing_placeholders:
                placements.extend(drawing_placeholders)

        # 画像の配置
        if placements and image_map:
            from PIL import Image as PILImage

            media_dir = os.path.join(tmpdir, "xl", "media")
            drawings_dir = os.path.join(tmpdir, "xl", "drawings")
            drawings_rels_dir = os.path.join(drawings_dir, "_rels")
            _xlsx_ensure_dir(media_dir)
            _xlsx_ensure_dir(drawings_dir)
            _xlsx_ensure_dir(drawings_rels_dir)

            existing_media = set(os.listdir(media_dir)) if os.path.isdir(media_dir) else set()
            existing_drawings = set(os.listdir(drawings_dir)) if os.path.isdir(drawings_dir) else set()

            sheet_tree_cache: Dict[str, Tuple[ET.ElementTree, Dict[str, str]]] = {}
            sheet_rels_cache: Dict[str, Tuple[ET.ElementTree, Dict[str, str]]] = {}
            drawing_cache: Dict[str, Dict[str, object]] = {}
            used_media_exts: Set[str] = set()
            new_drawing_files: Set[str] = set()

            def _ensure_sheet_tree(sheet_file: str) -> Optional[ET.ElementTree]:
                cached = sheet_tree_cache.get(sheet_file)
                if cached is not None:
                    return cached[0]
                path = os.path.join(tmpdir, "xl", "worksheets", sheet_file)
                if not os.path.exists(path):
                    return None
                tree, nsmap = _xml_parse_tree(path)
                sheet_tree_cache[sheet_file] = (tree, nsmap)
                return tree

            def _ensure_sheet_rels(sheet_file: str) -> ET.ElementTree:
                cached = sheet_rels_cache.get(sheet_file)
                if cached is not None:
                    return cached[0]
                rels_dir = os.path.join(tmpdir, "xl", "worksheets", "_rels")
                _xlsx_ensure_dir(rels_dir)
                path = os.path.join(rels_dir, f"{sheet_file}.rels")
                if os.path.exists(path):
                    tree, nsmap = _xml_parse_tree(path)
                else:
                    root_rels = ET.Element(f"{{{REL_NS}}}Relationships")
                    tree = ET.ElementTree(root_rels)
                    nsmap = {"": REL_NS}
                sheet_rels_cache[sheet_file] = (tree, nsmap)
                return tree

            def _ensure_drawing_state(drawing_name: str) -> Optional[Dict[str, object]]:
                state = drawing_cache.get(drawing_name)
                if state is not None:
                    return state
                drawing_path = os.path.join(drawings_dir, drawing_name)
                if os.path.exists(drawing_path):
                    tree, drawing_nsmap = _xml_parse_tree(drawing_path)
                else:
                    root = ET.Element(f"{{{XDR_NS}}}wsDr")
                    tree = ET.ElementTree(root)
                    drawing_nsmap = {"": XDR_NS}
                root = tree.getroot()
                drawing_rels_path = os.path.join(drawings_rels_dir, f"{drawing_name}.rels")
                if os.path.exists(drawing_rels_path):
                    rels_tree, drawing_rels_nsmap = _xml_parse_tree(drawing_rels_path)
                else:
                    rels_root = ET.Element(f"{{{REL_NS}}}Relationships")
                    rels_tree = ET.ElementTree(rels_root)
                    drawing_rels_nsmap = {"": REL_NS}
                state = {
                    "tree": tree,
                    "root": root,
                    "nsmap": drawing_nsmap,
                    "path": drawing_path,
                    "rels_tree": rels_tree,
                    "rels_path": drawing_rels_path,
                    "rels_nsmap": drawing_rels_nsmap,
                    "max_id": _xlsx_max_docpr_id(root),
                }
                drawing_cache[drawing_name] = state
                return state

            def _next_media_name() -> str:
                idx = 1
                while True:
                    candidate = f"image{idx}.png"
                    if candidate not in existing_media:
                        existing_media.add(candidate)
                        return candidate
                    idx += 1

            for item in placements:
                var = item.get("var")
                if not var:
                    continue
                meta = image_map.get(var)
                if not meta:
                    continue

                source = item.get("source")
                sheet_file = item.get("sheet_file")
                cell_ref = item.get("cell_ref")
                size_hint = item.get("size_hint")
                drawing_name = item.get("drawing_name")

                if source == "drawing":
                    if not drawing_name:
                        continue
                else:
                    if not sheet_file or not cell_ref:
                        continue

                url = meta.get("url")
                if not url:
                    continue
                mm = meta.get("mm") or size_hint
                raw_data = _xlsx_fetch_image_bytes(url)
                if raw_data is None:
                    continue
                try:
                    img = PILImage.open(io.BytesIO(raw_data)).convert("RGBA")
                except Exception:
                    continue
                if mm:
                    target_px = mm_to_pixels(mm, dpi=96)
                    if target_px > 0 and img.size[0] > 0:
                        new_height = int(round(img.size[1] * (target_px / img.size[0])))
                        if new_height <= 0:
                            new_height = 1
                        img = img.resize((target_px, new_height), PILImage.LANCZOS)
                if img.size[0] <= 0 or img.size[1] <= 0:
                    continue
                buffer = io.BytesIO()
                img.save(buffer, format="PNG")
                png_bytes = buffer.getvalue()
                buffer.close()

                if source == "drawing":
                    state = _ensure_drawing_state(drawing_name)
                    if not state:
                        continue

                    root = state["root"]
                    anchors = list(root)
                    anchor_index = item.get("anchor_index")
                    if anchor_index is None or anchor_index >= len(anchors):
                        continue
                    anchor_el = anchors[anchor_index]
                    sp_el = anchor_el.find(f"{{{XDR_NS}}}sp")
                    if sp_el is None:
                        continue

                    current_max = int(state.get("max_id", 0))
                    docpr_id_val = item.get("docpr_id")
                    try:
                        docpr_int = int(docpr_id_val)
                    except (TypeError, ValueError):
                        docpr_int = None
                    if docpr_int is None:
                        docpr_int = current_max + 1
                    state["max_id"] = max(current_max, docpr_int)

                    shape_name = item.get("shape_name") or f"Picture {docpr_int}"

                    insert_pos = None
                    for child_idx, child in enumerate(list(anchor_el)):
                        if child.tag == f"{{{XDR_NS}}}sp":
                            insert_pos = child_idx
                            anchor_el.remove(child)
                            break

                    image_name = _next_media_name()
                    with open(os.path.join(media_dir, image_name), "wb") as f:
                        f.write(png_bytes)
                    ext = os.path.splitext(image_name)[1].lstrip(".").lower()
                    if ext:
                        used_media_exts.add(ext)

                    rels_tree = state["rels_tree"]
                    rels_root_drawing = rels_tree.getroot()
                    img_rid = _xlsx_next_rel_id(rels_root_drawing)
                    rel = ET.SubElement(rels_root_drawing, f"{{{REL_NS}}}Relationship")
                    rel.set("Id", img_rid)
                    rel.set("Type", f"{R_NS}/image")
                    rel.set("Target", f"../media/{image_name}")

                    pic = ET.Element(f"{{{XDR_NS}}}pic")
                    nv_pic = ET.SubElement(pic, f"{{{XDR_NS}}}nvPicPr")
                    c_nv_pr = ET.SubElement(nv_pic, f"{{{XDR_NS}}}cNvPr")
                    c_nv_pr.set("id", str(docpr_int))
                    c_nv_pr.set("name", shape_name)
                    c_nv_pic_pr = ET.SubElement(nv_pic, f"{{{XDR_NS}}}cNvPicPr")
                    pic_locks = ET.SubElement(c_nv_pic_pr, f"{{{A_NS}}}picLocks")
                    pic_locks.set("noChangeAspect", "1")
                    pic_locks.set("noChangeArrowheads", "1")

                    blip_fill = ET.SubElement(pic, f"{{{XDR_NS}}}blipFill")
                    blip = ET.SubElement(blip_fill, f"{{{A_NS}}}blip")
                    blip.set(f"{{{R_NS}}}embed", img_rid)
                    stretch = ET.SubElement(blip_fill, f"{{{A_NS}}}stretch")
                    ET.SubElement(stretch, f"{{{A_NS}}}fillRect")

                    sp_pr = ET.SubElement(pic, f"{{{XDR_NS}}}spPr")
                    xfrm = ET.SubElement(sp_pr, f"{{{A_NS}}}xfrm")
                    off = ET.SubElement(xfrm, f"{{{A_NS}}}off")
                    off.set("x", item.get("off_x") or "0")
                    off.set("y", item.get("off_y") or "0")

                    if mm:
                        cx_val = max(1, int(round(img.size[0] * EMU_PER_PIXEL)))
                        cy_val = max(1, int(round(img.size[1] * EMU_PER_PIXEL)))
                    else:
                        cx_val = cy_val = None
                        try:
                            if item.get("cx"):
                                cx_val = max(1, int(item.get("cx")))
                            if item.get("cy"):
                                cy_val = max(1, int(item.get("cy")))
                        except (TypeError, ValueError):
                            cx_val = cy_val = None
                        if cx_val is None or cy_val is None:
                            cx_val = max(1, int(round(img.size[0] * EMU_PER_PIXEL)))
                            cy_val = max(1, int(round(img.size[1] * EMU_PER_PIXEL)))

                    ext = ET.SubElement(xfrm, f"{{{A_NS}}}ext")
                    ext.set("cx", str(cx_val))
                    ext.set("cy", str(cy_val))
                    _xlsx_set_anchor_size(anchor_el, cx_val, cy_val)
                    prst = ET.SubElement(sp_pr, f"{{{A_NS}}}prstGeom")
                    prst.set("prst", "rect")
                    ET.SubElement(prst, f"{{{A_NS}}}avLst")

                    if insert_pos is None:
                        anchor_el.append(pic)
                    else:
                        anchor_el.insert(insert_pos, pic)
                else:
                    col_idx, row_idx = _xlsx_cell_to_coords(cell_ref)
                    if col_idx is None or row_idx is None:
                        continue

                    sheet_tree = _ensure_sheet_tree(sheet_file)
                    if sheet_tree is None:
                        continue
                    sheet_root = sheet_tree.getroot()

                    sheet_rels_tree = _ensure_sheet_rels(sheet_file)
                    rels_root = sheet_rels_tree.getroot()

                    drawing_rel = None
                    for rel in rels_root.findall(f"{{{REL_NS}}}Relationship"):
                        if rel.get("Type") == f"{R_NS}/drawing":
                            drawing_rel = rel
                            break

                    target_drawing_name = None
                    drawing_rel_id = None
                    if drawing_rel is None:
                        drawing_rel_id = _xlsx_next_rel_id(rels_root)
                        next_idx = 1
                        while True:
                            candidate = f"drawing{next_idx}.xml"
                            if candidate not in existing_drawings:
                                target_drawing_name = candidate
                                existing_drawings.add(candidate)
                                break
                            next_idx += 1
                        drawing_rel = ET.SubElement(rels_root, f"{{{REL_NS}}}Relationship")
                        drawing_rel.set("Id", drawing_rel_id)
                        drawing_rel.set("Type", f"{R_NS}/drawing")
                        drawing_rel.set("Target", f"../drawings/{target_drawing_name}")
                        drawing_el = ET.SubElement(sheet_root, f"{{{S_NS}}}drawing")
                        drawing_el.set(f"{{{R_NS}}}id", drawing_rel_id)
                        new_drawing_files.add(target_drawing_name)

                    else:
                        target_drawing_name = os.path.basename(drawing_rel.get("Target", ""))
                        drawing_rel_id = drawing_rel.get("Id")

                    if not target_drawing_name:
                        continue

                    state = _ensure_drawing_state(target_drawing_name)
                    if not state:
                        continue

                    image_name = _next_media_name()
                    with open(os.path.join(media_dir, image_name), "wb") as f:
                        f.write(png_bytes)
                    ext = os.path.splitext(image_name)[1].lstrip(".").lower()
                    if ext:
                        used_media_exts.add(ext)

                    rels_tree = state["rels_tree"]
                    rels_root_drawing = rels_tree.getroot()
                    img_rid = _xlsx_next_rel_id(rels_root_drawing)
                    rel = ET.SubElement(rels_root_drawing, f"{{{REL_NS}}}Relationship")
                    rel.set("Id", img_rid)
                    rel.set("Type", f"{R_NS}/image")
                    rel.set("Target", f"../media/{image_name}")

                    root = state["root"]
                    state["max_id"] = int(state.get("max_id", 0)) + 1
                    docpr_id = state["max_id"]

                    anchor = ET.SubElement(root, f"{{{XDR_NS}}}oneCellAnchor")
                    from_el = ET.SubElement(anchor, f"{{{XDR_NS}}}from")
                    ET.SubElement(from_el, f"{{{XDR_NS}}}col").text = str(col_idx)
                    ET.SubElement(from_el, f"{{{XDR_NS}}}colOff").text = "0"
                    ET.SubElement(from_el, f"{{{XDR_NS}}}row").text = str(row_idx)
                    ET.SubElement(from_el, f"{{{XDR_NS}}}rowOff").text = "0"

                    cx = max(1, int(round(img.size[0] * EMU_PER_PIXEL)))
                    cy = max(1, int(round(img.size[1] * EMU_PER_PIXEL)))

                    ET.SubElement(anchor, f"{{{XDR_NS}}}ext", {"cx": str(cx), "cy": str(cy)})
                    pic = ET.SubElement(anchor, f"{{{XDR_NS}}}pic")
                    nv_pic = ET.SubElement(pic, f"{{{XDR_NS}}}nvPicPr")
                    c_nv_pr = ET.SubElement(nv_pic, f"{{{XDR_NS}}}cNvPr")
                    c_nv_pr.set("id", str(docpr_id))
                    c_nv_pr.set("name", f"Picture {docpr_id}")
                    c_nv_pic_pr = ET.SubElement(nv_pic, f"{{{XDR_NS}}}cNvPicPr")
                    pic_locks = ET.SubElement(c_nv_pic_pr, f"{{{A_NS}}}picLocks")
                    pic_locks.set("noChangeAspect", "1")
                    pic_locks.set("noChangeArrowheads", "1")

                    blip_fill = ET.SubElement(pic, f"{{{XDR_NS}}}blipFill")
                    blip = ET.SubElement(blip_fill, f"{{{A_NS}}}blip")
                    blip.set(f"{{{R_NS}}}embed", img_rid)
                    stretch = ET.SubElement(blip_fill, f"{{{A_NS}}}stretch")
                    ET.SubElement(stretch, f"{{{A_NS}}}fillRect")

                    sp_pr = ET.SubElement(pic, f"{{{XDR_NS}}}spPr")
                    xfrm = ET.SubElement(sp_pr, f"{{{A_NS}}}xfrm")
                    off = ET.SubElement(xfrm, f"{{{A_NS}}}off")
                    off.set("x", "0")
                    off.set("y", "0")
                    ext = ET.SubElement(xfrm, f"{{{A_NS}}}ext")
                    ext.set("cx", str(cx))
                    ext.set("cy", str(cy))
                    _xlsx_set_anchor_size(anchor, cx, cy)

                    prst = ET.SubElement(sp_pr, f"{{{A_NS}}}prstGeom")
                    prst.set("prst", "rect")
                    ET.SubElement(prst, f"{{{A_NS}}}avLst")

                    ET.SubElement(anchor, f"{{{XDR_NS}}}clientData")

            _xlsx_update_content_types(tmpdir, used_media_exts, new_drawing_files)

            for sheet_file, (tree, nsmap) in sheet_tree_cache.items():
                path = os.path.join(tmpdir, "xl", "worksheets", sheet_file)
                _xml_write_tree(tree, path, namespace_map=nsmap)
            for sheet_file, (tree, nsmap) in sheet_rels_cache.items():
                rels_dir = os.path.join(tmpdir, "xl", "worksheets", "_rels")
                _xlsx_ensure_dir(rels_dir)
                path = os.path.join(rels_dir, f"{sheet_file}.rels")
                default_ns = nsmap.get("") if nsmap else REL_NS
                _xml_write_tree(tree, path, default_namespace=default_ns, namespace_map=nsmap)
            for drawing_name, state in drawing_cache.items():
                tree = state["tree"]
                rels_tree = state["rels_tree"]
                path = state["path"]
                rels_path = state["rels_path"]
                tree_nsmap = state.get("nsmap") or {"": XDR_NS}
                _xml_write_tree(tree, path, namespace_map=tree_nsmap)
                _xlsx_ensure_dir(os.path.dirname(rels_path))
                rels_nsmap = state.get("rels_nsmap") or {"": REL_NS}
                default_rels_ns = rels_nsmap.get("") if rels_nsmap else REL_NS
                _xml_write_tree(
                    rels_tree,
                    rels_path,
                    default_namespace=default_rels_ns,
                    namespace_map=rels_nsmap,
                )

        # 再計算フラグ
        xlsx_force_full_recalc(tmpdir)

        # 再パック
        with zipfile.ZipFile(dst_xlsx, 'w', zipfile.ZIP_DEFLATED) as zout:
            for root, _, files in os.walk(tmpdir):
                for fn in files:
                    full = os.path.join(root, fn)
                    zout.write(full, os.path.relpath(full, tmpdir))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    xlsx_update_formula_caches(dst_xlsx, formula_cells)

# ------------ PDF / JPEG ------------
def libreoffice_to_pdf(input_path: str, out_dir: str) -> str:
    if not os.path.isdir(out_dir): os.makedirs(out_dir, exist_ok=True)
    run([
        "soffice", "--headless", "--nologo", "--nodefault", "--nolockcheck", "--norestore",
        "--convert-to", "pdf", "--outdir", out_dir, input_path
    ])
    base = os.path.splitext(os.path.basename(input_path))[0]
    pdf_path = os.path.join(out_dir, f"{base}.pdf")
    if not os.path.exists(pdf_path):
        for fn in os.listdir(out_dir):
            if fn.lower().endswith(".pdf"):
                pdf_path = os.path.join(out_dir, fn); break
    if not os.path.exists(pdf_path): raise RuntimeError("PDF not produced by LibreOffice.")
    return pdf_path

def pdf_to_jpegs(pdf_path: str, dpi: int, pages: List[int]) -> List[Tuple[int, bytes]]:
    out: List[Tuple[int, bytes]] = []
    for p in pages:
        prefix = f"{pdf_path}-p{p}"
        run(["pdftoppm", "-jpeg", "-r", str(dpi), "-f", str(p), "-l", str(p), pdf_path, prefix])
        jpg = f"{prefix}-1.jpg"
        if not os.path.exists(jpg):
            alt = f"{prefix}.jpg"
            jpg = alt if os.path.exists(alt) else jpg
        if not os.path.exists(jpg):
            for fn in os.listdir(os.path.dirname(pdf_path)):
                if fn.startswith(os.path.basename(prefix)) and fn.lower().endswith(".jpg"):
                    jpg = os.path.join(os.path.dirname(pdf_path), fn); break
        if not os.path.exists(jpg): raise RuntimeError(f"Failed to create JPEG page {p}")
        with open(jpg, "rb") as f: out.append((p, f.read()))
        try: os.remove(jpg)
        except: pass
    return out

# ------------ API ------------
@app.get("/healthz")
def healthz():
    # Cloud Run 起動判定用：即応答
    return {"ok": True}

def _guess_office_ext_from_mime(mime: str) -> Optional[str]:
    if not mime:
        return None
    mime = mime.split(";", 1)[0].strip().lower()
    mapping = {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.ms-word.document.macroenabled.12": ".docm",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/vnd.ms-excel.sheet.macroenabled.12": ".xlsm",
    }
    ext = mapping.get(mime)
    if ext in {".docm", ".xlsm"}:
        return ".docx" if ext == ".docm" else ".xlsx"
    return ext


def _sniff_office_extension(data: Optional[bytes]) -> Optional[str]:
    if not data:
        return None
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            names = zf.namelist()
    except zipfile.BadZipFile:
        return None
    for prefix, ext in (("word/", ".docx"), ("xl/", ".xlsx")):
        if any(name.startswith(prefix) for name in names):
            return ext
    return None


def _load_template_from_data_string(text: str) -> Tuple[Optional[bytes], Optional[str]]:
    value = (text or "").strip()
    if not value:
        return None, None
    if value.startswith("data:"):
        header, _, payload = value.partition(",")
        if not payload:
            return None, None
        mime = ""
        if header.startswith("data:"):
            mime = header[5:]
            if ";" in mime:
                mime = mime.split(";", 1)[0]
        if ";base64" in header:
            data = _decode_base64_payload(payload)
        else:
            data = urllib.parse.unquote_to_bytes(payload)
        return data, _guess_office_ext_from_mime(mime)
    if value.startswith("base64:"):
        _, _, payload = value.partition(":")
        return _decode_base64_payload(payload), None
    if value.startswith("base64,"):
        return _decode_base64_payload(value[7:]), None
    return None, None


def _download_template_from_url(url: str) -> Tuple[Optional[bytes], Optional[str]]:
    source = (url or "").strip()
    if not source:
        return None, None
    try:
        resp = requests.get(source, timeout=30)
        resp.raise_for_status()
    except Exception:
        return None, None
    data = resp.content
    mime = resp.headers.get("Content-Type", "")
    ext = _guess_office_ext_from_mime(mime)
    if not ext:
        path = urllib.parse.urlparse(source).path
        ext = file_ext_lower(path)
    return data, ext


@app.post("/merge")
async def merge(
    file: Optional[UploadFile] = File(None),
    mapping_text: str = Form(""),
    filename: str = Form("document"),
    jpeg_dpi: int = Form(150),
    jpeg_pages: str = Form("1"),
    return_pdf: bool = Form(True),
    return_jpegs: bool = Form(True),
    return_document: bool = Form(True),
    file_data_uri: str = Form(""),
    file_url: str = Form(""),
    x_auth_id: Optional[str] = Header(None, alias="X-Auth-Id"),
    authorization: Optional[str] = Header(None),
):
    from pypdf import PdfReader  # lazy import

    try:
        auth_id = x_auth_id or ""
        if not auth_id and authorization:
            auth_id = authorization.strip()
            if auth_id.lower().startswith("bearer "):
                auth_id = auth_id[7:].strip()

        if not auth_id:
            return err("missing auth_id", status=401)

        if not validate_auth_id(auth_id):
            return err("invalid auth_id", status=401)

        template_bytes: Optional[bytes] = None
        ext = ""
        if file is not None:
            template_bytes = await file.read()
            ext = file_ext_lower(file.filename or "")

        if (template_bytes is None or not template_bytes) and file_data_uri:
            template_bytes, inferred_ext = _load_template_from_data_string(file_data_uri)
            if inferred_ext and not ext:
                ext = inferred_ext

        if (template_bytes is None or not template_bytes) and file_url:
            template_bytes, inferred_ext = _download_template_from_url(file_url)
            if inferred_ext and not ext:
                ext = inferred_ext

        if not template_bytes:
            return err("file must be provided via upload, data URI, or URL", 400)

        sniffed_ext = _sniff_office_extension(template_bytes)
        if sniffed_ext and ext not in {".docx", ".xlsx"}:
            ext = sniffed_ext
        if not ext:
            ext = sniffed_ext or ""

        if ext not in {".docx", ".xlsx"}:
            return err("file must be .docx or .xlsx", 400)

        text_map, image_map, loop_map = parse_mapping_text(mapping_text or "")

        with tempfile.TemporaryDirectory() as td:
            src = os.path.join(td, f"src{ext}")
            with open(src, "wb") as f:
                f.write(template_bytes)

            rendered = os.path.join(td, f"rendered{ext}")
            if ext == ".docx":
                docx_render(src, rendered, text_map, image_map, loop_map)
            else:
                xlsx_patch_and_place(src, rendered, text_map, image_map, loop_map)

            pdf_bytes: Optional[bytes] = None
            pdf_path: Optional[str] = None
            total_pages: Optional[int] = None

            if return_pdf or return_jpegs:
                pdf_dir = os.path.join(td, "pdf")
                pdf_path = libreoffice_to_pdf(rendered, pdf_dir)
                with open(pdf_path, "rb") as f:
                    pdf_bytes = f.read()
                total_pages = len(PdfReader(io.BytesIO(pdf_bytes)).pages)

            selected: List[int] = []
            jpgs: List[Tuple[int, bytes]] = []
            if return_jpegs:
                if total_pages is None or pdf_path is None or pdf_bytes is None:
                    raise RuntimeError("JPEG output requested but PDF generation failed")
                selected = parse_pages_arg(jpeg_pages, total_pages)
                jpgs = pdf_to_jpegs(pdf_path, jpeg_dpi, selected)

            response: Dict[str, object] = {}

            base_filename = (filename or "document").strip().rstrip(".")
            if not base_filename:
                base_filename = "document"

            if return_pdf:
                if pdf_bytes is None:
                    raise RuntimeError("PDF output requested but PDF generation failed")
                response["file_name"] = base_filename + ".pdf"
                response["pdf_data_uri"] = data_uri("application/pdf", pdf_bytes)
                if total_pages is not None:
                    response["total_pdf_pages"] = total_pages

            if return_jpegs:
                response["jpeg_dpi"] = jpeg_dpi
                response["jpeg_pages"] = selected
                response["jpeg_data_uris"] = [
                    {"page": p, "data_uri": data_uri("image/jpeg", b)}
                    for p, b in jpgs
                ]
                if total_pages is not None:
                    response["total_pdf_pages"] = total_pages

            if return_document:
                with open(rendered, "rb") as f:
                    doc_bytes = f.read()
                mime = DOC_MIME_MAP.get(ext, "application/octet-stream")
                response["document_file_name"] = base_filename + ext
                response["document_data_uri"] = data_uri(mime, doc_bytes)

            if not response:
                response["message"] = "No outputs requested"

            return ok(response)
    except Exception as e:
        # 500 ではなく 400 を返す（Bubble 側で原因が見える）
        return err(str(e), 400)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, workers=1, lifespan="off")
