import os
import re
import io
import base64
import tempfile
import zipfile
import shutil
import subprocess
import xml.etree.ElementTree as ET
from decimal import Decimal
from lxml import etree as LET
from jinja2 import Environment, DebugUndefined
from typing import Dict, Tuple, List, Optional

from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse

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

# ------------ tag & number parsing ------------
VAR_NAME = r"[A-Za-z_][A-Za-z0-9_]*"
IMG_KEY_PATTERN = re.compile(rf"^\{{\[(?P<var>{VAR_NAME})\](?::(?P<size>[^}}]+))?\}}$")
TXT_KEY_PATTERN = re.compile(rf"^\{{(?P<var>{VAR_NAME})\}}$")
IMG_TAG_PATTERN = re.compile(rf"\{{\[(?P<var>{VAR_NAME})\](?::(?P<size>[^}}]+))?\}}")
WORD_INLINE_PATTERN = re.compile(
    rf"(?<!\{{)\{{\s*(?:\[\s*(?P<img>{VAR_NAME})\s*\](?::(?P<size>[^}}]+))?|(?P<txt>{VAR_NAME}))\s*\}}(?!\}})"
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

def _format_formula_value(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value == value.to_integral():
            value = int(value)
        else:
            value = float(value)
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)

def _with_newlines(v: str) -> str:
    return (v or "").replace("<br>", "\n")

def parse_mapping_text(raw: str) -> Tuple[Dict[str, str], Dict[str, Dict]]:
    """
    {a}:X,{b}:Y でも 改行でもOK。URL内カンマ保護。
    {[img]:50mm}:URL / {[img]}:URL
    """
    text_map: Dict[str, str] = {}
    image_map: Dict[str, Dict] = {}
    if not raw: return text_map, image_map

    SAFE = "\u241B"  # protect ://
    protected = [line.replace("://", SAFE) for line in raw.splitlines()]
    joined = "\n".join(protected)

    items: List[str] = []
    for line in joined.splitlines():
        if not line.strip(): continue
        for seg in line.split(","):
            seg = seg.strip()
            if seg: items.append(seg.replace(SAFE, "://"))

    for seg in items:
        if "}" not in seg:
            continue
        close = seg.find("}")
        key = seg[:close+1].strip()
        value = seg[close+1:].lstrip(":").strip()
        if not key:
            continue

        m_img = IMG_KEY_PATTERN.match(key)
        if m_img:
            v = m_img.group("var"); mm = parse_size_mm(m_img.group("size") or "")
            image_map[v] = {"url": value, "mm": mm}; continue

        m_txt = TXT_KEY_PATTERN.match(key)
        if m_txt:
            v = m_txt.group("var"); text_map[v] = _with_newlines(value); continue

    return text_map, image_map

def _apply_text_tokens(text: Optional[str], text_map: Dict[str, str]) -> Optional[str]:
    if text is None or not text_map:
        return text
    result = text
    for key, value in text_map.items():
        result = result.replace(f"{{{key}}}", value)
    return result

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
EMU_PER_INCH = 914400

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

def _word_convert_placeholders(root, size_hints: Dict[str, Optional[float]]):
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
        _word_splice_text(nodes, match.start(), match.end(), f"{{{{ {var} }}}}")

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

def docx_convert_tags_to_jinja(in_docx: str, out_docx: str) -> Dict[str, Optional[float]]:
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
            _word_convert_placeholders(root, size_hints)
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

def docx_render(in_docx: str, out_docx: str, text_map: Dict[str, str], image_map: Dict[str, Dict]):
    from docxtpl import DocxTemplate, InlineImage
    from docx.shared import Mm
    import requests

    tmp = in_docx + ".jinja.docx"
    size_hints = docx_convert_tags_to_jinja(in_docx, tmp)

    doc = DocxTemplate(tmp)
    ctx: Dict[str, object] = {}
    for k, v in text_map.items():
        ctx[k] = v
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
def _xlsx_sheet_map(extracted_dir: str) -> Dict[str, Tuple[Optional[str], Optional[int]]]:
    mapping: Dict[str, Tuple[Optional[str], Optional[int]]] = {}
    workbook_xml = os.path.join(extracted_dir, "xl", "workbook.xml")
    rels_xml = os.path.join(extracted_dir, "xl", "_rels", "workbook.xml.rels")
    if not os.path.exists(workbook_xml):
        return mapping

    rid_to_target: Dict[str, str] = {}
    if os.path.exists(rels_xml):
        tree = ET.parse(rels_xml); root = tree.getroot()
        for rel in root.findall(f".//{{{R_NS}}}Relationship"):
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
    tree = ET.parse(p); root = tree.getroot()
    calcPr = root.find("s:calcPr", ns) or ET.SubElement(root, f"{{{ns['s']}}}calcPr")
    calcPr.set("calcMode", "auto")
    calcPr.set("fullCalcOnLoad", "1")
    calcPr.set("calcOnSave", "1")
    calcPr.set("forceFullCalc", "1")
    chain = os.path.join(extracted_dir, "xl", "calcChain.xml")
    if os.path.exists(chain):
        try: os.remove(chain)
        except: pass
    tree.write(p, encoding="utf-8", xml_declaration=True)

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

    computed: Dict[Tuple[str, str], Optional[str]] = {}
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
        address = f"{_xlsx_escape_sheet_name(sheet_name)}!{cell_ref}"
        try:
            value = evaluator.evaluate(address)
        except Exception:
            continue
        if hasattr(value, "value"):
            value = value.value
        formatted = _format_formula_value(value)
        if formatted is None:
            continue
        computed[(sheet_file, cell_ref)] = formatted

    if not computed:
        return

    tmpdir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(xlsx_path, 'r') as zin:
            zin.extractall(tmpdir)
        ns = {"s": S_NS}
        updated: Dict[str, ET.ElementTree] = {}
        for info in formula_cells:
            sheet_file = info.get("sheet_file")
            cell_ref = info.get("cell_ref")
            if not sheet_file or not cell_ref:
                continue
            key = (sheet_file, cell_ref)
            if key not in computed:
                continue
            sheet_path = os.path.join(tmpdir, "xl", "worksheets", sheet_file)
            if not os.path.exists(sheet_path):
                continue
            if sheet_file not in updated:
                updated[sheet_file] = ET.parse(sheet_path)
            tree = updated[sheet_file]
            root = tree.getroot()
            cell = root.find(f".//s:c[@r='{cell_ref}']", ns)
            if cell is None:
                continue
            v_node = cell.find("s:v", ns)
            if v_node is None:
                v_node = ET.SubElement(cell, f"{{{ns['s']}}}v")
            v_node.text = computed[key]
            if computed[key] in ("1", "0") and info.get("boolean", False):
                cell.set("t", "b")
            elif cell.get("t") == "str":
                cell.attrib.pop("t")
        for sheet_file, tree in updated.items():
            sheet_path = os.path.join(tmpdir, "xl", "worksheets", sheet_file)
            tree.write(sheet_path, encoding="utf-8", xml_declaration=True)
        with zipfile.ZipFile(xlsx_path, 'w', zipfile.ZIP_DEFLATED) as zout:
            for root_dir, _, files in os.walk(tmpdir):
                for fn in files:
                    full = os.path.join(root_dir, fn)
                    zout.write(full, os.path.relpath(full, tmpdir))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

def xlsx_patch_and_place(src_xlsx: str, dst_xlsx: str, text_map: Dict[str, str], image_map: Dict[str, Dict]):
    """
    1) XML直編集で {var} を置換（完全一致は数値化、<br>→\n）、{[img]} はセルから除去し placements に記録
    2) fullCalcOnLoad=1 で再計算
    3) openpyxl で placements に新規画像挿入（既存図形/グラフは原則維持）
    """
    ns = {"s": S_NS}
    tmpdir = tempfile.mkdtemp()
    placements: List[Dict[str, object]] = []
    formula_cells: List[Dict[str, object]] = []
    try:
        with zipfile.ZipFile(src_xlsx, 'r') as zin:
            zin.extractall(tmpdir)

        # sharedStrings
        sst_path = os.path.join(tmpdir, "xl", "sharedStrings.xml")
        numeric_candidates: Dict[int, Tuple[bool, Optional[float]]] = {}
        img_sst_idx: Dict[int, Tuple[str, Optional[float]]] = {}

        if os.path.exists(sst_path):
            tree = ET.parse(sst_path); root = tree.getroot(); idx = -1
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
                    continue

                # テキスト置換
                replaced = _apply_text_tokens(original, text_map)

                # 書き戻し
                for r in list(si): si.remove(r)
                t = ET.SubElement(si, f"{{{ns['s']}}}t"); t.text = replaced

                # 完全一致 = 数値候補
                txt_match = TXT_KEY_PATTERN.match(original or "")
                if txt_match:
                    var_name = txt_match.group("var")
                    mapped = text_map.get(var_name)
                    if mapped is not None:
                        num, _ = parse_numberlike(mapped)
                        numeric_candidates[idx] = ((num is not None), num)

            tree.write(sst_path, encoding="utf-8", xml_declaration=True)

        # worksheets
        ws_dir = os.path.join(tmpdir, "xl", "worksheets")
        sheet_map = _xlsx_sheet_map(tmpdir)
        if os.path.isdir(ws_dir):
            for fn in os.listdir(ws_dir):
                if not fn.endswith(".xml"): continue
                p = os.path.join(ws_dir, fn)
                tree = ET.parse(p); root = tree.getroot()

                sheet_name, sheet_index = sheet_map.get(fn, (None, None))
                if sheet_index is None:
                    m = re.findall(r'\d+', fn)
                    sheet_index = int(m[0]) - 1 if m else 0

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
                            "boolean": (c.get("t") == "b"),
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

                tree.write(p, encoding="utf-8", xml_declaration=True)

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

    # openpyxl で画像を追加（既存図形・グラフは通常維持）
    if placements and image_map:
        from openpyxl import load_workbook
        from openpyxl.drawing.image import Image as XLImage
        from PIL import Image as PILImage
        import requests

        wb = load_workbook(dst_xlsx)
        for item in placements:
            sheet_file = item.get("sheet_file")
            cell_ref = item.get("cell_ref")
            var = item.get("var")
            size_hint = item.get("size_hint")
            sheet_index = item.get("sheet_index") or 0
            sheet_name = item.get("sheet_name")
            meta = image_map.get(var)
            if not meta: continue
            url = meta["url"]
            mm = meta.get("mm") or size_hint
            r = requests.get(url, timeout=20); r.raise_for_status()
            img = PILImage.open(io.BytesIO(r.content)).convert("RGBA")
            if mm:
                px = mm_to_pixels(mm, dpi=96)
                w, h = img.size
                new_h = int(round(h * (px / w))) if w else h
                img = img.resize((px, new_h), PILImage.LANCZOS)
            bio = io.BytesIO(); img.save(bio, format="PNG"); bio.seek(0)
            ws = None
            if sheet_name and sheet_name in wb:
                ws = wb[sheet_name]
            elif isinstance(sheet_index, int) and 0 <= sheet_index < len(wb.worksheets):
                ws = wb.worksheets[sheet_index]
            if ws is None:
                ws = wb.active
            ws.add_image(XLImage(bio), cell_ref)
        wb.save(dst_xlsx)

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

@app.post("/merge")
async def merge(
    file: UploadFile = File(...),
    mapping_text: str = Form(""),
    filename: str = Form("document"),
    jpeg_dpi: int = Form(150),
    jpeg_pages: str = Form("1"),
):
    from pypdf import PdfReader  # lazy import

    try:
        ext = file_ext_lower(file.filename or "")
        if ext not in [".docx", ".xlsx"]:
            return err("file must be .docx or .xlsx", 400)

        text_map, image_map = parse_mapping_text(mapping_text or "")

        with tempfile.TemporaryDirectory() as td:
            src = os.path.join(td, f"src{ext}")
            with open(src, "wb") as f: f.write(await file.read())

            rendered = os.path.join(td, f"rendered{ext}")
            if ext == ".docx":
                docx_render(src, rendered, text_map, image_map)
            else:
                xlsx_patch_and_place(src, rendered, text_map, image_map)

            pdf_dir = os.path.join(td, "pdf")
            pdf_path = libreoffice_to_pdf(rendered, pdf_dir)

            with open(pdf_path, "rb") as f:
                total_pages = len(PdfReader(f).pages)

            selected = parse_pages_arg(jpeg_pages, total_pages)
            jpgs = pdf_to_jpegs(pdf_path, jpeg_dpi, selected)

            pdf_b = open(pdf_path, "rb").read()
            return ok({
                "file_name": (filename or "document").strip().rstrip(".") + ".pdf",
                "pdf_data_uri": data_uri("application/pdf", pdf_b),
                "jpeg_dpi": jpeg_dpi,
                "jpeg_pages": selected,
                "jpeg_data_uris": [{"page": p, "data_uri": data_uri("image/jpeg", b)} for p, b in jpgs],
                "total_pdf_pages": total_pages
            })
    except Exception as e:
        # 500 ではなく 400 を返す（Bubble 側で原因が見える）
        return err(str(e), 400)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, workers=1, lifespan="off")
