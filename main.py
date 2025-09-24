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

def _word_collect_text_nodes(root) -> List[Tuple[LET._Element, int, int]]:
    nodes: List[Tuple[LET._Element, int, int]] = []
    cursor = 0
    for t in root.iter(f"{{{W_NS}}}t"):
        text = t.text or ""
        start = cursor
        cursor += len(text)
        nodes.append((t, start, cursor))
    return nodes

def _word_replace_range(root, start: int, end: int, replacement: str):
    nodes = _word_collect_text_nodes(root)
    inserted = False
    for node, node_start, node_end in nodes:
        if node_end <= start or node_start >= end:
            continue
        text = node.text or ""
        local_start = max(0, start - node_start)
        local_end = min(len(text), end - node_start)
        before = text[:local_start]
        after = text[local_end:]
        if not inserted:
            node.text = before + replacement + after
            inserted = True
        else:
            node.text = before + after

def _word_convert_placeholders(root, size_hints: Dict[str, Optional[float]]):
    while True:
        nodes = _word_collect_text_nodes(root)
        if not nodes:
            break
        full_text = "".join((node.text or "") for node, _, _ in nodes)
        m_img = IMG_TAG_INLINE.search(full_text)
        if m_img:
            var = m_img.group("var")
            size = parse_size_mm(m_img.group("size") or "")
            if size is not None:
                size_hints.setdefault(var, size)
            _word_replace_range(root, m_img.start(), m_img.end(), f"{{{{ {var} }}}}")
            continue
        m_txt = TXT_TAG_INLINE.search(full_text)
        if m_txt:
            var = m_txt.group("var")
            _word_replace_range(root, m_txt.start(), m_txt.end(), f"{{{{ {var} }}}}")
            continue
        break

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
        with zipfile.ZipFile(out_docx, 'w', zipfile.ZIP_DEFLATED) as zout:
            for root, _, files in os.walk(tmpdir):
                for fn in files:
                    full = os.path.join(root, fn)
                    zout.write(full, os.path.relpath(full, tmpdir))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    return size_hints

def docx_render(in_docx: str, out_docx: str, text_map: Dict[str, str], image_map: Dict[str, Dict]):
    from docxtpl import DocxTemplate, InlineImage
    from docx.shared import Mm
    import requests

    tmp = in_docx + ".jinja.docx"
    size_hints = docx_convert_tags_to_jinja(in_docx, tmp)

    doc = DocxTemplate(tmp)
    ctx: Dict[str, object] = {}
    for k, v in text_map.items(): ctx[k] = v
    for k, meta in image_map.items():
        r = requests.get(meta["url"], timeout=20); r.raise_for_status()
        bio = io.BytesIO(r.content)
        mm = meta.get("mm") or size_hints.get(k)
        ctx[k] = InlineImage(doc, bio, width=Mm(mm)) if mm else InlineImage(doc, bio)
    doc.render(ctx)
    doc.save(out_docx)
    os.remove(tmp)

    # 図形内テキストなどに残った {{var}} を後置換
    tmpdir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(out_docx, 'r') as zin:
            zin.extractall(tmpdir)
        for p in _word_content_xmls(tmpdir):
            s = open(p, "r", encoding="utf-8").read()
            for k, v in text_map.items():
                s = s.replace(f"{{{{ {k} }}}}", v).replace(f"{{{{{k}}}}}", v)
            open(p, "w", encoding="utf-8").write(s)
        with zipfile.ZipFile(out_docx, 'w', zipfile.ZIP_DEFLATED) as zout:
            for root, _, files in os.walk(tmpdir):
                for fn in files:
                    full = os.path.join(root, fn)
                    zout.write(full, os.path.relpath(full, tmpdir))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

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
                replaced = original
                for k, v in text_map.items(): replaced = replaced.replace(f"{{{k}}}", _with_newlines(v))

                # 書き戻し
                for r in list(si): si.remove(r)
                t = ET.SubElement(si, f"{{{ns['s']}}}t"); t.text = replaced

                # 完全一致 = 数値候補
                for k, v in text_map.items():
                    if original == f"{{{k}}}":
                        num, _ = parse_numberlike(v)
                        numeric_candidates[idx] = ((num is not None), num)
                        break

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
                                c.attrib.pop("t", None)
                                try: c.remove(is_node)
                                except: pass
                                if v_node is not None:
                                    c.remove(v_node)
                                continue
                            # テキスト置換／数値化
                            replaced = txt
                            for k, v in text_map.items(): replaced = replaced.replace(f"{{{k}}}", _with_newlines(v))
                            for k, v in text_map.items():
                                if txt == f"{{{k}}}":
                                    num, _ = parse_numberlike(v)
                                    if num is not None:
                                        c.set("t", "n")
                                        try: c.remove(is_node)
                                        except: pass
                                        if v_node is None: v_node = ET.SubElement(c, f"{{{ns['s']}}}v")
                                        v_node.text = str(num)
                                        break
                            else:
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
