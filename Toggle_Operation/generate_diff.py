#!/usr/bin/env python3
import json, html, argparse, datetime
from pathlib import Path
from collections import OrderedDict

# ==========================================================
# Utility Functions
# ==========================================================
def load_json(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f, object_pairs_hook=OrderedDict)

def esc(s):
    return html.escape(str(s), quote=False)

def pretty(obj):
    """Preserve full JSON structure & key order with indentation"""
    return json.dumps(obj, indent=2, ensure_ascii=False)

def lines(obj):
    return len(pretty(obj).splitlines())

def pad_space(lines, cls):
    """Generate matching blank block for alignment"""
    return f"<div class='{cls}' style='white-space:pre-wrap'>{'<br>' * lines}</div>"

# ==========================================================
# Diff Logic
# ==========================================================
def diff_value(a, b):
    """Highlight value differences"""
    if a == b:
        return esc(json.dumps(a, ensure_ascii=False)), esc(json.dumps(b, ensure_ascii=False))
    return (
        f"<span style='background:#fff3cd;border-left:4px solid #ffb300;padding:2px 4px;'>{esc(json.dumps(a, ensure_ascii=False))}</span>",
        f"<span style='background:#fff3cd;border-left:4px solid #ffb300;padding:2px 4px;'>{esc(json.dumps(b, ensure_ascii=False))}</span>"
    )

def diff_block(a, b):
    """Recursive block-level comparison preserving structure"""
    if a == b:
        return esc(pretty(a)), esc(pretty(b))

    # Handle added / deleted blocks
    if a is None and b is not None:
        h = lines(b)
        return pad_space(h, "block-added"), f"<div class='block-added'><pre>{esc(pretty(b))}</pre></div>"
    if b is None and a is not None:
        h = lines(a)
        return f"<div class='block-deleted'><pre>{esc(pretty(a))}</pre></div>", pad_space(h, "block-deleted")

    # Dict comparison preserving order
    if isinstance(a, dict) and isinstance(b, dict):
        keys = list(OrderedDict.fromkeys(list(a.keys()) + list(b.keys())))
        ao, bo = [], []
        for k in keys:
            av, bv = a.get(k), b.get(k)
            if isinstance(av, (dict, list)) or isinstance(bv, (dict, list)):
                xo, xn = diff_block(av, bv)
            else:
                xo, xn = diff_value(av, bv)
            ao.append(f'  "{k}": {xo}')
            bo.append(f'  "{k}": {xn}')
        return "{\n" + ",\n".join(ao) + "\n}", "{\n" + ",\n".join(bo) + "\n}"

    # List comparison preserving brackets
    if isinstance(a, list) and isinstance(b, list):
        key = None
        if all(isinstance(x, dict) for x in a + b):
            if any("resourceType" in x for x in a + b): key = "resourceType"
            elif any("name" in x for x in a + b): key = "name"

        m1 = {(x.get(key) if key else i): x for i, x in enumerate(a)}
        m2 = {(x.get(key) if key else i): x for i, x in enumerate(b)}
        keys = list(OrderedDict.fromkeys(list(m1.keys()) + list(m2.keys())))
        ao, bo = [], []
        for k in keys:
            av, bv = m1.get(k), m2.get(k)
            if av is None and bv is not None:
                h = lines(bv)
                ao.append(pad_space(h, "block-added"))
                bo.append(f"<div class='block-added'><pre>{esc(pretty(bv))}</pre></div>")
            elif bv is None and av is not None:
                h = lines(av)
                ao.append(f"<div class='block-deleted'><pre>{esc(pretty(av))}</pre></div>")
                bo.append(pad_space(h, "block-deleted"))
            else:
                xo, xn = diff_block(av, bv)
                ao.append(xo)
                bo.append(xn)
        return "[\n" + ",\n".join(ao) + "\n]", "[\n" + ",\n".join(bo) + "\n]"

    return diff_value(a, b)

# ==========================================================
# Flattening Helpers
# ==========================================================
def flatten_arrays(prefix, obj, result):
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, list):
                result[new_key] = v
            else:
                flatten_arrays(new_key, v, result)

def find_toggles(obj):
    if isinstance(obj, dict):
        if "toggles" in obj:
            return obj["toggles"]
        for v in obj.values():
            res = find_toggles(v)
            if res:
                return res
    return None

# ==========================================================
# Table Builder
# ==========================================================
def build_table(name, a1, a2, f1, f2):
    arr1 = a1 if isinstance(a1, list) else []
    arr2 = a2 if isinstance(a2, list) else []
    key = None
    if name.endswith("checkPermissions") or name == "checkPermissions":
        key = "action"
    elif any(isinstance(x, dict) and "resourceType" in x for x in arr1 + arr2):
        key = "resourceType"

    m1 = {(x.get(key) if key else i): x for i, x in enumerate(arr1)}
    m2 = {(x.get(key) if key else i): x for i, x in enumerate(arr2)}
    keys = list(OrderedDict.fromkeys(list(m1.keys()) + list(m2.keys())))

    rows = []
    for k in keys:
        o, n = m1.get(k), m2.get(k)
        if o is None and n is None:
            continue
        if o is None:
            status = "added"
        elif n is None:
            status = "deleted"
        elif o == n:
            continue
        else:
            status = "modified"

        # 🔧 Dynamic display label: use 'name' if available
        display_label = k
        if isinstance(n, dict) and "name" in n and isinstance(n["name"], str):
            display_label = n["name"]
        elif isinstance(o, dict) and "name" in o and isinstance(o["name"], str):
            display_label = o["name"]

        xo, xn = diff_block(o, n)
        xo = f"<pre style='white-space:pre-wrap;margin:0;'>{xo}</pre>"
        xn = f"<pre style='white-space:pre-wrap;margin:0;'>{xn}</pre>"
        summary = f"{name}:{display_label} – {status}"

        rows.append(f"""
<tr><td colspan='4' style='padding:0;border:none;'>
<details><summary style='padding:6px 10px;cursor:pointer;'>{esc(summary)}</summary>
<table style='width:100%;table-layout:fixed;border-collapse:collapse;'>
<tr><th style='width:50%;text-align:left;'>{esc(f1)}</th><th style='width:50%;text-align:left;'>{esc(f2)}</th></tr>
<tr><td>{xo}</td><td>{xn}</td></tr>
</table></details></td></tr>""")

    if not rows:
        return ""
    return f"<details id='{esc(name)}'><summary>{esc(name)}</summary><table>{''.join(rows)}</table></details>"

# ==========================================================
# HTML Template
# ==========================================================
HTML_HEAD = """<!DOCTYPE html>
<html><head><meta charset='utf-8'><title>Toggle Diff Report</title>
<style>
body{font-family:Segoe UI,Arial;background:#fafafa;margin:0;}
header{position:sticky;top:0;background:#fff;padding:10px 16px;box-shadow:0 2px 4px rgba(0,0,0,.1);}
.legend span{padding:4px 10px;margin:2px 4px;border-radius:4px;font-size:13px;}
.added{background:#e8f5e9;border-left:4px solid #388e3c;}
.deleted{background:#ffebee;border-left:4px solid #c62828;}
.modified{background:#fff8e1;border-left:4px solid #ff8f00;}
.block-added{background:#e8f5e9;border-left:4px solid #388e3c;}
.block-deleted{background:#ffebee;border-left:4px solid #c62828;}
table th,td{vertical-align:top;border-right:1px solid #ddd;padding:10px;}
table td:last-child,th:last-child{border-right:none;}
details{background:#fff;margin:12px;padding:10px 16px;border-radius:8px;box-shadow:0 2px 6px rgba(0,0,0,.06);}
details summary{font-weight:600;cursor:pointer;font-size:16px;}
#sidebarBtn{position:fixed;top:10px;left:10px;background:#1976d2;color:#fff;border:none;border-radius:4px;padding:10px 14px;cursor:pointer;z-index:1000;}
#sidebar{position:fixed;top:0;left:0;width:240px;height:100%;background:#fff;box-shadow:2px 0 8px rgba(0,0,0,.1);transform:translateX(-260px);transition:.3s;z-index:999;overflow-y:auto;padding-top:60px;}
#sidebar.open{transform:translateX(0);}
#sidebar a{display:block;padding:10px 20px;color:#333;text-decoration:none;border-left:4px solid transparent;}
#sidebar a.active{background:#e3f2fd;border-left:4px solid #1976d2;font-weight:600;}
#backToTop{display:none;position:fixed;bottom:20px;right:25px;background:#1976d2;color:#fff;border:none;border-radius:50%;width:40px;height:40px;font-size:18px;cursor:pointer;}
pre{margin:0;white-space:pre-wrap;font-family:Consolas,monospace;}
</style>
<script>
function toggleSidebar(){document.getElementById('sidebar').classList.toggle('open');}
function scrollToSection(id){document.getElementById('sidebar').classList.remove('open');document.getElementById(id).scrollIntoView({behavior:'smooth'});}
window.addEventListener('scroll',()=>{
const secs=document.querySelectorAll('details');let a=null;
secs.forEach(sec=>{const r=sec.getBoundingClientRect();if(r.top<window.innerHeight/3&&r.bottom>100)a=sec.id;});
document.querySelectorAll('#sidebar a').forEach(x=>x.classList.toggle('active',x.dataset.target===a));
document.getElementById('backToTop').style.display=window.scrollY>600?'block':'none';});
function backToTop(){window.scrollTo({top:0,behavior:'smooth'});}
</script></head><body>
<button id='sidebarBtn' onclick='toggleSidebar()'>☰ Toggles</button>
<div id='sidebar'>{links}</div>
<header><h2>Toggle Diff Report</h2>
<div class='legend'><span class='added'>🟩 Added</span><span class='deleted'>🟥 Deleted</span><span class='modified'>🟧 Modified</span></div>
<p><b>{v1}</b> → <b>{v2}</b> | {dt}</p></header>
"""

HTML_FOOT = "<button id='backToTop' onclick='backToTop()'>↑</button></body></html>"

# ==========================================================
# Renderer
# ==========================================================
def render(v1, v2, out):
    j1, j2 = load_json(v1), load_json(v2)
    t1, t2 = find_toggles(j1), find_toggles(j2)
    if not t1 and not t2:
        print("No 'toggles' found.")
        return

    a1, a2 = {}, {}
    flatten_arrays("", t1 or {}, a1)
    flatten_arrays("", t2 or {}, a2)
    keys = sorted(set(a1) | set(a2))

    sections, links = [], []
    for k in keys:
        part = build_table(k, a1.get(k), a2.get(k), v1.name, v2.name)
        if part:
            sections.append(part)
            links.append(f"<a href='javascript:void(0)' data-target='{k}' onclick='scrollToSection(\"{k}\")'>{esc(k)}</a>")

    html = (HTML_HEAD + "\n".join(sections) + HTML_FOOT)\
        .replace("{v1}", v1.name)\
        .replace("{v2}", v2.name)\
        .replace("{dt}", datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))\
        .replace("{links}", "\n".join(links))

    Path(out).write_text(html, encoding="utf-8")
    print(f"✅ Toggle Diff Report generated: {out}")

# ==========================================================
# Entry Point
# ==========================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument("-v1", required=True)
    p.add_argument("-v2", required=True)
    p.add_argument("-o", default="Toggle_Diff_Report.html")
    a = p.parse_args()
    render(Path(a.v1), Path(a.v2), a.o)

if __name__ == "__main__":
    main()
