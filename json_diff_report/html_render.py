
from __future__ import annotations
import json
from html import escape
from typing import Any, List, Set, Tuple

CSS = """
<style>
  body { font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif; margin:24px; line-height:1.5; }
  h1,h2,h3 { margin-top:0; }
  .section { border:1px solid #ddd; border-radius:12px; padding:16px; margin-bottom:16px; }
  table { width:100%; border-collapse:collapse; margin-top:8px; }
  th, td { border:1px solid #eee; padding:8px; text-align:left; vertical-align:top; }
  th { background:#f7f7f7; }
  code, pre { background:#f9f9f9; padding:2px 6px; border-radius:6px; }
  .small { color:#666; font-size:13px; }

  /* FULL ROW colours by status */
  tr.status-added    { background:#e7f7ed; }  /* green */
  tr.status-deleted  { background:#fde7e7; }  /* red */
  tr.status-modified { background:#fff7e6; }  /* yellow */

  /* Blocks showing a full set */
  .block { border:1px dashed #ddd; border-radius:10px; padding:10px; margin:10px 0; }
  .kv { display:grid; grid-template-columns:200px 1fr; gap:8px; }
  .keycell { white-space:nowrap; }
  .valuecell { white-space:pre-wrap; }

  /* BRIGHT RED for exact changed values */
  .value-diff { color:#ff0000; font-weight:700; }
  .missing    { color:#ff0000; font-weight:700; }

  /* Modified details table */
  .subtable { width:100%; border-collapse:collapse; margin-top:6px; }
  .subtable th, .subtable td { border:1px solid #eee; padding:6px; }
  .subtable th { background:#fafafa; }
</style>
"""

def render_value(v: Any, is_changed: bool) -> str:
    if v == "(missing)":
        return "<span class='missing'>(missing)</span>"
    try:
        txt = json.dumps(v, ensure_ascii=False, indent=2)
    except Exception:
        txt = str(v)
    return f"<span class='value-diff'>{escape(txt)}</span>" if is_changed else escape(txt)

def render_with_path_highlights(obj: Any, changed_paths: Set[str], base_path: str = "") -> str:
    """Render dicts as key/value grids; lists/primitives as <pre>. Highlight ONLY exact changed paths."""
    if not isinstance(obj, dict):
        return "<pre class='valuecell'>" + render_value(obj, base_path in changed_paths) + "</pre>"

    rows = []
    for k in sorted(obj.keys(), key=lambda x: str(x)):
        path_k = f"{base_path}.{k}" if base_path else k
        v = obj[k]
        if isinstance(v, dict):
            sub_html = render_with_path_highlights(v, changed_paths, path_k)
            rows.append(
                "<div class='keycell'><code>" + escape(str(k)) + "</code></div>"
                "<div class='valuecell'>" + sub_html + "</div>"
            )
        elif isinstance(v, list):
            parts = []
            for i, item in enumerate(v):
                ipath = f"{path_k}[{i}]"
                if isinstance(item, dict):
                    parts.append(render_with_path_highlights(item, changed_paths, ipath))
                else:
                    parts.append("<pre class='valuecell'>" + render_value(item, ipath in changed_paths) + "</pre>")
            rows.append(
                "<div class='keycell'><code>" + escape(str(k)) + "</code></div>"
                "<div class='valuecell'>" + "".join(parts) + "</div>"
            )
        else:
            rows.append(
                "<div class='keycell'><code>" + escape(str(k)) + "</code></div>"
                "<div class='valuecell'><pre>" + render_value(v, path_k in changed_paths) + "</pre></div>"
            )
    return "<div class='kv'>" + "".join(rows) + "</div>"

def tr(row_class: str, cells: List[str]) -> str:
    tds = "".join("<td>" + c + "</td>" for c in cells)
    cls = " class='" + row_class + "'" if row_class else ""
    return "<tr" + cls + ">" + tds + "</tr>"

def build_modified_details_table(title: str, id_key_label: str, details: List[Tuple[str, str, str, str]]) -> str:
    if not details:
        return ""
    parts = ["<div class='section'><h3>" + escape(title) + " — Modified details</h3>"]
    parts.append("<table class='subtable'>")
    parts.append("<tr><th>" + escape(id_key_label) + "</th><th>Field path</th><th>Old</th><th>New</th></tr>")
    for ident, path, old_html, new_html in details:
        parts.append("<tr><td><code>" + escape(ident) + "</code></td><td><code>" + escape(path) + "</code></td><td>" + old_html + "</td><td>" + new_html + "</td></tr>")
    parts.append("</table></div>")
    return "".join(parts)
