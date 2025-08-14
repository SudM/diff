#!/usr/bin/env python3
"""
json_diff_simple.py
Compare two JSON files and generate an HTML diff focused on:
- arbitrary.services      (keyed by name)
- checkPermissions        (keyed by action)
- getPermissions          (keyed by resourceType)

Rows are coloured by status:
  Added   -> green
  Deleted -> red
  Modified-> yellow
For Modified rows, the full set is shown for V1 and V2, with the exact changed values in BRIGHT RED.
Unchanged items are omitted.

Usage:
  python json_diff_simple.py "Toggle V1.json" "Toggle V2.json" -o Diff_Report.html
"""

from __future__ import annotations
import argparse
import json
from html import escape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Set

# -----------------------
# HTML / CSS rendering
# -----------------------

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

# -----------------------
# JSON helpers
# -----------------------

def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8", errors="replace"))

def find_first_key(root: Any, key: str) -> Optional[Any]:
    """DFS: return the first value for 'key' anywhere in the JSON."""
    if isinstance(root, dict):
        if key in root:
            return root[key]
        for v in root.values():
            found = find_first_key(v, key)
            if found is not None:
                return found
    elif isinstance(root, list):
        for item in root:
            found = find_first_key(item, key)
            if found is not None:
                return found
    return None

def locate_arbitrary_services(root: Any) -> Optional[List[Dict[str, Any]]]:
    """Prefer toggles.arbitrary.services; else any 'arbitrary' with a 'services' list."""
    try:
        s = root["toggles"]["arbitrary"]["services"]
        if isinstance(s, list):
            return s  # type: ignore[return-value]
    except Exception:
        pass
    arb = find_first_key(root, "arbitrary")
    if isinstance(arb, dict):
        s = arb.get("services")
        if isinstance(s, list):
            return s  # type: ignore[return-value]
    return None

def locate_check_permissions(root: Any) -> Optional[List[Dict[str, Any]]]:
    """Prefer toggles.checkPermissions; else any 'checkPermissions' list."""
    try:
        cp = root["toggles"]["checkPermissions"]
        if isinstance(cp, list):
            return cp  # type: ignore[return-value]
    except Exception:
        pass
    cp = find_first_key(root, "checkPermissions")
    return cp if isinstance(cp, list) else None

def locate_get_permissions(root: Any) -> Optional[List[Dict[str, Any]]]:
    """Prefer toggles.getPermissions; else any 'getPermissions' list."""
    try:
        gp = root["toggles"]["getPermissions"]
        if isinstance(gp, list):
            return gp  # type: ignore[return-value]
    except Exception:
        pass
    gp = find_first_key(root, "getPermissions")
    return gp if isinstance(gp, list) else None

# -----------------------
# Diff logic (no deps)
# -----------------------

def dicts_by_key(items: Iterable[dict], key: str) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for it in items or []:
        if isinstance(it, dict) and key in it:
            out[str(it[key])] = it
    return out

def paths_changed(a: Any, b: Any, prefix: str = "") -> List[Tuple[str, Any, Any]]:
    """
    Recursively compute differences between a and b.
    Returns [(path, old, new)]. Uses '(missing)' sentinel for adds/deletes.
    Paths look like: key.sub[2].name
    """
    diffs: List[Tuple[str, Any, Any]] = []

    if type(a) is not type(b):
        diffs.append((prefix or "(root)", a, b))
        return diffs

    if isinstance(a, dict):
        keys = sorted(set(a.keys()) | set(b.keys()), key=lambda x: str(x))
        for k in keys:
            p = f"{prefix}.{k}" if prefix else k
            if k not in a:
                diffs.append((p, "(missing)", b[k]))
            elif k not in b:
                diffs.append((p, a[k], "(missing)"))
            else:
                diffs.extend(paths_changed(a[k], b[k], p))
        return diffs

    if isinstance(a, list):
        m = min(len(a), len(b))
        for i in range(m):
            p = f"{prefix}[{i}]" if prefix else f"[{i}]"
            diffs.extend(paths_changed(a[i], b[i], p))
        for i in range(m, len(a)):
            diffs.append((f"{prefix}[{i}]", a[i], "(missing)"))
        for i in range(m, len(b)):
            diffs.append((f"{prefix}[{i}]", "(missing)", b[i]))
        return diffs

    if a != b:
        diffs.append((prefix or "(value)", a, b))
    return diffs

# -----------------------
# Report building
# -----------------------

class GroupSpec(Tuple[str, str, str, Any]):
    """
    title, id_key_label, id_key, locator
    Just a light tuple-like container for simplicity in this single-file script.
    """

def build_group_section(
    title: str,
    id_key_label: str,
    id_key: str,
    v1_items: Optional[List[Dict[str, Any]]],
    v2_items: Optional[List[Dict[str, Any]]],
) -> Tuple[str, List[Tuple[str, str, str, str]]]:
    html_parts: List[str] = ["<div class='section'><h2>" + escape(title) + "</h2>"]
    details_rows: List[Tuple[str, str, str, str]] = []

    if v1_items is None and v2_items is None:
        html_parts.append("<p><b>Not found</b> in either file.</p></div>")
        return "".join(html_parts), details_rows

    v1_map = dicts_by_key(v1_items or [], id_key)
    v2_map = dicts_by_key(v2_items or [], id_key)

    added   = sorted(set(v2_map) - set(v1_map))
    deleted = sorted(set(v1_map) - set(v2_map))
    common  = sorted(set(v1_map) & set(v2_map))

    # Skip rendering if truly nothing changed for this group
    has_any = bool(added or deleted or any(paths_changed(v1_map[k], v2_map[k]) for k in common))
    if not has_any:
        html_parts.append("<p>No Added/Deleted/Modified items.</p></div>")
        return "".join(html_parts), details_rows

    html_parts.append("<table><tr><th>Group</th><th>Status</th><th>" + escape(id_key_label) + "</th><th>V1</th><th>V2</th></tr>")

    for ident in added:
        v2 = v2_map[ident]
        html_parts.append(tr(
            "status-added",
            [
                escape(title),
                "Added",
                "<code>" + escape(ident) + "</code>",
                "",
                "<div class='block'>" + render_with_path_highlights(v2, set()) + "</div>",
            ],
        ))

    for ident in deleted:
        v1 = v1_map[ident]
        html_parts.append(tr(
            "status-deleted",
            [
                escape(title),
                "Deleted",
                "<code>" + escape(ident) + "</code>",
                "<div class='block'>" + render_with_path_highlights(v1, set()) + "</div>",
                "",
            ],
        ))

    for ident in common:
        v1 = v1_map[ident]
        v2 = v2_map[ident]
        diffs = paths_changed(v1, v2, prefix="")
        if not diffs:
            continue  # unchanged -> omit
        changed_paths = {p for p, _, _ in diffs}
        v1_block = render_with_path_highlights(v1, changed_paths, "")
        v2_block = render_with_path_highlights(v2, changed_paths, "")

        html_parts.append(tr(
            "status-modified",
            [
                escape(title),
                "Modified",
                "<code>" + escape(ident) + "</code>",
                "<div class='block'>" + v1_block + "</div>",
                "<div class='block'>" + v2_block + "</div>",
            ],
        ))

        for path, old, new in diffs:
            old_text = old if isinstance(old, str) else json.dumps(old, ensure_ascii=False)
            new_text = new if isinstance(new, str) else json.dumps(new, ensure_ascii=False)
            old_html = "<span class='value-diff'>" + escape(old_text) + "</span>" if old != "(missing)" else "<span class='missing'>(missing)</span>"
            new_html = "<span class='value-diff'>" + escape(new_text) + "</span>" if new != "(missing)" else "<span class='missing'>(missing)</span>"
            details_rows.append((ident, path, old_html, new_html))

    html_parts.append("</table></div>")
    return "".join(html_parts), details_rows

def build_report(v1_path: Path, v2_path: Path) -> str:
    j1 = load_json(v1_path)
    j2 = load_json(v2_path)

    groups = [
        ("arbitrary → services", "Service Name", "name", locate_arbitrary_services),
        ("checkPermissions", "Action", "action", locate_check_permissions),
        ("getPermissions", "Resource Type", "resourceType", locate_get_permissions),
    ]

    # Presence summary
    presence = []
    for title, _, _, locator in groups:
        v1_items = locator(j1)
        v2_items = locator(j2)
        presence.append((title, v1_items is not None, v2_items is not None))

    parts: List[str] = [
        "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'>",
        "<title>Services, Check & Get Permissions Diff</title>",
        CSS,
        "</head><body>",
        "<h1>Services, Check & Get Permissions Diff</h1>",
        "<div class='section'><h2>Summary</h2><ul>",
    ]

    for title, p1, p2 in presence:
        parts.append("<li><b>" + escape(title) + "</b>: " + ("present in V1" if p1 else "missing in V1") + " / " + ("present in V2" if p2 else "missing in V2") + "</li>")
    parts.append("</ul><div class='small'>Compared files: <code>" + escape(str(v1_path)) + "</code> and <code>" + escape(str(v2_path)) + "</code></div></div>")

    # Each group
    for title, id_label, id_key, locator in groups:
        v1_items = locator(j1)
        v2_items = locator(j2)
        main_html, details_rows = build_group_section(title, id_label, id_key, v1_items, v2_items)
        parts.append(main_html)
        parts.append(build_modified_details_table(title, id_label, details_rows))

    parts.append("</body></html>")
    return "".join(parts)

# -----------------------
# CLI
# -----------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Compare services (by name), checkPermissions (by action), getPermissions (by resourceType). "
                    "Rows coloured by status; modified values highlighted in bright red. "
                    "Unchanged items are omitted."
    )
    p.add_argument("v1", type=Path, help="Path to V1 JSON")
    p.add_argument("v2", type=Path, help="Path to V2 JSON")
    p.add_argument("-o", "--output", type=Path, default=Path("Diff_Report.html"), help="Output HTML file (default: Diff_Report.html)")
    return p.parse_args()

def main() -> None:
    args = parse_args()
    if not args.v1.exists():
        raise SystemExit(f"Not found: {args.v1}")
    if not args.v2.exists():
        raise SystemExit(f"Not found: {args.v2}")
    html = build_report(args.v1, args.v2)
    args.output.write_text(html, encoding="utf-8")
    print("Report saved to:", args.output.resolve())

if __name__ == "__main__":
    main()
