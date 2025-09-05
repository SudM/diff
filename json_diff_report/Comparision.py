#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json
from collections import defaultdict
from html import escape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

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

  tr.status-added    { background:#e7f7ed; }  /* green */
  tr.status-deleted  { background:#fde7e7; }  /* red */
  tr.status-modified { background:#fff7e6; }  /* yellow */

  pre.json-raw { background:#f9f9ff; border:1px dashed #ddd; border-radius:10px; padding:10px; overflow:auto; }

  .value-diff { color:#ff0000; font-weight:700; }
  .missing    { color:#ff0000; font-weight:700; }

  .ident-hdr { margin-top:12px; }
  .ident-chip { display:inline-block; background:#eef2ff; border:1px solid #dbe3ff; padding:4px 10px; border-radius:999px; font-weight:600; }
  .ident-meta { color:#666; margin-left:8px; font-size:12px; }
  table.subtable { width:100%; border-collapse:collapse; margin-top:6px; }
  .subtable th, .subtable td { border:1px solid #eee; padding:8px; vertical-align:top; }
  .subtable th { background:#fafafa; position:sticky; top:0; z-index:1; }
  .subtable tr:nth-child(even) { background:#fbfbfb; }
  .arrow-cell { width:32px; text-align:center; color:#999; font-weight:600; }
  .wrap { white-space:pre-wrap; word-break:break-word; }
</style>
"""

def tr(row_class: str, cells: List[str]) -> str:
    tds = "".join("<td>" + c + "</td>" for c in cells)
    cls = " class='" + row_class + "'" if row_class else ""
    return "<tr" + cls + ">" + tds + "</tr>"

# ---------- RAW JSON WITH HIGHLIGHTS (fixed for removed/added composites) ----------

def _json_value_html(v: Any, is_changed: bool) -> str:
    if v == "(missing)":
        return "<span class='missing'>(missing)</span>"
    token = json.dumps(v, ensure_ascii=False)
    token = escape(token)
    return f"<span class='value-diff'>{token}</span>" if is_changed else token

def _json_pretty_html(
    obj: Any,
    changed_paths: Set[str],
    whole_highlight: Set[str],
    base_path: str,
    indent: str,
    level: int,
) -> str:
    """Pretty-print JSON with:
       - leaf value highlights when path ∈ changed_paths
       - whole block highlights (dict/list) when base_path ∈ whole_highlight
    """
    sp = indent * level
    sp2 = indent * (level + 1)

    # Dict
    if isinstance(obj, dict):
        items = list(obj.items())
        if not items:
            rendered = "{}"
        else:
            lines = ["{"]
            for i, (k, v) in enumerate(items):
                path_k = f"{base_path}.{k}" if base_path else k
                key_html = '"' + escape(str(k)) + '"'
                if isinstance(v, (dict, list)):
                    val_html = _json_pretty_html(v, changed_paths, whole_highlight, path_k, indent, level + 1)
                else:
                    val_html = _json_value_html(v, path_k in changed_paths)
                comma = "," if i < len(items) - 1 else ""
                lines.append(f"{sp2}{key_html}: {val_html}{comma}")
            lines.append(f"{sp}}}")
            rendered = "\n".join(lines)
        # If this whole composite path changed (removed/added/type-change), wrap the block in red
        return f"<span class='value-diff'>{rendered}</span>" if (base_path in whole_highlight and base_path != "") else rendered

    # List
    if isinstance(obj, list):
        if not obj:
            rendered = "[]"
        else:
            lines = ["["]
            for i, item in enumerate(obj):
                ipath = f"{base_path}[{i}]"
                if isinstance(item, (dict, list)):
                    item_html = _json_pretty_html(item, changed_paths, whole_highlight, ipath, indent, level + 1)
                else:
                    item_html = _json_value_html(item, ipath in changed_paths)
                comma = "," if i < len(obj) - 1 else ""
                lines.append(f"{sp2}{item_html}{comma}")
            lines.append(f"{sp}]")
            rendered = "\n".join(lines)
        return f"<span class='value-diff'>{rendered}</span>" if (base_path in whole_highlight and base_path != "") else rendered

    # Primitive
    return _json_value_html(obj, base_path in changed_paths)

def render_json_raw_with_highlights(
    obj: Any,
    changed_paths: Set[str],
    whole_highlight: Set[str] | None = None,
    base_path: str = "",
    indent: str = "  ",
) -> str:
    whole = whole_highlight or set()
    html = _json_pretty_html(obj, changed_paths, whole, base_path, indent, 0)
    return "<pre class='json-raw'>" + html + "</pre>"

# -----------------------
# JSON helpers
# -----------------------

def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8", errors="replace"))

def find_first_key(root: Any, key: str) -> Optional[Any]:
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
    try:
        cp = root["toggles"]["checkPermissions"]
        if isinstance(cp, list):
            return cp  # type: ignore[return-value]
    except Exception:
        pass
    cp = find_first_key(root, "checkPermissions")
    return cp if isinstance(cp, list) else None

def locate_get_permissions(root: Any) -> Optional[List[Dict[str, Any]]]:
    try:
        gp = root["toggles"]["getPermissions"]
        if isinstance(gp, list):
            return gp  # type: ignore[return-value]
    except Exception:
        pass
    gp = find_first_key(root, "getPermissions")
    return gp if isinstance(gp, list) else None

# -----------------------
# Diff logic (stdlib only)
# -----------------------

def dicts_by_key(items: Iterable[dict], key: str) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for it in items or []:
        if isinstance(it, dict) and key in it:
            out[str(it[key])] = it
    return out

def paths_changed(a: Any, b: Any, prefix: str = "") -> List[Tuple[str, Any, Any]]:
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

def build_group_section(
    title: str,
    id_key_label: str,
    id_key: str,
    v1_items: Optional[List[Dict[str, Any]]],
    v2_items: Optional[List[Dict[str, Any]]],
    col_label_1: str,
    col_label_2: str,
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

    has_any = bool(added or deleted or any(paths_changed(v1_map[k], v2_map[k]) for k in common))
    if not has_any:
        html_parts.append("<p>No Added/Deleted/Modified items.</p></div>")
        return "".join(html_parts), details_rows

    html_parts.append("<table><tr><th>Group</th><th>Status</th><th>" + escape(id_key_label) + "</th>"
                      "<th>" + escape(col_label_1) + "</th><th>" + escape(col_label_2) + "</th></tr>")

    # Added rows
    for ident in added:
        v2 = v2_map[ident]
        v2_raw = render_json_raw_with_highlights(v2, changed_paths=set(), whole_highlight={"(root)"})  # whole block red on the "added" side
        html_parts.append(tr("status-added", [escape(title), "Added", "<code>" + escape(ident) + "</code>", "", v2_raw]))

    # Deleted rows
    for ident in deleted:
        v1 = v1_map[ident]
        v1_raw = render_json_raw_with_highlights(v1, changed_paths=set(), whole_highlight={"(root)"})  # whole block red on the "deleted" side
        html_parts.append(tr("status-deleted", [escape(title), "Deleted", "<code>" + escape(ident) + "</code>", v1_raw, ""]))

    # Modified rows
    for ident in common:
        v1 = v1_map[ident]
        v2 = v2_map[ident]
        diffs = paths_changed(v1, v2, prefix="")
        if not diffs:
            continue

        changed_paths = {p for p, _, _ in diffs}

        # For composite removals/additions (or type changes), wrap the WHOLE block at that path in red
        missing_in_v2 = {p for p, _, n in diffs if n == "(missing)"}
        missing_in_v1 = {p for p, o, _ in diffs if o == "(missing)"}
        type_change_composite = {p for p, o, n in diffs if type(o) is not type(n) and (isinstance(o, (dict, list)) or isinstance(n, (dict, list)))}

        whole_v1 = missing_in_v2 | type_change_composite
        whole_v2 = missing_in_v1 | type_change_composite

        v1_raw = render_json_raw_with_highlights(v1, changed_paths, whole_highlight=whole_v1)
        v2_raw = render_json_raw_with_highlights(v2, changed_paths, whole_highlight=whole_v2)

        html_parts.append(tr("status-modified", [escape(title), "Modified", "<code>" + escape(ident) + "</code>", v1_raw, v2_raw]))

        # details rows
        for path, old, new in diffs:
            old_text = old if isinstance(old, str) else json.dumps(old, ensure_ascii=False)
            new_text = new if isinstance(new, str) else json.dumps(new, ensure_ascii=False)
            old_html = "<span class='value-diff'>" + escape(old_text) + "</span>" if old != "(missing)" else "<span class='missing'>(missing)</span>"
            new_html = "<span class='value-diff'>" + escape(new_text) + "</span>" if new != "(missing)" else "<span class='missing'>(missing)</span>"
            details_rows.append((ident, path, old_html, new_html))

    html_parts.append("</table></div>")
    return "".join(html_parts), details_rows

def build_modified_details_table_grouped(
    title: str,
    id_key_label: str,
    details: List[Tuple[str, str, str, str]],
    col_label_1: str,
    col_label_2: str,
) -> str:
    if not details:
        return ""
    grouped: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
    for ident, path, old_html, new_html in details:
        grouped[ident].append((path, old_html, new_html))

    parts = [f"<div class='section'><h3>{escape(title)} — Modified details</h3>"]
    for ident in sorted(grouped.keys(), key=lambda s: s.lower()):
        rows = grouped[ident]
        parts.append("<div class='ident-hdr'><span class='ident-chip'>" + escape(ident) + "</span>"
                     "<span class='ident-meta'>" + escape(id_key_label) + f" • {len(rows)} change(s)</span></div>")
        parts.append("<table class='subtable'>")
        parts.append("<thead><tr><th>Field path</th>"
                     f"<th class='wrap'>{escape(col_label_1)}</th>"
                     "<th class='arrow-cell'>→</th>"
                     f"<th class='wrap'>{escape(col_label_2)}</th></tr></thead><tbody>")
        for path, old_html, new_html in rows:
            parts.append("<tr><td><code>" + escape(path) + "</code></td>"
                         "<td class='wrap'>" + old_html + "</td>"
                         "<td class='arrow-cell'>→</td>"
                         "<td class='wrap'>" + new_html + "</td></tr>")
        parts.append("</tbody></table>")
    parts.append("</div>")
    return "".join(parts)

def build_report(v1_path: Path, v2_path: Path) -> str:
    j1 = load_json(v1_path)
    j2 = load_json(v2_path)

    label1 = v1_path.name
    label2 = v2_path.name

    groups = [
        ("arbitrary → services", "Service Name", "name", locate_arbitrary_services),
        ("checkPermissions", "Action", "action", locate_check_permissions),
        ("getPermissions", "Resource Type", "resourceType", locate_get_permissions),
    ]

    presence = []
    for title, _, _, locator in groups:
        v1_items = locator(j1)
        v2_items = locator(j2)
        presence.append((title, v1_items is not None, v2_items is not None))

    parts: List[str] = [
        "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'>",
        "<title>Toggle JSON Diff Report</title>",
        CSS,
        "</head><body>",
        "<h1>Toggle JSON Diff Report</h1>",
        "<div class='section'><h2>Summary</h2><ul>",
    ]
    for title, p1, p2 in presence:
        parts.append("<li><b>" + escape(title) + "</b>: " +
                     ("present in " + escape(label1) if p1 else "missing in " + escape(label1)) +
                     " / " +
                     ("present in " + escape(label2) if p2 else "missing in " + escape(label2)) +
                     "</li>")
    parts.append("</ul><div class='small'>Compared files: <code>" + escape(str(v1_path)) + "</code> and <code>" + escape(str(v2_path)) + "</code></div></div>")

    for title, id_label, id_key, locator in groups:
        v1_items = locator(j1)
        v2_items = locator(j2)
        main_html, details_rows = build_group_section(title, id_label, id_key, v1_items, v2_items, label1, label2)
        parts.append(main_html)
        parts.append(build_modified_details_table_grouped(title, id_label, details_rows, label1, label2))

    parts.append("</body></html>")
    return "".join(parts)

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Compare arbitrary.services (by name), checkPermissions (by action), getPermissions (by resourceType). "
                    "Added=green, Deleted=red, Modified=yellow. Exact differences highlighted in bright red."
    )
    p.add_argument("v1", type=Path, help="Path to first JSON file")
    p.add_argument("v2", type=Path, help="Path to second JSON file")
    p.add_argument("-o", "--output", type=Path, default=Path("Diff_Report.html"), help="Output HTML file")
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
