
from __future__ import annotations
import json
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .discovery import load_json
from .diffing import dicts_by_key, paths_changed
from .html_render import CSS, render_with_path_highlights, tr, build_modified_details_table
from .groups import GroupSpec, GROUPS

def build_group_section(
    spec: GroupSpec,
    v1_items: Optional[List[Dict[str, Any]]],
    v2_items: Optional[List[Dict[str, Any]]],
) -> Tuple[str, List[Tuple[str, str, str, str]]]:
    html: List[str] = ["<div class='section'><h2>" + escape(spec.title) + "</h2>"]
    details_rows: List[Tuple[str, str, str, str]] = []

    if v1_items is None and v2_items is None:
        html.append("<p><b>Not found</b> in either file.</p></div>")
        return "".join(html), details_rows

    v1_map = dicts_by_key(v1_items or [], spec.id_key)
    v2_map = dicts_by_key(v2_items or [], spec.id_key)

    added   = sorted(set(v2_map) - set(v1_map))
    deleted = sorted(set(v1_map) - set(v2_map))
    common  = sorted(set(v1_map) & set(v2_map))

    has_any = bool(added or deleted or any(paths_changed(v1_map[k], v2_map[k]) for k in common))
    if not has_any:
        html.append("<p>No Added/Deleted/Modified items.</p></div>")
        return "".join(html), details_rows

    html.append("<table><tr><th>Group</th><th>Status</th><th>" + escape(spec.id_key_label) + "</th><th>V1</th><th>V2</th></tr>")

    for ident in added:
        v2 = v2_map[ident]
        html.append(tr(
            "status-added",
            [
                escape(spec.title),
                "Added",
                "<code>" + escape(ident) + "</code>",
                "",
                "<div class='block'>" + render_with_path_highlights(v2, set()) + "</div>",
            ],
        ))

    for ident in deleted:
        v1 = v1_map[ident]
        html.append(tr(
            "status-deleted",
            [
                escape(spec.title),
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
            continue
        changed_paths = {p for p, _, _ in diffs}
        v1_block = render_with_path_highlights(v1, changed_paths, "")
        v2_block = render_with_path_highlights(v2, changed_paths, "")

        html.append(tr(
            "status-modified",
            [
                escape(spec.title),
                "Modified",
                "<code>" + escape(ident) + "</code>",
                "<div class='block'>" + v1_block + "</div>",
                "<div class='block'>" + v2_block + "</div>",
            ],
        ))

        for path, old, new in diffs:
            if isinstance(old, str):
                old_text = old
            else:
                old_text = json.dumps(old, ensure_ascii=False)
            if isinstance(new, str):
                new_text = new
            else:
                new_text = json.dumps(new, ensure_ascii=False)
            old_html = "<span class='value-diff'>" + escape(old_text) + "</span>" if old != "(missing)" else "<span class='missing'>(missing)</span>"
            new_html = "<span class='value-diff'>" + escape(new_text) + "</span>" if new != "(missing)" else "<span class='missing'>(missing)</span>"
            details_rows.append((ident, path, old_html, new_html))

    html.append("</table></div>")
    return "".join(html), details_rows

def build_report(v1_path: Path, v2_path: Path) -> str:
    j1 = load_json(v1_path)
    j2 = load_json(v2_path)

    presence = []
    for spec in GROUPS:
        v1_items = spec.locator(j1)
        v2_items = spec.locator(j2)
        presence.append((spec.title, v1_items is not None, v2_items is not None))

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

    for spec in GROUPS:
        v1_items = spec.locator(j1)
        v2_items = spec.locator(j2)
        main_html, details_rows = build_group_section(spec, v1_items, v2_items)
        parts.append(main_html)
        parts.append(build_modified_details_table(spec.title, spec.id_key_label, details_rows))

    parts.append("</body></html>")
    return "".join(parts)

def build_report_from_objects(j1, j2, source1: str = "V1", source2: str = "V2") -> str:
    presence = []
    for spec in GROUPS:
        v1_items = spec.locator(j1)
        v2_items = spec.locator(j2)
        presence.append((spec.title, v1_items is not None, v2_items is not None))

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
    parts.append("</ul><div class='small'>Compared sources: <code>" + escape(source1) + "</code> and <code>" + escape(source2) + "</code></div></div>")

    for spec in GROUPS:
        v1_items = spec.locator(j1)
        v2_items = spec.locator(j2)
        main_html, details_rows = build_group_section(spec, v1_items, v2_items)
        parts.append(main_html)
        parts.append(build_modified_details_table(spec.title, spec.id_key_label, details_rows))

    parts.append("</body></html>")
    return "".join(parts)
