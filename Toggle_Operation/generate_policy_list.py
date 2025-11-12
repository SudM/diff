#!/usr/bin/env python3
"""
generate_package_toggle.py - FINAL ALL-IN-ONE
============================================
1. Read .deploymentpackage → Excel + raw JSON
2. Clean: OFF propagation, prune ON-only
3. Replace in prod_toggle.json
4. Generate BEAUTIFUL HTML DIFF (your exact layout)
"""

import json
import pandas as pd
import os
import sys
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from collections import OrderedDict, defaultdict
import difflib
import hashlib

# --------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------- #
LOG_FILE = "toggle_operation.log"
logging.basicConfig(
    filename=LOG_FILE,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# --------------------------------------------------------------------- #
# Utility
# --------------------------------------------------------------------- #
def safe_load_json(path: str) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.exception(f"Failed to load JSON: {e}")
        print("Failed to read file.")
        sys.exit(1)

def flatten_values(val) -> str:
    if isinstance(val, list):
        return ", ".join(str(v) for v in val)
    return str(val) if val is not None else ""

def find_property_case_insensitive(props: dict, key: str):
    for k, v in props.items():
        if k.strip().lower() == key.strip().lower():
            return v
    return ""

def map_service_from_path(path: str) -> str:
    p = path.lower()
    if "check permissions" in p:
        return "checkpermission"
    return ""

def is_enabled_from_status(status) -> bool:
    s = str(status).strip().lower()
    return s not in {"off", "false", "disabled", "no", "inactive"}

# --------------------------------------------------------------------- #
# Build Policy Tree
# --------------------------------------------------------------------- #
def build_policy_tree(data: List[Dict[str, Any]]) -> pd.DataFrame:
    metadata_nodes = [d for d in data if d.get("class") == "Metadata" and d.get("originType") in ("PolicySet", "Policy")]
    id_lookup = {d.get("id"): d for d in data if "id" in d}
    cd_nodes = {d["id"]: d for d in data if d.get("class") == "CombinedDecisionNode"}
    metadata_lookup = {m["originId"]: m for m in metadata_nodes}
    origin_to_cdnode = {}
    for cd in cd_nodes.values():
        if cd.get("originLink"):
            origin_to_cdnode.setdefault(cd["originLink"], []).append(cd)

    records = []

    def traverse(origin_id, path_stack, position):
        node = metadata_lookup.get(origin_id)
        if not node:
            return
        origin_type = node.get("originType", "")
        name = node.get("name", "")
        full_path = path_stack + [f"{origin_type}:{name}"]
        policy_fullpath = " / ".join(full_path)
        props = node.get("properties", {}) or {}

        record = {
            "Position": position,
            "ID": node.get("originId", ""),
            "Policy FullPath": policy_fullpath,
            "Epic": flatten_values(find_property_case_insensitive(props, "Epic")),
            "Feature": flatten_values(find_property_case_insensitive(props, "Feature")),
            "Defect": flatten_values(find_property_case_insensitive(props, "Defect")),
            "Status In Prod": flatten_values(find_property_case_insensitive(props, "Status in PROD")),
            "Toggle Type": flatten_values(find_property_case_insensitive(props, "Toggle Type")),
            "Toggle Name": flatten_values(find_property_case_insensitive(props, "Toggle Name")),
            "Service": map_service_from_path(policy_fullpath),
            "Action": flatten_values(find_property_case_insensitive(props, "action")),
            "Category": origin_type,
        }
        records.append(record)

        for cd in origin_to_cdnode.get(origin_id, []):
            for i, inp_id in enumerate(cd.get("inputNodes", []), 1):
                tmn = id_lookup.get(inp_id)
                if tmn and tmn.get("class") == "TargetMatchNode" and tmn.get("metadataId"):
                    traverse(tmn["metadataId"], full_path, f"{position}.{i}")

    package_meta = next((m for m in data if m.get("class") in ("Package", "DeploymentPackage")), None)
    if not package_meta or not package_meta.get("rootEntityId"):
        print("Invalid .deploymentpackage: no rootEntityId found.")
        sys.exit(1)

    traverse(package_meta["rootEntityId"], [], "1")
    return pd.DataFrame(records)

# --------------------------------------------------------------------- #
# Group + Build Tree
# --------------------------------------------------------------------- #
def get_action_groups(df: pd.DataFrame) -> List[pd.DataFrame]:
    groups = []
    action_rows = df[df["Action"].ne("")]
    for _, act_row in action_rows.iterrows():
        prefix = " / ".join(act_row["Policy FullPath"].split(" / ")[:5])
        group = df[df["Policy FullPath"].str.startswith(prefix)].copy()
        group.attrs["action_id"] = act_row["ID"]
        group.attrs["action_value"] = act_row["Action"]
        groups.append(group)
    return groups

def build_tree(group: pd.DataFrame) -> Optional[Dict[str, Any]]:
    act_id = group.attrs["action_id"]
    act_value = group.attrs["action_value"]

    toggle_rows = group[group["Toggle Type"].str.strip().ne("")].copy()
    if toggle_rows.empty:
        return None

    path_df = toggle_rows.set_index("Policy FullPath", drop=False)
    node_map: Dict[str, Dict] = {}
    for _, r in toggle_rows.iterrows():
        p = r["Policy FullPath"]
        t_type = r["Toggle Type"].strip()
        t_name = r["Toggle Name"].strip() or t_type
        node_map[p] = {"id": r["ID"], "name": t_name.upper(), "isEnabled": True, "versions": []}

    # Group versions
    version_rows = toggle_rows[toggle_rows["Toggle Type"].str.lower() == "versions"]
    version_by_parent = defaultdict(list)
    for _, vr in version_rows.iterrows():
        v_path = vr["Policy FullPath"]
        parent_paths = [p for p in node_map.keys() if v_path.startswith(p + " / ") and p != v_path]
        if parent_paths:
            parent_path = max(parent_paths, key=len)
            version_by_parent[parent_path].append(vr)

    for parent_path, vrs in version_by_parent.items():
        target = node_map.get(parent_path)
        if target:
            for vr in vrs:
                target["versions"].append({
                    "id": vr["ID"],
                    "name": vr["Toggle Name"].upper() or "V?",
                    "isEnabled": is_enabled_from_status(vr["Status In Prod"]),
                    "Category": vr["Category"]
                })

    # Build hierarchy
    root_obj: Dict[str, List] = {}
    sorted_paths = sorted(node_map.keys(), key=lambda x: x.count(" / "))
    for path in sorted_paths:
        node = node_map[path]
        segments = [s.strip() for s in path.split(" / ")]
        current = root_obj
        for i in range(len(segments) - 1):
            prefix = " / ".join(segments[:i + 1])
            if prefix not in path_df.index:
                continue
            row = path_df.loc[prefix]
            t_type = row["Toggle Type"].strip()
            if t_type.lower() == "versions":
                continue
            if t_type not in current:
                current[t_type] = []
            parent_list = current[t_type]
            parent_node = next((n for n in parent_list if n["name"] == (row["Toggle Name"].strip().upper() or t_type.upper())), None)
            if not parent_node:
                parent_node = {"id": row["ID"], "name": row["Toggle Name"].strip().upper() or t_type.upper(), "isEnabled": True, "versions": []}
                parent_list.append(parent_node)
            current = parent_node
        row = path_df.loc[path]
        t_type = row["Toggle Type"].strip()
        if t_type.lower() != "versions":
            if t_type not in current:
                current[t_type] = []
            current[t_type].append(node)

    if not root_obj:
        return None

    return {"id": act_id, "action": act_value, "isEnabled": True, **root_obj}

# --------------------------------------------------------------------- #
# CLEANUP
# --------------------------------------------------------------------- #
def propagate_off(node: Dict[str, Any]) -> bool:
    if "versions" in node:
        vers = node.get("versions", [])
        if not vers:
            node["isEnabled"] = False
            return True
        all_off = all(not v.get("isEnabled", True) for v in vers)
        if all_off:
            node["isEnabled"] = False
        return any(not v.get("isEnabled", True) for v in vers)

    has_off = False
    all_off = True
    for k, v in node.items():
        if k in {"id", "name", "isEnabled", "action", "Category", "type"} or not isinstance(v, list):
            continue
        for c in v:
            if isinstance(c, dict):
                child_off = propagate_off(c)
                has_off |= child_off
                if c.get("isEnabled", True):
                    all_off = False
    if all_off and has_off:
        node["isEnabled"] = False
    return has_off

def prune_versions_and_empty(node: Any) -> Any:
    if isinstance(node, dict):
        if "versions" in node:
            node["versions"] = [v for v in node["versions"] if not v.get("isEnabled", True)]
        to_delete = []
        for k, v in node.items():
            if isinstance(v, list):
                new_list = [prune_versions_and_empty(i) for i in v if i]
                if new_list:
                    node[k] = new_list
                else:
                    to_delete.append(k)
            elif isinstance(v, dict):
                node[k] = prune_versions_and_empty(v)
        for k in to_delete:
            del node[k]
        return node
    elif isinstance(node, list):
        return [prune_versions_and_empty(i) for i in node if i]
    return node

def remove_empty_on_toggles(node: Any) -> Any:
    if isinstance(node, dict):
        keys_to_delete = []
        for k, v in list(node.items()):
            if isinstance(v, list):
                cleaned = [remove_empty_on_toggles(i) for i in v if i]
                if cleaned:
                    node[k] = cleaned
                else:
                    keys_to_delete.append(k)
        for k in keys_to_delete:
            node.pop(k, None)
        has_versions = "versions" in node and bool(node["versions"])
        has_sub = any(isinstance(v, list) and v for k, v in node.items() if k not in {"id", "name", "isEnabled", "versions"})
        if node.get("isEnabled", True) and not has_versions and not has_sub:
            return None
        return node
    elif isinstance(node, list):
        return [x for x in [remove_empty_on_toggles(i) for i in node] if x]
    return node

def cleanup_action(action: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    act = json.loads(json.dumps(action))
    has_off = False
    for k, v in act.items():
        if k in {"id", "action", "isEnabled"}:
            continue
        if isinstance(v, list):
            for c in v:
                if isinstance(c, dict) and propagate_off(c):
                    has_off = True
    if not has_off:
        return None
    prune_versions_and_empty(act)
    return remove_empty_on_toggles(act)

# --------------------------------------------------------------------- #
# HTML DIFF ENGINE (Your Exact Layout)
# --------------------------------------------------------------------- #
def generate_html_diff(prod_path: Path, final_path: Path, output_path: Path):
    with open(prod_path) as f:
        old_data = json.load(f, object_pairs_hook=OrderedDict)
    with open(final_path) as f:
        new_data = json.load(f, object_pairs_hook=OrderedDict)

    old_actions = {a["action"]: a for a in old_data.get("toggles", {}).get("checkpermission", [])}
    new_actions = {a["action"]: a for a in new_data.get("toggles", {}).get("checkpermission", [])}

    added = {k: v for k, v in new_actions.items() if k not in old_actions}
    deleted = {k: v for k, v in old_actions.items() if k not in new_actions}
    modified = {}
    for k in set(old_actions) & set(new_actions):
        if json.dumps(old_actions[k], sort_keys=True) != json.dumps(new_actions[k], sort_keys=True):
            modified[k] = (old_actions[k], new_actions[k])

    # === Build HTML ===
    html = """<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'><title>Toggle JSON Diff Report</title>
<style>
  body { font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif; margin:24px; line-height:1.5; }
  h1,h2,h3 { margin-top:0; }
  .section { border:1px solid #ddd; border-radius:12px; padding:16px; margin-bottom:16px; }
  table { width:100%; border-collapse:collapse; margin-top:8px; }
  th, td { border:1px solid #eee; padding:8px; text-align:left; vertical-align:top; }
  th { background:#f7f7f7; }
  code, pre { background:#f9f9f9; padding:2px 6px; border-radius:6px; }
  .small { color:#666; font-size:13px; }
  tr.status-added    { background:#e7f7ed; }
  tr.status-deleted  { background:#fde7e7; }
  tr.status-modified { background:#fff7e6; }
  pre.json-raw { background:#f9f9ff; border:1px dashed #ddd; border-radius:10px; padding:10px; overflow:auto; font-size:13px; }
  .value-diff { color:#d00; font-weight:700; }
  .missing { color:#d00; font-weight:700; }
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
</head><body>"""
    html += f"<h1>Toggle JSON Diff Report</h1>"
    html += f"<div class='section'><h2>Summary</h2><ul>"
    for service in ["checkpermission"]:
        old_has = bool(old_data.get("toggles", {}).get(service))
        new_has = bool(new_data.get("toggles", {}).get(service))
        html += f"<li><b>{service}</b>: {'present' if old_has else 'missing'} in {prod_path.name} / {'present' if new_has else 'missing'} in {final_path.name}</li>"
    html += f"</ul><div class='small'>Compared files: <code>{prod_path.name}</code> and <code>{final_path.name}</code></div></div>"

    # === checkPermissions ===
    html += "<div class='section'><h2>checkPermissions</h2>"
    if not (added or deleted or modified):
        html += "<p>No Added/Deleted/Modified items.</p>"
    else:
        html += "<table><tr><th>Group</th><th>Status</th><th>Action</th><th>{}</th><th>{}</th></tr>".format(prod_path.name, final_path.name)
        for action, new_act in added.items():
            html += f"<tr class='status-added'><td>checkPermissions</td><td>Added</td><td><code>{action}</code></td><td></td><td><pre class='json-raw'>{json.dumps(new_act, indent=2)}</pre></td></tr>"
        for action, old_act in deleted.items():
            html += f"<tr class='status-deleted'><td>checkPermissions</td><td>Deleted</td><td><code>{action}</code></td><td><pre class='json-raw'>{json.dumps(old_act, indent=2)}</pre></td><td></td></tr>"
        for action, (old_act, new_act) in modified.items():
            old_json = json.dumps(old_act, indent=2)
            new_json = json.dumps(new_act, indent=2)
            # Highlight diffs
            old_lines = old_json.splitlines()
            new_lines = new_json.splitlines()
            diff = difflib.unified_diff(old_lines, new_lines, n=0)
            highlighted_old = []
            highlighted_new = []
            for line in diff:
                if line.startswith('-') and not line.startswith('---'):
                    highlighted_old.append(f"<span class='value-diff'>{line[1:]}</span>")
                elif line.startswith('+') and not line.startswith('+++'):
                    highlighted_new.append(f"<span class='value-diff'>{line[1:]}</span>")
                else:
                    highlighted_old.append(line[1:] if line.startswith('-') else line)
                    highlighted_new.append(line[1:] if line.startswith('+') else line)
            old_html = "<br>".join(highlighted_old)
            new_html = "<br>".join(highlighted_new)
            html += f"<tr class='status-modified'><td>checkPermissions</td><td>Modified</td><td><code>{action}</code></td><td><pre class='json-raw'>{old_html}</pre></td><td><pre class='json-raw'>{new_html}</pre></td></tr>"

        html += "</table>"

        # Subtables
        if modified:
            html += "<h3>checkPermissions — Modified details</h3>"
            for action, (old_act, new_act) in modified.items():
                changes = find_field_changes(old_act, new_act)
                if changes:
                    html += f"<div class='ident-hdr'><span class='ident-chip'>{action}</span><span class='ident-meta'>Action • {len(changes)} change(s)</span></div>"
                    html += "<table class='subtable'><thead><tr><th>Field path</th><th class='wrap'>{}</th><th class='arrow-cell'>→</th><th class='wrap'>{}</th></tr></thead><tbody>".format(prod_path.name, final_path.name)
                    for path, (old_val, new_val) in changes.items():
                        old_str = json.dumps(old_val, ensure_ascii=False) if old_val is not None else "<span class='missing'>(missing)</span>"
                        new_str = json.dumps(new_val, ensure_ascii=False) if new_val is not None else "<span class='missing'>(missing)</span>"
                        html += f"<tr><td><code>{path}</code></td><td class='wrap'>{old_str}</td><td class='arrow-cell'>→</td><td class='wrap'>{new_str}</td></tr>"
                    html += "</tbody></table>"

    html += "</div></body></html>"
    output_path.write_text(html, encoding="utf-8")
    print(f"Diff report → {output_path}")

def find_field_changes(old: Dict, new: Dict, prefix: str = "") -> Dict[str, Tuple[Any, Any]]:
    changes = {}
    all_keys = set(old.keys()) | set(new.keys())
    for key in all_keys:
        old_val = old.get(key)
        new_val = new.get(key)
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(old_val, dict) and isinstance(new_val, dict):
            changes.update(find_field_changes(old_val, new_val, path))
        elif isinstance(old_val, list) and isinstance(new_val, list):
            for i in range(max(len(old_val), len(new_val))):
                o = old_val[i] if i < len(old_val) else None
                n = new_val[i] if i < len(new_val) else None
                sub_path = f"{path}[{i}]"
                if isinstance(o, dict) and isinstance(n, dict):
                    changes.update(find_field_changes(o, n, sub_path))
                elif o != n:
                    changes[sub_path] = (o, n)
        elif old_val != new_val:
            changes[path] = (old_val, new_val)
    return changes

# --------------------------------------------------------------------- #
# Replace + Diff
# --------------------------------------------------------------------- #
def replace_and_diff(prod_path: Path, cleaned_data: Dict, out_json: Path, diff_html: Path):
    with open(prod_path, "r", encoding="utf-8") as f:
        prod_data = json.load(f, object_pairs_hook=OrderedDict)

    cleaned_check = cleaned_data.get("toggles", {}).get("checkpermission", [])
    prod_toggles = prod_data.get("toggles", OrderedDict())
    prod_toggles["checkpermission"] = cleaned_check

    final_out = OrderedDict([
        ("schemaVersion", prod_data.get("schemaVersion")),
        ("strategy", prod_data.get("strategy")),
        ("toggles", prod_toggles)
    ])

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(final_out, indent=2), encoding="utf-8")

    generate_html_diff(prod_path, out_json, diff_html)

# --------------------------------------------------------------------- #
# Export
# --------------------------------------------------------------------- #
def export_excel(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Policy_Tree", index=False)
    print(f"Excel → {path}")

# --------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------- #
def main():
    import argparse
    parser = argparse.ArgumentParser(description="All-in-one toggle generator + diff")
    parser.add_argument("-d", "--deployment", required=True)
    parser.add_argument("-p", "--prod", default="./input/prod_toggle.json")
    parser.add_argument("-x", "--excel", default="./output/Policy_Export.xlsx")
    parser.add_argument("-f", "--final", default="./output/final_package_toggle.json")
    parser.add_argument("--diff", default="./output/Arbitrary_Services_Diff.html")
    args = parser.parse_args()

    print("Starting pipeline...")
    data = safe_load_json(args.deployment)
    df = build_policy_tree(data)
    export_excel(df, Path(args.excel))

    raw_actions = [build_tree(g) for g in get_action_groups(df) if build_tree(g)]
    cleaned_actions = [cleanup_action(a) for a in raw_actions if cleanup_action(a)]

    cleaned_data = {
        "schemaVersion": "1.0.0",
        "strategy": "blacklist",
        "toggles": {"checkpermission": cleaned_actions}
    }

    replace_and_diff(Path(args.prod), cleaned_data, Path(args.final), Path(args.diff))
    print("ALL DONE!")

if __name__ == "__main__":
    main()