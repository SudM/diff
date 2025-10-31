import json
import pandas as pd
import argparse
import os
import numpy as np
from collections import OrderedDict

# -----------------------------
# Helpers
# -----------------------------
def safe_load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def flatten_values(val):
    if isinstance(val, list):
        return ",".join(str(v) for v in val)
    return val

# -----------------------------
# Condition ID collector
# -----------------------------
def collect_condition_def_ids_from_tree(id_map, root_node_id):
    stack, seen, result = [root_node_id], set(), set()
    while stack:
        nid = stack.pop()
        if not nid or nid in seen:
            continue
        seen.add(nid)
        node = id_map.get(nid, {})
        if not node:
            continue
        if node.get("class") == "ConditionReferenceNode":
            rid = node.get("definitionId") or node.get("ref") or node.get("conditionId")
            if rid:
                result.add(rid)
        for key in ("inputNode", "guardNode", "condition", "lhsInputNode", "rhsInputNode"):
            val = node.get(key)
            if isinstance(val, str):
                stack.append(val)
        if isinstance(node.get("inputNodes"), list):
            stack.extend(node.get("inputNodes"))
    return result

# -----------------------------
# pick_single_condition_path
# -----------------------------
def pick_single_condition_path(cond_names):
    targeting = [c for c in cond_names if c.startswith("POLICY.TARGETING")]
    toggles = [c for c in cond_names if c.startswith("POLICY.TOGGLES")]

    def longest(cands):
        return max(cands, key=lambda s: len(s.split("."))) if cands else ""

    if targeting:
        action_first = [c for c in targeting if ".ACTION." in c]
        return longest(action_first) if action_first else longest(targeting)
    if toggles:
        return longest(toggles)
    return ""

# -----------------------------
# Policy Tree
# -----------------------------
def build_policy_tree(data):
    metadata_nodes = [
        d for d in data
        if d.get("class") == "Metadata" and d.get("originType") in ("PolicySet", "Policy")
    ]
    condition_defs = [d for d in data if d.get("class") == "ConditionDefinition"]
    id_lookup = {d["id"]: d for d in data if "id" in d}
    cd_nodes = {d["id"]: d for d in data if d.get("class") == "CombinedDecisionNode"}

    origin_to_cdnode = {}
    for cd in cd_nodes.values():
        if cd.get("originLink"):
            origin_to_cdnode.setdefault(cd["originLink"], []).append(cd)

    metadata_lookup = {m["originId"]: m for m in metadata_nodes}
    records = []

    def traverse(origin_id, path_names, position):
        node = metadata_lookup.get(origin_id)
        if not node:
            return

        fullpath_parts = path_names + [f"{node['originType']}:{node['name']}"]

        cond_names_parent = []
        for cd in origin_to_cdnode.get(origin_id, []):
            if cd.get("guardNode"):
                for cid in collect_condition_def_ids_from_tree(id_lookup, cd["guardNode"]):
                    cond = next((c for c in condition_defs if c["id"] == cid), None)
                    if cond and cond.get("name"):
                        cond_names_parent.append(cond["name"])

        cond_val_parent = pick_single_condition_path(list(set(cond_names_parent)))
        cond_id_parent = next(
            (c["id"] for c in condition_defs if c.get("name") == cond_val_parent),
            ""
        )

        epic = feature = defect = status = version = ""
        toggle_type = toggle_name = ""
        if node.get("originType") == "PolicySet" and str(node.get("name", "")).lower().startswith("entitlement check"):
            props = node.get("properties", {}) or {}
            epic = flatten_values(props.get("Epic", ""))
            feature = flatten_values(props.get("Feature", ""))
            defect = flatten_values(props.get("Defect", ""))
            status = flatten_values(props.get("Status", ""))
            version = flatten_values(props.get("Version", ""))

        # --- NEW: Extract Toggle Type & Toggle Name from properties ---
        props = node.get("properties", {}) or {}
        if "Toggle Type" in props:
            toggle_type = flatten_values(props.get("Toggle Type", ""))
        if "Toggle Name" in props:
            toggle_name = flatten_values(props.get("Toggle Name", ""))
        
        # --- NEW: Determine Action based on Policy FullPath ---
        policy_fullpath = " / ".join(fullpath_parts)
        lower_path = policy_fullpath.lower()
        if "check permissions" in lower_path:
            action = "checkpermission"
        elif "get permissions" in lower_path:
            action = "getpermission"
        elif "check user capabilities" in lower_path:
            action = "checkcapability"
        else:
            action = ""

        # --- NEW: Map originType directly to Type ---
        type_val = node.get("originType", "")

        # --- Append record (with new columns) ---
        records.append({
            "Position": position,
            "ID": node["originId"],
            "Policy FullPath": policy_fullpath,
            "Condition Path": cond_val_parent,
            "Condition ID": cond_id_parent,
            "Epic": epic,
            "Feature": feature,
            "Defect": defect,
            "Status": status,
            "Version": version,
            "Action": action,              # ← NEW
            "Type": type_val,              # ← NEW
            "Toggle Type": toggle_type,    # ← NEW
            "Toggle Name": toggle_name     # ← NEW
        })

        for cd in origin_to_cdnode.get(origin_id, []):
            for i, inp_id in enumerate(cd.get("inputNodes", []), 1):
                tmn = id_lookup.get(inp_id)
                if not tmn or tmn["class"] != "TargetMatchNode":
                    continue
                child_id = tmn.get("metadataId")
                if child_id:
                    traverse(child_id, fullpath_parts, f"{position}.{i}")

    package_meta = next((m for m in data if m.get("class") in ("Package", "DeploymentPackage")), None)
    if not package_meta:
        raise ValueError("No rootEntityId found in deployment package.")

    root_id = package_meta.get("rootEntityId")
    if root_id:
        traverse(root_id, [], "1")

    return pd.DataFrame(records)

# -----------------------------
# Action Extraction
# -----------------------------
def resolve_constants(node_id, id_lookup, attr_defs, seen=None):
    if seen is None:
        seen = set()
    if not node_id or node_id in seen:
        return []
    seen.add(node_id)
    node = id_lookup.get(node_id) or attr_defs.get(node_id)
    if not node:
        return []
    results = []
    cls = node.get("class")
    if cls == "ConstantNode":
        val = node.get("value") or node.get("constant")
        if val is not None:
            results.append(str(val))
    elif cls == "ConditionDefinition" and node.get("condition"):
        results.extend(resolve_constants(node["condition"], id_lookup, attr_defs, seen))
    elif cls == "ConditionReferenceNode":
        ref = node.get("definitionId")
        results.extend(resolve_constants(ref, id_lookup, attr_defs, seen))
    elif cls in ("BooleanLogicNode", "ComparisonNode", "StatementNode"):
        for field in ("inputNode", "lhsInputNode", "rhsInputNode", "guardNode"):
            if node.get(field):
                results.extend(resolve_constants(node[field], id_lookup, attr_defs, seen))
        for field in ("inputNodes", "statements"):
            for child in node.get(field, []):
                results.extend(resolve_constants(child, id_lookup, attr_defs, seen))
    return list(set(results))

def extract_actions(condition_defs, id_lookup, attr_defs):
    records = []
    for cond in condition_defs:
        if cond.get("name", "").startswith("ACTION."):
            vals = resolve_constants(cond["id"], id_lookup, attr_defs)
            records.append({
                "Action ID": cond["id"],
                "Full Path": cond.get("name", ""),
                "Value_action": ";".join(vals) if vals else ""
            })
    return pd.DataFrame(records)

# -----------------------------
# Targeting Extraction
# -----------------------------
def extract_targeting(condition_defs, id_lookup):
    def collect_linked_action_conditions_with_ids(root_node_id):
        stack, seen, results = [root_node_id], set(), set()
        while stack:
            nid = stack.pop()
            if not nid or nid in seen:
                continue
            seen.add(nid)
            node = id_lookup.get(nid, {})
            if not node:
                continue
            if node.get("class") == "ConditionReferenceNode":
                ref_id = node.get("definitionId") or node.get("ref") or node.get("conditionId")
                if ref_id:
                    ref_node = id_lookup.get(ref_id)
                    if ref_node and ref_node.get("class") == "ConditionDefinition" and ref_node.get("name", "").startswith("ACTION."):
                        results.add((ref_node["id"], ref_node["name"]))
            for key in ("inputNode", "guardNode", "condition", "lhsInputNode", "rhsInputNode"):
                val = node.get(key)
                if isinstance(val, str):
                    stack.append(val)
            if isinstance(node.get("inputNodes"), list):
                stack.extend(node.get("inputNodes"))
        return results

    targeting_records = []
    for cond in condition_defs:
        name = cond.get("name", "")
        if name.startswith("POLICY.TARGETING"):
            linked = collect_linked_action_conditions_with_ids(cond["id"])
            action_ids = ";".join(sorted({lid for lid, lname in linked}))
            action_names = ";".join(sorted({lname for lid, lname in linked}))
            targeting_records.append({
                "Condition ID": cond["id"],
                "Full Path": name,
                "Category": "Targeting",
                "Action ID": action_ids,
                "Value_action": action_names
            })
    return pd.DataFrame(targeting_records)

# -----------------------------
# Merge Logic
# -----------------------------
def merge_datasets(df_policy_tree, df_action, df_policy_targeting):
    merged_df = df_policy_tree.copy()
    if "Condition ID" in merged_df.columns:
        merged_df = merged_df.merge(df_policy_targeting, on="Condition ID", how="left")
    if "Action ID" in merged_df.columns:
        merged_df = merged_df.merge(df_action, on="Action ID", how="left", suffixes=("_target", "_action"))
    if "Value_action_action" in merged_df.columns:
        merged_df["Value_action"] = merged_df["Value_action_action"]
        merged_df.drop(columns=["Value_action_action", "Value_action_target"], errors="ignore", inplace=True)
    return merged_df

# -----------------------------
# Toggle JSON generator
# -----------------------------


# ---------- Core Generator Function ----------
def generate_package_toggle(merged_df):
    """
    Generate the full package toggle JSON from a merged DataFrame.
    Expects columns: ID, Type, Action, Status, Version, Toggle Type, Toggle Name, Value_action
    """

    # ---------- Normalize column names ----------
    df = merged_df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip().str.lower().replace("nan", np.nan)

    # ---------- Fill hierarchy columns ----------
    for c in ["value_action", "toggle_type", "toggle_name"]:
        if c in df.columns:
            df[c] = df[c].ffill()

    # ---------- Filter only checkpermission rows ----------
    if "action" not in df.columns:
        raise ValueError("Input DataFrame must contain an 'Action' column")
    df_check = df[df["action"] == "checkpermission"].copy()

    # ---------- Helper: get id for given toggle_type/toggle_name ----------
    def get_id_for(group, toggle_type, toggle_name):
        row = group[
            (group["toggle_type"] == toggle_type)
            & (group["toggle_name"] == toggle_name)
        ]
        return str(row["id"].iloc[0]) if not row.empty else None

    # ---------- Core hierarchy builder ----------
    def build_hierarchy(group):
        result = {"isEnabled": True}
        toggle_levels = [
            lvl for lvl in group["toggle_type"].dropna().unique().tolist()
            if lvl and lvl != "nan"
        ]
        current_level = result

        # Build nested toggle levels dynamically
        for toggle_type in toggle_levels:
            toggle_names = [
                n for n in group[group["toggle_type"] == toggle_type]["toggle_name"]
                .dropna().unique().tolist() if n and n != "nan"
            ]
            if not toggle_names:
                continue

            items = []
            for name in toggle_names:
                node_id = get_id_for(group, toggle_type, name)
                node = {"name": name, "isEnabled": True}
                if node_id:
                    node["id"] = node_id
                items.append(node)
            current_level[toggle_type] = items
            current_level = items[0]  # descend into next level

        # Determine ON/OFF logic
        off_versions = group[group["status"] == "off"]
        on_versions = group[group["status"] == "on"]
        total = len(group)

        if total > 0 and len(on_versions) == total:
            return None  # all ON → skip

        if len(off_versions) == 1:
            # Single OFF → parent disabled
            current_level["isEnabled"] = False
        elif len(off_versions) > 1:
            versions = []
            for _, row in off_versions.iterrows():
                versions.append({
                    "name": str(row["version"]),
                    "id": str(row["id"]),
                    "type": str(row["type"]),
                    "isEnabled": False
                })
            current_level["versions"] = versions

        return result

    # ---------- Build JSON structure ----------
    output_data = []
    for value_action, group in df_check.groupby("value_action"):
        if not value_action or str(value_action).lower() == "nan":
            continue
        if len(group[group["status"] == "off"]) == 0:
            continue

        hierarchy = build_hierarchy(group)
        if hierarchy is None:
            continue

        entry = {
            "action": str(value_action),
            "id": str(group["id"].iloc[0]),
            "isEnabled": True
        }
        entry.update(hierarchy)
        output_data.append(entry)

    # ---------- Final JSON wrapper ----------
    final_json = {
        "schemaVersion": "1.0.0",
        "strategy": "blacklist",
        "toggles": {"checkPermissions": output_data}
    }

    # Save to file
    with open("package_toggle.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, indent=2)

    print(json.dumps(final_json, indent=2))
    print("\n✅ JSON saved as package_toggle.json")



def generate_package_toggle(merged_df):
    # Filter relevant rows
    df = merged_df[merged_df['Policy FullPath'].str.contains('Check Permission', case=False, na=False)].copy()

    # Normalize Value_action and Status
    df['Value_action'] = df['Value_action'].astype(str).str.strip().str.lower()
    df['Status'] = df['Status'].astype(str).str.strip().str.upper()

    # Drop invalid actions
    invalid_actions = ["", "NAN", "NONE", "NULL"]
    df = df[~df['Value_action'].isin(invalid_actions)]

    # Mark real status
    df['has_status'] = ~df['Status'].isin(["", "NAN", "NONE", "NULL"])

    toggles = []

    # Build per action logic — enabled if *any descendant path* with same action has status
    for action in sorted(df['Value_action'].unique()):
        if action in ("nan", "", "none", "null"):
            continue  # Skip invalid
        subset = df[df['Value_action'] == action]
        enabled = bool(subset['has_status'].any())

        # If not directly enabled, check descendant policy paths
        if not enabled:
            paths = subset['Policy FullPath'].tolist()
            for p in df[df['has_status']]['Policy FullPath']:
                if any(p.startswith(path) for path in paths):
                    enabled = True
                    break

        toggles.append({
            "action": str(action),
            "isEnabled": bool(enabled)
        })
        print(f"DEBUG → {action} | Enabled={enabled}")

    package_toggle = {
        "schemaVersion": "1.0.0",
        "strategy": "blacklist",
        "toggles": {"checkPermissions": toggles}
    }

    # Dump to JSON (convert NumPy bools)
    with open("package_toggle.json", "w", encoding="utf-8") as f:
        json.dump(package_toggle, f, indent=2, ensure_ascii=False)

    print(f"✅ package_toggle.json created successfully with {len(toggles)} toggles.")

from collections import OrderedDict
import json
import os

def merge_with_prod_toggle(package_toggle_path, prod_toggle_path, output_path="merged_toggle.json"):
    """Merge new package_toggle.json with prod_toggle.json while preserving group order."""
    with open(package_toggle_path, "r", encoding="utf-8") as f:
        package_data = json.load(f)
    with open(prod_toggle_path, "r", encoding="utf-8") as f:
        prod_data = json.load(f, object_pairs_hook=OrderedDict)

    merged = OrderedDict()
    merged["schemaVersion"] = package_data.get("schemaVersion", "1.0.0")
    merged["strategy"] = package_data.get("strategy", "blacklist")
    merged["toggles"] = OrderedDict()

    # Build a lookup for prod checkPermissions actions
    prod_cp_lookup = {
        i.get("action"): i
        for i in prod_data.get("toggles", {}).get("checkPermissions", [])
    }

    # Build merged checkPermissions list
    merged_cp = []
    for pkg_item in package_data.get("toggles", {}).get("checkPermissions", []):
        action = pkg_item.get("action")
        if action in prod_cp_lookup:
            merged_cp.append(prod_cp_lookup[action])  # Replace with prod version
            print(f"→ Replaced from prod: {action}")
        else:
            merged_cp.append(pkg_item)  # Keep package version
            print(f"→ Kept from package: {action}")

    # Preserve prod toggle group order
    for key in prod_data.get("toggles", OrderedDict()).keys():
        if key == "checkPermissions":
            merged["toggles"][key] = merged_cp
        else:
            merged["toggles"][key] = prod_data["toggles"][key]

    # Append any *new* toggle groups that exist in package but not prod
    for key, value in package_data.get("toggles", {}).items():
        if key not in merged["toggles"]:
            merged["toggles"][key] = value
            print(f"→ Added new toggle group from package: {key}")

    # Save with preserved order
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    print(f"✅ Merged toggle file created with preserved order: {os.path.abspath(output_path)}")

# -----------------------------
# CLI Runner
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Export Policy Tree, Action, Targeting, and Package Toggle JSON.")
    parser.add_argument("-d", "--deployment", required=True, help="Path to deployment package (.deploymentpackage)")
    parser.add_argument("-o", "--output", default="Policy_Export.xlsx", help="Output Excel filename (default: Policy_Export.xlsx)")
    parser.add_argument("-p", "--prod-toggle", help="Path to existing prod_toggle.json for merging")
    
    args = parser.parse_args()

    data = safe_load_json(args.deployment)
    condition_defs = [d for d in data if d.get("class") == "ConditionDefinition"]
    attr_defs = {o["id"]: o for o in data if o.get("class") == "AttributeDefinition"}
    id_lookup = {d["id"]: d for d in data if "id" in d}

    df_policy_tree = build_policy_tree(data)
    df_action = extract_actions(condition_defs, id_lookup, attr_defs)
    df_policy_targeting = extract_targeting(condition_defs, id_lookup)
    df_merged = merge_datasets(df_policy_tree, df_action, df_policy_targeting)

    with pd.ExcelWriter(args.output, engine="xlsxwriter") as writer:
        df_policy_tree.to_excel(writer, sheet_name="Policy_Tree", index=False)
        df_action.to_excel(writer, sheet_name="Action", index=False)
        df_policy_targeting.to_excel(writer, sheet_name="Policy_Targeting", index=False)
        df_merged.to_excel(writer, sheet_name="Merged", index=False)

    # Generate the JSON toggle file
    generate_package_toggle(df_merged)

    # --- merge with prod if provided ---
    if args.prod_toggle:
        merge_with_prod_toggle("package_toggle.json", args.prod_toggle, "merged_toggle.json")


    print(f"✅ Export complete: {os.path.abspath(args.output)}")

if __name__ == "__main__":
    main()
