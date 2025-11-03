#!/usr/bin/env python3
"""
generate_policy_list.py
-----------------------
Generates a Policy Tree, Action, and Targeting report from a PingAuthorize
deployment package (.deploymentpackage) and exports the results to Excel.
Then it builds a `package_toggle.json` and optionally merges it with
a production toggle file.

"""

import json
import pandas as pd
import argparse
import os
import numpy as np
import logging
from collections import OrderedDict
import sys

# ---------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------
LOG_FILE = "toggle_operation.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------
def safe_load_json(path):
    """Safely load JSON data from file."""
    try:
        logging.info(f"Loading JSON from {path}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.exception(f"Error loading JSON file: {e}")
        print("❌ Failed to load deployment package. Check log for details.")
        sys.exit(1)


def flatten_values(val):
    """Convert lists into comma-separated string."""
    if isinstance(val, list):
        return ",".join(str(v) for v in val)
    return val


def collect_condition_def_ids_from_tree(id_map, root_node_id):
    """Collect ConditionDefinition IDs linked from a CombinedDecisionNode."""
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


def pick_single_condition_path(cond_names):
    """Pick most specific condition name preferring POLICY.TARGETING."""
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

# ---------------------------------------------------------------------
# Policy Tree Builder
# ---------------------------------------------------------------------
def build_policy_tree(data):
    """Build a hierarchical policy DataFrame from deployment package."""
    logging.info("Building policy tree ...")
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

        props = node.get("properties", {}) or {}
        epic = flatten_values(props.get("Epic", ""))
        feature = flatten_values(props.get("Feature", ""))
        defect = flatten_values(props.get("Defect", ""))
        status = flatten_values(props.get("Status", ""))
        version = flatten_values(props.get("Version", ""))
        toggle_type = flatten_values(props.get("Toggle Type", ""))
        toggle_name = flatten_values(props.get("Toggle Name", ""))

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
            "Action": action,
            "Type": node.get("originType", ""),
            "Toggle Type": toggle_type,
            "Toggle Name": toggle_name
        })

        for cd in origin_to_cdnode.get(origin_id, []):
            for i, inp_id in enumerate(cd.get("inputNodes", []), 1):
                tmn = id_lookup.get(inp_id)
                if tmn and tmn["class"] == "TargetMatchNode" and tmn.get("metadataId"):
                    traverse(tmn["metadataId"], fullpath_parts, f"{position}.{i}")

    package_meta = next((m for m in data if m.get("class") in ("Package", "DeploymentPackage")), None)
    if not package_meta:
        logging.error("No rootEntityId found in deployment package.")
        print("❌ Invalid deployment package. Missing rootEntityId.")
        sys.exit(1)

    root_id = package_meta.get("rootEntityId")
    if root_id:
        traverse(root_id, [], "1")

    logging.info(f"Policy tree built with {len(records)} rows.")
    return pd.DataFrame(records)

# ---------------------------------------------------------------------
# Action and Targeting Extraction
# ---------------------------------------------------------------------
def resolve_constants(node_id, id_lookup, attr_defs, seen=None):
    """Recursively resolve ConstantNode values."""
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
    """Extract ACTION.* condition definitions."""
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


def extract_targeting(condition_defs, id_lookup):
    """Extract POLICY.TARGETING conditions and linked ACTION conditions."""
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
            action_ids = ";".join(sorted({lid for lid, _ in linked}))
            action_names = ";".join(sorted({lname for _, lname in linked}))
            targeting_records.append({
                "Condition ID": cond["id"],
                "Full Path": name,
                "Category": "Targeting",
                "Action ID": action_ids,
                "Value_action": action_names
            })
    logging.info(f"Extracted {len(targeting_records)} targeting conditions.")
    return pd.DataFrame(targeting_records)

# ---------------------------------------------------------------------
# Merge Logic with Robust Error Handling
# ---------------------------------------------------------------------
def merge_datasets(df_policy_tree, df_action, df_policy_targeting):
    """Merge datasets with safety checks and error handling."""
    try:
        merged_df = df_policy_tree.copy()

        if "Condition ID" in merged_df.columns and "Condition ID" in df_policy_targeting.columns:
            merged_df = merged_df.merge(df_policy_targeting, on="Condition ID", how="left")
        else:
            logging.warning("⚠️ Skipping merge with Policy_Targeting: missing 'Condition ID'.")

        if "Action ID" in merged_df.columns and "Action ID" in df_action.columns:
            merged_df = merged_df.merge(df_action, on="Action ID", how="left", suffixes=("_target", "_action"))
        else:
            logging.warning("⚠️ Skipping merge with Action: missing 'Action ID'.")

        if "Value_action_action" in merged_df.columns:
            merged_df["Value_action"] = merged_df["Value_action_action"]
            merged_df.drop(columns=["Value_action_action", "Value_action_target"], errors="ignore", inplace=True)

        logging.info(f"Merged dataset created with {len(merged_df)} rows.")
        return merged_df

    except KeyError as ke:
        logging.exception(f"KeyError during merge: {ke}")
        print("❌ Merge failed (missing required column). Check toggle_operation.log for details.")
        sys.exit(1)

    except Exception as e:
        logging.exception(f"Unexpected merge error: {e}")
        print("❌ Unexpected error during merge. Check log file for details.")
        sys.exit(1)

# ---------------------------------------------------------------------
# Toggle JSON Generator
# ---------------------------------------------------------------------
def generate_package_toggle(merged_df):
    """Generate package_toggle.json from merged DataFrame."""
    logging.info("Generating package_toggle.json ...")
    df = merged_df[merged_df['Policy FullPath'].str.contains('Check Permission', case=False, na=False)].copy()
    df['Value_action'] = df['Value_action'].astype(str).str.strip().str.lower()
    df['Status'] = df['Status'].astype(str).str.strip().str.upper()
    invalid_actions = ["", "NAN", "NONE", "NULL"]
    df = df[~df['Value_action'].isin(invalid_actions)]
    df['has_status'] = ~df['Status'].isin(["", "NAN", "NONE", "NULL"])

    toggles = []
    for action in sorted(df['Value_action'].unique()):
        if action in ("nan", "", "none", "null"):
            continue
        subset = df[df['Value_action'] == action]
        enabled = bool(subset['has_status'].any())
        if not enabled:
            paths = subset['Policy FullPath'].tolist()
            for p in df[df['has_status']]['Policy FullPath']:
                if any(p.startswith(path) for path in paths):
                    enabled = True
                    break
        toggles.append({"action": str(action), "isEnabled": bool(enabled)})
        logging.info(f"Action {action} | Enabled={enabled}")

    package_toggle = {
        "schemaVersion": "1.0.0",
        "strategy": "blacklist",
        "toggles": {"checkPermissions": toggles}
    }

    with open("package_toggle.json", "w", encoding="utf-8") as f:
        json.dump(package_toggle, f, indent=2, ensure_ascii=False)
    logging.info(f"✅ package_toggle.json created ({len(toggles)} toggles)")

# ---------------------------------------------------------------------
# Merge with Production Toggle
# ---------------------------------------------------------------------
def merge_with_prod_toggle(package_toggle_path, prod_toggle_path, output_path="merged_toggle.json"):
    """Merge new package_toggle.json with production toggle."""
    logging.info("Merging with production toggle ...")
    with open(package_toggle_path, "r", encoding="utf-8") as f:
        package_data = json.load(f)
    with open(prod_toggle_path, "r", encoding="utf-8") as f:
        prod_data = json.load(f, object_pairs_hook=OrderedDict)

    merged = OrderedDict()
    merged["schemaVersion"] = package_data.get("schemaVersion", "1.0.0")
    merged["strategy"] = package_data.get("strategy", "blacklist")
    merged["toggles"] = OrderedDict()

    prod_cp_lookup = {
        i.get("action"): i for i in prod_data.get("toggles", {}).get("checkPermissions", [])
    }

    merged_cp = []
    for pkg_item in package_data.get("toggles", {}).get("checkPermissions", []):
        action = pkg_item.get("action")
        if action in prod_cp_lookup:
            merged_cp.append(prod_cp_lookup[action])
            logging.info(f"→ Replaced from prod: {action}")
        else:
            merged_cp.append(pkg_item)
            logging.info(f"→ Kept from package: {action}")

    for key in prod_data.get("toggles", OrderedDict()).keys():
        merged["toggles"][key] = merged_cp if key == "checkPermissions" else prod_data["toggles"][key]

    for key, value in package_data.get("toggles", {}).items():
        if key not in merged["toggles"]:
            merged["toggles"][key] = value
            logging.info(f"→ Added new toggle group: {key}")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    logging.info(f"✅ Merged toggle saved: {os.path.abspath(output_path)}")

# ---------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------
def main():
    """Main CLI entry point for policy export and toggle generation."""
    parser = argparse.ArgumentParser(description="Export Policy Tree and generate Toggle JSON.")
    parser.add_argument("-d", "--deployment", required=True, help="Path to .deploymentpackage file")
    parser.add_argument("-o", "--output", default="Policy_Export.xlsx", help="Output Excel filename")
    parser.add_argument("-p", "--prod-toggle", help="Optional path to production toggle for merging")
    args = parser.parse_args()

    try:
        logging.info(f"Starting export for {args.deployment}")
        data = safe_load_json(args.deployment)
        condition_defs = [d for d in data if d.get("class") == "ConditionDefinition"]
        attr_defs = {o["id"]: o for o in data if o.get("class") == "AttributeDefinition"}
        id_lookup = {d["id"]: d for d in data if "id" in d}

        df_policy_tree = build_policy_tree(data)
        df_action = extract_actions(condition_defs, id_lookup, attr_defs)
        df_targeting = extract_targeting(condition_defs, id_lookup)

        df_merged = merge_datasets(df_policy_tree, df_action, df_targeting)

        with pd.ExcelWriter(args.output, engine="xlsxwriter") as writer:
            df_policy_tree.to_excel(writer, sheet_name="Policy_Tree", index=False)
            df_action.to_excel(writer, sheet_name="Action", index=False)
            df_targeting.to_excel(writer, sheet_name="Policy_Targeting", index=False)
            df_merged.to_excel(writer, sheet_name="Merged", index=False)

        generate_package_toggle(df_merged)

        if args.prod_toggle:
            merge_with_prod_toggle("package_toggle.json", args.prod_toggle, "merged_toggle.json")

        logging.info(f"✅ Export complete: {os.path.abspath(args.output)}")
        print(f"✅ Export complete: {os.path.abspath(args.output)}")

    except Exception as e:
        logging.exception(f"Critical error: {e}")
        print("❌ Critical error occurred. Check toggle_operation.log for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
