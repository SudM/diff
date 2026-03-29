#!/usr/bin/env python3
"""
generate_policy_list_all_filterable.py
-------------------------------------
Generates a complete Policy Tree, Action, and Targeting report from a
PingAuthorize deployment package (.deploymentpackage) and exports the
results to Excel with all policies retained so the user can filter by
Condition Path in Excel.
"""

import argparse
import json
import logging
import os
import sys

import pandas as pd

LOG_FILE = "toggle_operation.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def safe_load_json(path):
    """Safely load JSON data from file."""
    try:
        logging.info("Loading JSON from %s", path)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.exception("Error loading JSON file: %s", e)
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
    condition_name_by_id = {c["id"]: c.get("name", "") for c in condition_defs if c.get("id")}
    condition_id_by_name = {c.get("name", ""): c["id"] for c in condition_defs if c.get("id") and c.get("name")}
    records = []

    def traverse(origin_id, path_names, position):
        node = metadata_lookup.get(origin_id)
        if not node:
            return

        fullpath_parts = path_names + [f"{node['originType']}:{node['name']}"]

        cond_names_parent = []
        all_condition_names = set()
        all_condition_ids = set()

        for cd in origin_to_cdnode.get(origin_id, []):
            if cd.get("guardNode"):
                for cid in collect_condition_def_ids_from_tree(id_lookup, cd["guardNode"]):
                    cond_name = condition_name_by_id.get(cid)
                    if cond_name:
                        cond_names_parent.append(cond_name)
                        all_condition_names.add(cond_name)
                    if cid:
                        all_condition_ids.add(cid)

        cond_val_parent = pick_single_condition_path(list(set(cond_names_parent)))
        cond_id_parent = condition_id_by_name.get(cond_val_parent, "")

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
            "All Condition Paths": ";".join(sorted(all_condition_names)),
            "All Condition IDs": ";".join(sorted(all_condition_ids)),
            "Epic": epic,
            "Feature": feature,
            "Defect": defect,
            "Status": status,
            "Version": version,
            "Service": action,
            "Type": node.get("originType", ""),
            "Toggle Type": toggle_type,
            "Toggle Name": toggle_name,
        })

        for cd in origin_to_cdnode.get(origin_id, []):
            for i, inp_id in enumerate(cd.get("inputNodes", []), 1):
                tmn = id_lookup.get(inp_id)
                if tmn and tmn.get("class") == "TargetMatchNode" and tmn.get("metadataId"):
                    traverse(tmn["metadataId"], fullpath_parts, f"{position}.{i}")

    package_meta = next((m for m in data if m.get("class") in ("Package", "DeploymentPackage")), None)
    if not package_meta:
        logging.error("No rootEntityId found in deployment package.")
        print("❌ Invalid deployment package. Missing rootEntityId.")
        sys.exit(1)

    root_id = package_meta.get("rootEntityId")
    if root_id:
        traverse(root_id, [], "1")

    logging.info("Policy tree built with %s rows.", len(records))
    return pd.DataFrame(records)



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
                "Value_action": ";".join(vals) if vals else "",
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
                    if (
                        ref_node
                        and ref_node.get("class") == "ConditionDefinition"
                        and ref_node.get("name", "").startswith("ACTION.")
                    ):
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
                "Value_action": action_names,
            })

    logging.info("Extracted %s targeting conditions.", len(targeting_records))
    return pd.DataFrame(targeting_records)



def merge_datasets(df_policy_tree, df_action, df_policy_targeting):
    """Merge datasets while keeping columns needed for filtering."""
    try:
        merged_df = df_policy_tree.copy()

        if "Condition ID" in merged_df.columns and "Condition ID" in df_policy_targeting.columns:
            merged_df = merged_df.merge(df_policy_targeting, on="Condition ID", how="left")
        else:
            logging.warning("Skipping merge with Policy_Targeting: missing 'Condition ID'.")

        if "Action ID" in merged_df.columns and "Action ID" in df_action.columns:
            merged_df = merged_df.merge(df_action, on="Action ID", how="left", suffixes=("_target", "_action"))
        else:
            logging.warning("Skipping merge with Action: missing 'Action ID'.")

        if "Value_action_action" in merged_df.columns:
            merged_df["action_value"] = merged_df["Value_action_action"]

        merged_df.drop(
            columns=[
                "Value_action_action",
                "Value_action_target",
                "Full Path_target",
                "Category",
                "Full Path_action",
            ],
            errors="ignore",
            inplace=True,
        )

        logging.info("Merged dataset created with %s rows.", len(merged_df))
        return merged_df

    except KeyError as ke:
        logging.exception("KeyError during merge: %s", ke)
        print("❌ Merge failed (missing required column). Check toggle_operation.log for details.")
        sys.exit(1)

    except Exception as e:
        logging.exception("Unexpected merge error: %s", e)
        print("❌ Unexpected error during merge. Check log file for details.")
        sys.exit(1)



def apply_excel_formatting(writer, df, sheet_name):
    """Enable Excel autofilter and freeze header row."""
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    max_row, max_col = df.shape
    worksheet.autofilter(0, 0, max_row, max_col - 1)
    worksheet.freeze_panes(1, 0)

    for idx, col in enumerate(df.columns):
        width = min(max(len(str(col)), df[col].astype(str).map(len).max() if not df.empty else 0) + 2, 60)
        worksheet.set_column(idx, idx, width)



def main():
    parser = argparse.ArgumentParser(
        description="Export all policies with all conditions so they can be filtered in Excel."
    )
    parser.add_argument("-d", "--deployment", required=True, help="Path to .deploymentpackage file")
    parser.add_argument("-o", "--output", default="./output/Policy_Export_All_Filterable.xlsx", help="Output Excel filename")
    args = parser.parse_args()

    try:
        logging.info("Starting full export for %s", args.deployment)
        data = safe_load_json(args.deployment)
        condition_defs = [d for d in data if d.get("class") == "ConditionDefinition"]
        attr_defs = {o["id"]: o for o in data if o.get("class") == "AttributeDefinition"}
        id_lookup = {d["id"]: d for d in data if "id" in d}

        df_policy_tree = build_policy_tree(data)
        df_action = extract_actions(condition_defs, id_lookup, attr_defs)
        df_targeting = extract_targeting(condition_defs, id_lookup)
        df_merged = merge_datasets(df_policy_tree, df_action, df_targeting)

        output_dir = os.path.dirname(args.output)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        with pd.ExcelWriter(args.output, engine="xlsxwriter") as writer:
            df_merged.to_excel(writer, sheet_name="Policy_Tree", index=False)
            df_policy_tree.to_excel(writer, sheet_name="Policy_Tree_Raw", index=False)
            df_targeting.to_excel(writer, sheet_name="Targeting_Conditions", index=False)
            df_action.to_excel(writer, sheet_name="Action_Conditions", index=False)

            apply_excel_formatting(writer, df_merged, "Policy_Tree")
            apply_excel_formatting(writer, df_policy_tree, "Policy_Tree_Raw")
            apply_excel_formatting(writer, df_targeting, "Targeting_Conditions")
            apply_excel_formatting(writer, df_action, "Action_Conditions")

        print(f"✅ Export complete: {os.path.abspath(args.output)}")
        print(f"✅ Total policies exported: {len(df_merged)}")
        print("✅ Excel filters enabled on all sheets.")

    except Exception as e:
        logging.exception("Critical error: %s", e)
        print("❌ Critical error occurred. Check toggle_operation.log for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
