import json
import pandas as pd
import argparse
import os

# -----------------------------
# Helpers
# -----------------------------
def safe_load_json(path):
    """Safely load a JSON deployment package."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def flatten_values(val):
    """Flatten list-type property values to comma-separated strings."""
    if isinstance(val, list):
        return ",".join(str(v) for v in val)
    return val


# -----------------------------
# Policy Tree (Full Traversal)
# -----------------------------
def build_policy_tree(data):
    metadata_nodes = [d for d in data if d.get("class") == "Metadata"]
    id_lookup = {d["id"]: d for d in data if "id" in d}
    cd_nodes = {d["id"]: d for d in data if d.get("class") == "CombinedDecisionNode"}

    # Map originLink → CombinedDecisionNode list
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

        # Extract Epic/Feature/Defect/Status/Version for entitlement check nodes
        epic = feature = defect = status = version = ""
        if node.get("originType") == "PolicySet" and str(node.get("name", "")).lower().startswith("entitlement check"):
            props = node.get("properties", {}) or {}
            epic = flatten_values(props.get("Epic", ""))
            feature = flatten_values(props.get("Feature", ""))
            defect = flatten_values(props.get("Defect", ""))
            status = flatten_values(props.get("Status", ""))
            version = flatten_values(props.get("Version", ""))

        records.append({
            "Position": position,
            "ID": node.get("originId"),
            "Policy FullPath": " / ".join(fullpath_parts),
            "Condition ID": node.get("conditionId", ""),
            "Epic": epic,
            "Feature": feature,
            "Defect": defect,
            "Status": status,
            "Version": version
        })

        # Traverse children
        for cd in origin_to_cdnode.get(origin_id, []):
            for i, inp_id in enumerate(cd.get("inputNodes", []), 1):
                tmn = id_lookup.get(inp_id)
                if not tmn or tmn["class"] != "TargetMatchNode":
                    continue
                child_id = tmn.get("metadataId")
                if child_id:
                    traverse(child_id, fullpath_parts, f"{position}.{i}")

    # Root detection
    package_meta = next((m for m in data if m.get("class") in ("Package", "DeploymentPackage")), None)
    root_id = package_meta.get("rootEntityId") if package_meta else None

    if root_id:
        traverse(root_id, [], "1")

    return pd.DataFrame(records)


# -----------------------------
# Action Extraction
# -----------------------------
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
    """Extract ACTION.* conditions."""
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
    """Extract POLICY.TARGETING.* conditions and linked actions."""
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
    """Join Policy_Tree, Action, and Targeting datasets logically."""
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
# CLI Entry Point
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Extract Policy Tree, Actions, Targeting, and Merged datasets from a deployment package.")
    parser.add_argument("-d", "--deployment", required=True, help="Path to the .deploymentpackage JSON file")
    parser.add_argument("-o", "--output", default="Full_Extract_WithMerge.xlsx", help="Output Excel filename")
    args = parser.parse_args()

    if not os.path.exists(args.deployment):
        print(f"❌ Deployment file not found: {args.deployment}")
        return

    print(f"📦 Loading deployment package: {args.deployment}")
    data = safe_load_json(args.deployment)

    condition_defs = [d for d in data if d.get("class") == "ConditionDefinition"]
    attr_defs = {o["id"]: o for o in data if o.get("class") == "AttributeDefinition"}
    id_lookup = {d["id"]: d for d in data if "id" in d}

    print("🔧 Building datasets...")
    df_policy_tree = build_policy_tree(data)
    df_action = extract_actions(condition_defs, id_lookup, attr_defs)
    df_policy_targeting = extract_targeting(condition_defs, id_lookup)
    df_merged = merge_datasets(df_policy_tree, df_action, df_policy_targeting)

    # Export to Excel
    with pd.ExcelWriter(args.output, engine="xlsxwriter") as writer:
        df_policy_tree.to_excel(writer, sheet_name="Policy_Tree", index=False)
        df_action.to_excel(writer, sheet_name="Action", index=False)
        df_policy_targeting.to_excel(writer, sheet_name="Policy_Targeting", index=False)
        df_merged.to_excel(writer, sheet_name="Merged", index=False)

    print(f"✅ Export complete: {args.output}")


if __name__ == "__main__":
    main()
