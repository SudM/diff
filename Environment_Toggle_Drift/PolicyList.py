import json
import pandas as pd
import xlsxwriter

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
# Condition ID collector
# -----------------------------
def collect_condition_def_ids_from_tree(id_map, root_node_id):
    """Collect all ConditionDefinition IDs from a guardNode tree."""
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
# pick_single_condition_path logic
# -----------------------------
def pick_single_condition_path(cond_names):
    """Select the most relevant condition path string from candidate names."""
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
# Policy Tree (PolicySet + Policy only)
# -----------------------------
def build_policy_tree(data):
    """
    Build the Policy Tree with correct Condition ID logic.
    Includes only PolicySet and Policy nodes.
    """
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

        # ✅ Corrected Condition ID selection
        cond_val_parent = pick_single_condition_path(list(set(cond_names_parent)))
        cond_id_parent = next(
            (c["id"] for c in condition_defs if c.get("name") == cond_val_parent),
            ""
        )

        # Extract Entitlement Check properties
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
            "ID": node["originId"],
            "Policy FullPath": " / ".join(fullpath_parts),
            "Condition Path": cond_val_parent,
            "Condition ID": cond_id_parent,
            "Epic": epic,
            "Feature": feature,
            "Defect": defect,
            "Status": status,
            "Version": version
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
# Runner
# -----------------------------
def main():
    deployment_path = "test.deploymentpackage"
    data = safe_load_json(deployment_path)

    condition_defs = [d for d in data if d.get("class") == "ConditionDefinition"]
    attr_defs = {o["id"]: o for o in data if o.get("class") == "AttributeDefinition"}
    id_lookup = {d["id"]: d for d in data if "id" in d}

    df_policy_tree = build_policy_tree(data)
    df_action = extract_actions(condition_defs, id_lookup, attr_defs)
    df_policy_targeting = extract_targeting(condition_defs, id_lookup)
    df_merged = merge_datasets(df_policy_tree, df_action, df_policy_targeting)

    output_path = "Policy_Export.xlsx"
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        df_policy_tree.to_excel(writer, sheet_name="Policy_Tree", index=False)
        workbook = writer.book
        worksheet = writer.sheets["Policy_Tree"]
        yellow_fmt = workbook.add_format({"bg_color": "#FFF200"})  # Yellow
        worksheet.freeze_panes(1, 0)
        worksheet.autofilter(0, 0, len(df_policy_tree), len(df_policy_tree.columns) - 1)

        # Highlight entitlement checks
        for row_num, value in enumerate(df_policy_tree["Policy FullPath"], start=1):
            if "entitlement check" in str(value).lower():
                worksheet.set_row(row_num, None, yellow_fmt)

        df_action.to_excel(writer, sheet_name="Action", index=False)
        df_policy_targeting.to_excel(writer, sheet_name="Policy_Targeting", index=False)
        df_merged.to_excel(writer, sheet_name="Merged", index=False)

    print(f"✅ Export complete: {output_path}")

if __name__ == "__main__":
    main()
