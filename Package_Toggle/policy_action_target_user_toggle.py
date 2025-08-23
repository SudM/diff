import json
import pandas as pd
from typing import Dict, Set

# ---------- Load Deployment Package ----------
file_path = "."
with open(file_path, "r") as f:
    data = json.load(f)

# ---------- Build Lookups ----------
metadata_nodes = [d for d in data if d.get("class") == "Metadata"]
condition_defs = [d for d in data if d.get("class") == "ConditionDefinition"]
attr_defs = {o["id"]: o for o in data if o.get("class") == "AttributeDefinition"}
id_lookup = {d["id"]: d for d in data if "id" in d}

cd_nodes = {d["id"]: d for d in data if d.get("class") == "CombinedDecisionNode"}
origin_to_cdnode = {}
for cd in cd_nodes.values():
    if cd.get("originLink"):
        origin_to_cdnode.setdefault(cd["originLink"], []).append(cd)

metadata_lookup = {m["originId"]: m for m in metadata_nodes}

# ---------- Shared Helpers ----------
def resolve_constants(node_id, seen=None):
    if seen is None: seen=set()
    if not node_id or node_id in seen: return []
    seen.add(node_id)
    node = id_lookup.get(node_id) or attr_defs.get(node_id)
    if not node: return []
    results=[]
    cls=node.get("class")

    if cls=="ConstantNode":
        val=node.get("value") or node.get("constant")
        if val is not None:
            results.append(str(val))

    elif cls=="AttributeNode":
        adid=node.get("attributeDefinitionId")
        ad=attr_defs.get(adid)
        if ad:
            if ad.get("resolvers"):
                for r in ad["resolvers"]:
                    val=r.get("value")
                    if val is not None:
                        results.append(str(val))
            if ad.get("defaultValue"):
                results.append(str(ad["defaultValue"]))

    elif cls=="AttributeDefinition":
        if node.get("resolvers"):
            for r in node["resolvers"]:
                val=r.get("value")
                if val is not None:
                    results.append(str(val))
        if node.get("defaultValue"):
            results.append(str(node["defaultValue"]))

    elif cls=="ConditionDefinition":
        if node.get("condition"):
            results.extend(resolve_constants(node["condition"], seen))

    elif cls=="ConditionReferenceNode":
        ref=node.get("definitionId")
        results.extend(resolve_constants(ref, seen))

    elif cls in ("BooleanLogicNode","ComparisonNode","StatementNode"):
        for field in ("inputNode","lhsInputNode","rhsInputNode","guardNode"):
            if node.get(field):
                results.extend(resolve_constants(node[field], seen))
        for field in ("inputNodes","statements"):
            for child in node.get(field,[]):
                results.extend(resolve_constants(child, seen))

    return list(set(results))

def collect_condition_def_ids_from_tree(id_map: Dict[str, dict], root_node_id: str) -> Set[str]:
    stack = [root_node_id]
    seen = set()
    result = set()
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
    targeting = [c for c in cond_names if c.startswith("POLICY.TARGETING")]
    toggles = [c for c in cond_names if c.startswith("POLICY.TOGGLES")]

    def longest_by_segments(cands):
        return max(cands, key=lambda s: len(s.split("."))) if cands else ""

    if targeting:
        action_first = [c for c in targeting if ".ACTION." in c]
        if action_first:
            return longest_by_segments(action_first)
        return longest_by_segments(targeting)
    if toggles:
        return longest_by_segments(toggles)
    return ""

# ---------- Policy Tree Dataset ----------
def find_condition_id_by_name(name):
    for c in condition_defs:
        if c.get("name") == name:
            return c["id"]
    return ""

policy_tree_records = []

def traverse_policyset_precise_with_condid(origin_id, path_names, path_ids, position):
    node = metadata_lookup.get(origin_id)
    if not node:
        return

    fullpath = " / ".join(path_names + [f"{node['originType']}:{node['name']}"])
    fullpath_id = " / ".join(path_ids + [f"{node['originType']}:{node['originId']}"])

    # Parent-level conditions
    cond_names_parent = []
    for cd in origin_to_cdnode.get(origin_id, []):
        if cd.get("guardNode"):
            for cid in collect_condition_def_ids_from_tree(id_lookup, cd["guardNode"]):
                cond = next((c for c in condition_defs if c["id"] == cid), None)
                if cond and cond.get("name"):
                    cond_names_parent.append(cond["name"])
    cond_val_parent = pick_single_condition_path(list(set(cond_names_parent)))
    cond_id_parent = find_condition_id_by_name(cond_val_parent)

    policy_tree_records.append({
        "Position": position,
        "ID": node["originId"],
        "Policy FullPath": fullpath,
        "Policy FullPath ID": fullpath_id,
        "Condition Path": cond_val_parent,
        "Condition ID": cond_id_parent
    })

    # Child TargetMatchNodes
    for cd in origin_to_cdnode.get(origin_id, []):
        for i, inp_id in enumerate(cd.get("inputNodes", []), 1):
            tmn = id_lookup.get(inp_id)
            if not tmn or tmn["class"] != "TargetMatchNode":
                continue

            cond_names_child = []
            for cid in collect_condition_def_ids_from_tree(id_lookup, tmn.get("inputNode")):
                cond = next((c for c in condition_defs if c["id"] == cid), None)
                if cond and cond.get("name"):
                    cond_names_child.append(cond["name"])
            if tmn.get("guardNode"):
                for cid in collect_condition_def_ids_from_tree(id_lookup, tmn["guardNode"]):
                    cond = next((c for c in condition_defs if c["id"] == cid), None)
                    if cond and cond.get("name"):
                        cond_names_child.append(cond["name"])

            cond_val_child = pick_single_condition_path(list(set(cond_names_child)))
            cond_id_child = find_condition_id_by_name(cond_val_child)

            child_id = tmn.get("metadataId")
            if child_id:
                traverse_policyset_precise_with_condid(
                    child_id,
                    path_names + [f"{node['originType']}:{node['name']}"],
                    path_ids + [f"{node['originType']}:{node['originId']}"],
                    f"{position}.{i}"
                )
                # override child record
                policy_tree_records[-1]["Condition Path"] = cond_val_child
                policy_tree_records[-1]["Condition ID"] = cond_id_child

# Run traversal from root
root_id = next((m["originId"] for m in metadata_nodes if m["name"] == "Root"), None)
policy_tree_records.clear()
traverse_policyset_precise_with_condid(root_id, [], [], "1")
df_policy_tree = pd.DataFrame(policy_tree_records)

# ---------- Action Dataset ----------
action_records = []
for cond in condition_defs:
    if cond.get("name", "").startswith("ACTION."):
        cond_id = cond.get("id")
        cond_name = cond.get("name")
        vals = resolve_constants(cond["id"])
        action_records.append({"ID": cond_id, "Full Path": cond_name, "Value": ";".join(vals)})

df_action = pd.DataFrame(action_records)

# ---------- Policy Targeting Dataset ----------
def collect_linked_action_conditions_with_ids(id_map, root_node_id):
    stack = [root_node_id]
    seen = set()
    results = set()
    while stack:
        nid = stack.pop()
        if not nid or nid in seen:
            continue
        seen.add(nid)
        node = id_map.get(nid, {})
        if not node:
            continue
        if node.get("class") == "ConditionReferenceNode":
            ref_id = node.get("definitionId") or node.get("ref") or node.get("conditionId")
            if ref_id:
                ref_node = id_map.get(ref_id)
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
    if name.startswith("POLICY.TARGETING") or name.startswith("POLICY.TOGGLES"):
        category = "Targeting" if name.startswith("POLICY.TARGETING") else "Toggles"
        linked = collect_linked_action_conditions_with_ids(id_lookup, cond["id"])
        action_ids = ";".join(sorted({lid for lid, lname in linked}))
        action_names = ";".join(sorted({lname for lid, lname in linked}))
        targeting_records.append({
            "ID": cond["id"],
            "Full Path": name,
            "Category": category,
            "Action ID": action_ids,
            "Value": action_names
        })

df_policy_targeting = pd.DataFrame(targeting_records).drop_duplicates()
# ---------- Users Dataset ----------
user_records=[]
for cond in condition_defs:
    nm=cond.get("name","")
    if nm.startswith("USER.AUDIENCE.") or nm.startswith("USER.TENANCY_TYPE."):
        vals=resolve_constants(cond["id"])
        category="Audience" if nm.startswith("USER.AUDIENCE.") else "Tenancy"
        user_records.append({
            "ID": cond["id"],
            "Full Path": nm,
            "Category": category,
            "Value":";".join(vals) if vals else ""
        })
df_users = pd.DataFrame(user_records)

# ---------- Adaptor Dataset ----------
adaptor_records=[]
for cond in condition_defs:
    nm=cond.get("name","")
    if nm.startswith("ADAPTOR."):
        vals=resolve_constants(cond["id"])
        adaptor_records.append({
            "ID": cond["id"],
            "Full Path": nm,
            "Value":";".join(vals) if vals else ""
        })
df_adaptor = pd.DataFrame(adaptor_records)

# ---------- Build Lookups ----------
metadata_nodes = [d for d in data if d.get("class") == "Metadata"]
condition_defs = [d for d in data if d.get("class") == "ConditionDefinition"]
id_lookup = {d["id"]: d for d in data if "id" in d}
cd_nodes = {d["id"]: d for d in data if d.get("class") == "CombinedDecisionNode"}
origin_to_cdnode = {}
for cd in cd_nodes.values():
    if cd.get("originLink"):
        origin_to_cdnode.setdefault(cd["originLink"], []).append(cd)
metadata_lookup = {m["originId"]: m for m in metadata_nodes}

# ---------- Helpers ----------
def collect_condition_def_ids_from_tree(id_map, root_node_id):
    stack = [root_node_id]
    seen = set()
    result = set()
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
    targeting = [c for c in cond_names if c.startswith("POLICY.TARGETING")]
    toggles = [c for c in cond_names if c.startswith("POLICY.TOGGLES")]
    def longest_by_segments(cands):
        return max(cands, key=lambda s: len(s.split("."))) if cands else ""
    if targeting:
        action_first = [c for c in targeting if ".ACTION." in c]
        if action_first:
            return longest_by_segments(action_first)
        return longest_by_segments(targeting)
    if toggles:
        return longest_by_segments(toggles)
    return ""

def find_condition_id_by_name(name):
    for c in condition_defs:
        if c.get("name") == name:
            return c["id"]
    return ""

# ---------- Policy Tree Dataset ----------
policy_tree_records = []
def traverse_policyset_precise_with_condid(origin_id, path_names, path_ids, position):
    node = metadata_lookup.get(origin_id)
    if not node:
        return
    fullpath = " / ".join(path_names + [f"{node['originType']}:{node['name']}"])
    fullpath_id = " / ".join(path_ids + [f"{node['originType']}:{node['originId']}"])
    cond_names_parent = []
    for cd in origin_to_cdnode.get(origin_id, []):
        if cd.get("guardNode"):
            for cid in collect_condition_def_ids_from_tree(id_lookup, cd["guardNode"]):
                cond = next((c for c in condition_defs if c["id"] == cid), None)
                if cond and cond.get("name"):
                    cond_names_parent.append(cond["name"])
    cond_val_parent = pick_single_condition_path(list(set(cond_names_parent)))
    cond_id_parent = find_condition_id_by_name(cond_val_parent)
    policy_tree_records.append({
        "Position": position,
        "ID": node["originId"],
        "Policy FullPath": fullpath,
        "Policy FullPath ID": fullpath_id,
        "Condition Path": cond_val_parent,
        "Condition ID": cond_id_parent
    })
    for cd in origin_to_cdnode.get(origin_id, []):
        for i, inp_id in enumerate(cd.get("inputNodes", []), 1):
            tmn = id_lookup.get(inp_id)
            if not tmn or tmn["class"] != "TargetMatchNode":
                continue
            cond_names_child = []
            for cid in collect_condition_def_ids_from_tree(id_lookup, tmn.get("inputNode")):
                cond = next((c for c in condition_defs if c["id"] == cid), None)
                if cond and cond.get("name"):
                    cond_names_child.append(cond["name"])
            if tmn.get("guardNode"):
                for cid in collect_condition_def_ids_from_tree(id_lookup, tmn["guardNode"]):
                    cond = next((c for c in condition_defs if c["id"] == cid), None)
                    if cond and cond.get("name"):
                        cond_names_child.append(cond["name"])
            cond_val_child = pick_single_condition_path(list(set(cond_names_child)))
            cond_id_child = find_condition_id_by_name(cond_val_child)
            child_id = tmn.get("metadataId")
            if child_id:
                traverse_policyset_precise_with_condid(
                    child_id,
                    path_names + [f"{node['originType']}:{node['name']}"],
                    path_ids + [f"{node['originType']}:{node['originId']}"],
                    f"{position}.{i}"
                )
                policy_tree_records[-1]["Condition Path"] = cond_val_child
                policy_tree_records[-1]["Condition ID"] = cond_id_child

root_id = next((m["originId"] for m in metadata_nodes if m["name"] == "Root"), None)
policy_tree_records.clear()
traverse_policyset_precise_with_condid(root_id, [], [], "1")
df_policy_tree = pd.DataFrame(policy_tree_records)

# ---------- Policy Targeting Dataset ----------
def collect_linked_action_conditions_with_ids(id_map, root_node_id):
    stack = [root_node_id]
    seen = set()
    results = set()
    while stack:
        nid = stack.pop()
        if not nid or nid in seen:
            continue
        seen.add(nid)
        node = id_map.get(nid, {})
        if not node:
            continue
        if node.get("class") == "ConditionReferenceNode":
            ref_id = node.get("definitionId") or node.get("ref") or node.get("conditionId")
            if ref_id:
                ref_node = id_map.get(ref_id)
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
    if name.startswith("POLICY.TARGETING") or name.startswith("POLICY.TOGGLES"):
        category = "Targeting" if name.startswith("POLICY.TARGETING") else "Toggles"
        linked = collect_linked_action_conditions_with_ids(id_lookup, cond["id"])
        action_ids = ";".join(sorted({lid for lid, lname in linked}))
        action_names = ";".join(sorted({lname for lid, lname in linked}))
        targeting_records.append({
            "ID": cond["id"],
            "Full Path": name,
            "Category": category,
            "Action ID": action_ids,
            "Value_action": action_names
        })
df_policy_targeting = pd.DataFrame(targeting_records).drop_duplicates()

# ---------- Action Dataset ----------
def resolve_constants(node_id, seen=None):
    if seen is None: seen=set()
    if not node_id or node_id in seen: return []
    seen.add(node_id)
    node = id_lookup.get(node_id)
    if not node: return []
    results=[]
    cls=node.get("class")
    if cls=="ConstantNode":
        val=node.get("value") or node.get("constant")
        if val is not None:
            results.append(str(val))
    elif cls=="ConditionDefinition":
        if node.get("condition"):
            results.extend(resolve_constants(node["condition"], seen))
    elif cls=="ConditionReferenceNode":
        ref=node.get("definitionId")
        results.extend(resolve_constants(ref, seen))
    elif cls in ("BooleanLogicNode","ComparisonNode","StatementNode"):
        for field in ("inputNode","lhsInputNode","rhsInputNode","guardNode"):
            if node.get(field):
                results.extend(resolve_constants(node[field], seen))
        for field in ("inputNodes","statements"):
            for child in node.get(field,[]):
                results.extend(resolve_constants(child, seen))
    return list(set(results))

action_records = []
for cond in condition_defs:
    if cond.get("name", "").startswith("ACTION."):
        vals = resolve_constants(cond["id"])
        action_records.append({
            "ID": cond["id"],
            "Full Path": cond.get("name",""),
            "Value_action": ";".join(vals) if vals else ""
        })
df_action = pd.DataFrame(action_records)

# ---------- Raw Merge ----------
merged_df = df_policy_tree.merge(
    df_policy_targeting.rename(columns={"ID":"Condition ID"}),
    on="Condition ID", how="left"
)
merged_df = merged_df.merge(
    df_action.rename(columns={"ID":"Action ID"}),
    on="Action ID", how="left",
    suffixes=("_targeting","_action")
)

# ---------- Clean Value_action ----------
# Detect which column holds the Action constants
if "Value_action_y" in merged_df.columns:
    action_col = "Value_action_y"
elif "Value_action_action" in merged_df.columns:
    action_col = "Value_action_action"
else:
    action_col = "Value_action"

merged_df["Value_action"] = merged_df[action_col]
merged_df.drop(columns=["Value_action_x","Value_action_y","Value_action_action"], inplace=True, errors="ignore")

# ---------- Prefix-Aware Propagation ----------
def pos_tuple(p):
    return tuple(int(x) for x in str(p).split("."))

merged_df["_pos_tuple"] = merged_df["Position"].astype(str).map(pos_tuple)
merged_df.sort_values("_pos_tuple", inplace=True, kind="mergesort")
merged_df.reset_index(drop=True, inplace=True)

def is_prefix(ancestor, descendant):
    return len(ancestor) <= len(descendant) and ancestor == descendant[:len(ancestor)]

stack = []
for idx, row in merged_df.iterrows():
    segs = row["_pos_tuple"]

    # Trim stack until top is ancestor of current
    while stack and not is_prefix(stack[-1][0], segs):
        stack.pop()

    val = row.get("Value_action")
    seed = str(val).strip() if pd.notna(val) else ""   # <-- safe conversion
    has_cond = bool(str(row.get("Condition ID") or "").strip())

    if has_cond and seed:   # new seeding point
        stack.append((segs, seed))
        current = seed
    else:
        current = stack[-1][1] if stack else ""

    if not seed:
        merged_df.at[idx, "Value_action"] = current

merged_df.drop(columns="_pos_tuple", inplace=True)

# ---------- Build Toggles Dataset ----------
files = {
    "SIT1": "Toggle V1 - SIT1.json",
    "SIT4": "Toggle V1 - SIT4.json",
    "SIT5": "Toggle V1 - SIT5.json",
}

all_results = []

for env, file_path in files.items():
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Services
    for service in data.get("toggles", {}).get("arbitrary", {}).get("services", []):
        for version in service.get("versions", []):
            if not version.get("isEnabled", True):
                all_results.append({
                    "Category": "Services",
                    "Env": env,
                    "Path": f"*{service['name']}*{version['name']}*",
                    "Action": "",
                    "Resource": ""
                })

    # Check Permissions
    for cp in data.get("toggles", {}).get("checkPermissions", []):
        action_val = cp.get("action", "")
        for tenant in cp.get("tenants", []):
            if not tenant.get("isEnabled", True):
                all_results.append({
                    "Category": "Check Permission",
                    "Env": env,
                    "Path": f"*{tenant['name']}*",
                    "Action": action_val,
                    "Resource": ""
                })
            for version in tenant.get("versions", []):
                if not version.get("isEnabled", True):
                    all_results.append({
                        "Category": "Check Permission",
                        "Env": env,
                        "Path": f"*{tenant['name']}*{version['name']}*",
                        "Action": action_val,
                        "Resource": ""
                    })

    # Get Permissions
    for gp in data.get("toggles", {}).get("getPermissions", []):
        for resource in gp.get("resources", []):
            resource_type = resource.get("resourceType", "")
            resource_val = resource.get("name", "")
            if not resource.get("isEnabled", True):
                all_results.append({
                    "Category": "Get Permission",
                    "Env": env,
                    "Path": f"*{resource_val}*",
                    "Action": "",
                    "Resource": resource_type
                })
            for version in resource.get("versions", []):
                if not version.get("isEnabled", True):
                    all_results.append({
                        "Category": "Get Permission",
                        "Env": env,
                        "Path": f"*{resource_val}*{version['name']}*",
                        "Action": "",
                        "Resource": resource_type
                    })

df_toggles = pd.DataFrame(all_results)
df_toggles.head(20)

# --- Integrate toggles ON/OFF flags into merged_df ---

# Filter only Check Permission with valid Action
toggles_check = df_toggles[
    (df_toggles["Category"] == "Check Permission") &
    (df_toggles["Action"].notna()) &
    (df_toggles["Action"].str.strip() != "")
]

# Add environment columns, default to ON
envs = toggles_check["Env"].unique()
for env in envs:
    merged_df[env] = "ON"

# Mark OFF where Action + Policy FullPath matches
for _, trow in toggles_check.iterrows():
    env = trow["Env"]
    action_val = trow["Action"]
    path_pattern = trow["Path"].strip("*")
    mask = (
        merged_df["Value_action"].eq(action_val) &
        merged_df["Policy FullPath"].str.contains(path_pattern, case=False, na=False)
    )
    merged_df.loc[mask, env] = "OFF"
# Also add to Excel
# ---------- Save All Sheets to Excel ----------
with pd.ExcelWriter("All_Datasets.xlsx", engine="xlsxwriter") as writer:
    df_policy_tree.to_excel(writer, sheet_name="Policy_Tree", index=False)
    df_action.to_excel(writer, sheet_name="Action", index=False)
    df_policy_targeting.to_excel(writer, sheet_name="Policy_Targeting", index=False)
    df_users.to_excel(writer, sheet_name="Users", index=False)
    df_adaptor.to_excel(writer, sheet_name="Adaptor", index=False)
    merged_df.to_excel(writer, sheet_name="Merged", index=False)
    df_toggles.to_excel(writer, sheet_name="Toggles", index=False)

print("✅ Added Merged dataset to All_Datasets.xlsx")
