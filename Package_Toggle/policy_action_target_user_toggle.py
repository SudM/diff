import json
import pandas as pd

# ---------- Load Deployment Package ----------
with open("test.deploymentpackage", "r") as f:
    data = json.load(f)

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

# ---------- Helpers ----------
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
        if val is not None: results.append(str(val))
    elif cls=="ConditionDefinition" and node.get("condition"):
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

def collect_condition_def_ids_from_tree(id_map, root_node_id):
    stack = [root_node_id]; seen=set(); result=set()
    while stack:
        nid = stack.pop()
        if not nid or nid in seen: continue
        seen.add(nid)
        node = id_map.get(nid, {})
        if not node: continue
        if node.get("class") == "ConditionReferenceNode":
            rid = node.get("definitionId") or node.get("ref") or node.get("conditionId")
            if rid: result.add(rid)
        for key in ("inputNode","guardNode","condition","lhsInputNode","rhsInputNode"):
            val = node.get(key)
            if isinstance(val,str): stack.append(val)
        if isinstance(node.get("inputNodes"), list): stack.extend(node.get("inputNodes"))
    return result

def pick_single_condition_path(cond_names):
    targeting = [c for c in cond_names if c.startswith("POLICY.TARGETING")]
    toggles = [c for c in cond_names if c.startswith("POLICY.TOGGLES")]
    def longest(cands): return max(cands, key=lambda s: len(s.split("."))) if cands else ""
    if targeting:
        action_first = [c for c in targeting if ".ACTION." in c]
        return longest(action_first) if action_first else longest(targeting)
    if toggles: return longest(toggles)
    return ""

# ---------- Policy Tree Traversal (patched to always append names) ----------
policy_tree_records = []

def traverse_policyset_always_append(origin_id, path_names, path_ids, position):
    node = metadata_lookup.get(origin_id)
    if not node: return

    fullpath_parts = path_names + [f"{node['originType']}:{node['name']}"]
    fullpath_id_parts = path_ids + [f"{node['originType']}:{node['originId']}"]

    cond_names_parent = []
    for cd in origin_to_cdnode.get(origin_id, []):
        if cd.get("guardNode"):
            for cid in collect_condition_def_ids_from_tree(id_lookup, cd["guardNode"]):
                cond = next((c for c in condition_defs if c["id"] == cid), None)
                if cond and cond.get("name"): cond_names_parent.append(cond["name"])
    cond_val_parent = pick_single_condition_path(list(set(cond_names_parent)))
    cond_id_parent = next((c["id"] for c in condition_defs if c.get("name") == cond_val_parent), "")

    policy_tree_records.append({
        "Position": position,
        "ID": node["originId"],
        "Policy FullPath": " / ".join(fullpath_parts),
        "Policy FullPath ID": " / ".join(fullpath_id_parts),
        "Condition Path": cond_val_parent,
        "Condition ID": cond_id_parent
    })

    for cd in origin_to_cdnode.get(origin_id, []):
        for i, inp_id in enumerate(cd.get("inputNodes", []), 1):
            tmn = id_lookup.get(inp_id)
            if not tmn or tmn["class"] != "TargetMatchNode": continue
            cond_names_child = []
            for cid in collect_condition_def_ids_from_tree(id_lookup, tmn.get("inputNode")):
                cond = next((c for c in condition_defs if c["id"] == cid), None)
                if cond and cond.get("name"): cond_names_child.append(cond["name"])
            if tmn.get("guardNode"):
                for cid in collect_condition_def_ids_from_tree(id_lookup, tmn["guardNode"]):
                    cond = next((c for c in condition_defs if c["id"] == cid), None)
                    if cond and cond.get("name"): cond_names_child.append(cond["name"])
            cond_val_child = pick_single_condition_path(list(set(cond_names_child)))
            cond_id_child = next((c["id"] for c in condition_defs if c.get("name") == cond_val_child), "")
            child_id = tmn.get("metadataId")
            if child_id:
                traverse_policyset_always_append(child_id, fullpath_parts, fullpath_id_parts, f"{position}.{i}")
                policy_tree_records[-1]["Condition Path"] = cond_val_child
                policy_tree_records[-1]["Condition ID"] = cond_id_child

root_id = next((m["originId"] for m in metadata_nodes if m["name"] == "Root"), None)
policy_tree_records.clear()
traverse_policyset_always_append(root_id, [], [], "1")
df_policy_tree = pd.DataFrame(policy_tree_records)

# ---------- Action Dataset ----------
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

# ---------- Policy Targeting Dataset ----------
def collect_linked_action_conditions_with_ids(id_map, root_node_id):
    stack=[root_node_id]; seen=set(); results=set()
    while stack:
        nid=stack.pop()
        if not nid or nid in seen: continue
        seen.add(nid); node=id_map.get(nid,{})
        if not node: continue
        if node.get("class")=="ConditionReferenceNode":
            ref_id=node.get("definitionId") or node.get("ref") or node.get("conditionId")
            if ref_id:
                ref_node=id_map.get(ref_id)
                if ref_node and ref_node.get("class")=="ConditionDefinition" and ref_node.get("name","").startswith("ACTION."):
                    results.add((ref_node["id"], ref_node["name"]))
        for key in ("inputNode","guardNode","condition","lhsInputNode","rhsInputNode"):
            val=node.get(key)
            if isinstance(val,str): stack.append(val)
        if isinstance(node.get("inputNodes"),list): stack.extend(node.get("inputNodes"))
    return results

targeting_records=[]
for cond in condition_defs:
    name=cond.get("name","")
    if name.startswith("POLICY.TARGETING") or name.startswith("POLICY.TOGGLES"):
        category="Targeting" if name.startswith("POLICY.TARGETING") else "Toggles"
        linked=collect_linked_action_conditions_with_ids(id_lookup,cond["id"])
        action_ids=";".join(sorted({lid for lid,lname in linked}))
        action_names=";".join(sorted({lname for lid,lname in linked}))
        targeting_records.append({
            "ID": cond["id"],
            "Full Path": name,
            "Category": category,
            "Action ID": action_ids,
            "Value_action": action_names
        })
df_policy_targeting = pd.DataFrame(targeting_records).drop_duplicates()

# ---------- Merge ----------
merged_df = df_policy_tree.merge(
    df_policy_targeting.rename(columns={"ID":"Condition ID"}), on="Condition ID", how="left"
)
merged_df = merged_df.merge(
    df_action.rename(columns={"ID":"Action ID"}), on="Action ID", how="left",
    suffixes=("_targeting","_action")
)
if "Value_action_y" in merged_df.columns: action_col="Value_action_y"
elif "Value_action_action" in merged_df.columns: action_col="Value_action_action"
else: action_col="Value_action"
merged_df["Value_action"]=merged_df[action_col]
merged_df.drop(columns=["Value_action_x","Value_action_y","Value_action_action"], inplace=True, errors="ignore")

# ---------- Condition-ID Based Propagation ----------
def pos_tuple(p):
    return tuple(int(x) for x in str(p).split("."))

# Ensure Value_action is string
merged_df["Value_action"] = merged_df["Value_action"].fillna("").astype(str).str.strip()

# Sort by position hierarchy
merged_df["_pos_tuple"] = merged_df["Position"].astype(str).map(pos_tuple)
merged_df.sort_values("_pos_tuple", inplace=True, kind="mergesort")
merged_df.reset_index(drop=True, inplace=True)

stack = []  # (pos_tuple, action_value)

def is_prefix(anc, desc):
    return len(anc) < len(desc) and anc == desc[:len(anc)]

for idx, row in merged_df.iterrows():
    segs = row["_pos_tuple"]

    # Pop until top of stack is ancestor
    while stack and not is_prefix(stack[-1][0], segs):
        stack.pop()

    row_val = str(row.get("Value_action") or "").strip()
    has_cond = pd.notna(row.get("Condition ID")) and str(row.get("Condition ID")).strip() != ""

    # ✅ seed only if Condition ID exists AND Value_action is non-empty
    if has_cond and row_val:
        stack.append((segs, row_val))
        current = row_val
    else:
        current = stack[-1][1] if stack else ""
        if current and not row_val:
            merged_df.at[idx, "Value_action"] = current

merged_df.drop(columns="_pos_tuple", inplace=True)

# ---------- Toggle Integration ----------
toggle_files={"SIT1":"Toggle V1 - SIT1.json","SIT4":"Toggle V1 - SIT4.json","SIT5":"Toggle V1 - SIT5.json"}
all_results=[]
for env,file_path in toggle_files.items():
    with open(file_path,"r",encoding="utf-8") as f: tdata=json.load(f)
    for cp in tdata.get("toggles",{}).get("checkPermissions",[]):
        action_val=cp.get("action","")
        for tenant in cp.get("tenants",[]):
            if not tenant.get("isEnabled",True):
                all_results.append({"Category":"Check Permission","Env":env,"Path":f"*{tenant['name']}*","Action":action_val,"Resource":""})
            for version in tenant.get("versions",[]):
                if not version.get("isEnabled",True):
                    all_results.append({"Category":"Check Permission","Env":env,"Path":f"*{tenant['name']}*{version['name']}*","Action":action_val,"Resource":""})
df_toggles=pd.DataFrame(all_results)

for env in df_toggles["Env"].unique():
    merged_df[env]="ON"
for _,trow in df_toggles.iterrows():
    env=trow["Env"]; action_val=trow["Action"]; path_pattern=trow["Path"].strip("*"); resource=trow["Resource"]
    fragments=[frag for frag in path_pattern.split("*") if frag]
    mask=pd.Series([True]*len(merged_df))
    if action_val: mask &= merged_df["Value_action"].eq(action_val)
    for frag in fragments: mask &= merged_df["Policy FullPath"].str.contains(frag,case=False,na=False)
    if resource: mask &= merged_df["Policy FullPath"].str.contains(resource,case=False,na=False)
    merged_df.loc[mask,env]="OFF"

# ---------- Save ----------
with pd.ExcelWriter("All_Datasets_Patched.xlsx",engine="xlsxwriter") as writer:
    df_policy_tree.to_excel(writer,sheet_name="Policy_Tree",index=False)
    df_action.to_excel(writer,sheet_name="Action",index=False)
    df_policy_targeting.to_excel(writer,sheet_name="Policy_Targeting",index=False)
    merged_df.to_excel(writer,sheet_name="Merged",index=False)
    df_toggles.to_excel(writer,sheet_name="Toggles",index=False)

print("✅ All datasets exported to All_Datasets_Patched.xlsx")

def export_environment_drift_html(df, output_file="Environment Drift.html"):
    df_html = df.copy()
    df_html = df_html.rename(columns={"Value_action": "Action"})

    # Base & env columns
    base_cols = ["Position", "ID", "Policy FullPath", "Action"]
    env_cols = [c for c in df_html.columns if c.upper().startswith(("SIT","UAT","PROD"))]

    df_html = df_html[base_cols + env_cols]

    # Build unique dropdown values for Action + env columns
    dropdown_values = {}
    for col in ["Action"] + env_cols:
        vals = sorted(set(str(v).strip() for v in df_html[col].dropna().unique()))
        vals = [v for v in vals if v]  # remove blanks
        dropdown_values[col] = vals

    rows = []
    for _, row in df_html.iterrows():
        vals = [str(row[c]).strip() for c in env_cols if pd.notna(row[c])]
        drift = len(set(vals)) > 1
        all_off = all(v.upper() == "OFF" for v in vals if v)
        is_root = str(row["Policy FullPath"]).strip() == "PolicySet:Root"

        row_style = ""
        if not is_root:
            if all_off:
                row_style = ' style="background-color:#FFCCCC"'  # light red
            elif drift:
                row_style = ' style="background-color:#FFBF00"'  # amber

        new_row = "".join([f"<td>{row[col]}</td>" for col in base_cols + env_cols])
        rows.append(f"<tr{row_style}>{new_row}</tr>")

    # Build table header with dropdowns
    header_cells = []
    for col in base_cols + env_cols:
        if col == "Action" or col in env_cols:
            options = "".join([f"<option value='{val}'>{val}</option>" for val in dropdown_values.get(col, [])])
            dropdown = f"<br><select onchange=\"colFilter(this, {col!r})\"><option value=''>All</option>{options}</select>"
            header_cells.append(f"<th>{col}{dropdown}</th>")
        else:
            header_cells.append(f"<th>{col}</th>")
    header_html = "".join(header_cells)

    # Build HTML with dropdown JS filter
    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <title>Environment Drift</title>
      <style>
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ccc; padding: 6px; text-align: left; vertical-align: top; }}
        th {{ background-color: #f2f2f2; position: sticky; top: 0; z-index: 2; }}
        td {{ white-space: pre-wrap; word-wrap: break-word; }}
      </style>
      <script>
        function colFilter(selectElem, colName) {{
          var filter = selectElem.value.toLowerCase();
          var table = document.getElementById("driftTable");
          var colIndex = Array.from(table.rows[0].cells).findIndex(c => c.innerText.startsWith(colName));
          var trs = table.getElementsByTagName("tr");
          for (var i = 1; i < trs.length; i++) {{
            var td = trs[i].getElementsByTagName("td")[colIndex];
            if (td) {{
              if (filter === "" || td.innerText.toLowerCase() === filter) {{
                trs[i].style.display = "";
              }} else {{
                trs[i].style.display = "none";
              }}
            }}
          }}
        }}
      </script>
    </head>
    <body>
      <h2>Environment Drift Report</h2>
      <table id="driftTable">
        <tr>{header_html}</tr>
        {''.join(rows)}
      </table>
    </body>
    </html>
    """

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ Environment drift HTML exported to {output_file}")

# Call HTML export right after everything is built
export_environment_drift_html(merged_df, output_file="Environment Drift.html")
