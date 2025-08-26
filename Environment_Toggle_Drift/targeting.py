import pandas as pd
from .utils import logger

def extract_targeting(condition_defs, id_lookup) -> pd.DataFrame:
    """Extract POLICY.TARGETING.* and POLICY.TOGGLES.* conditions and link to actions."""
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
        if name.startswith("POLICY.TARGETING") or name.startswith("POLICY.TOGGLES"):
            category = "Targeting" if name.startswith("POLICY.TARGETING") else "Toggles"
            linked = collect_linked_action_conditions_with_ids(cond["id"])
            action_ids = ";".join(sorted({lid for lid, lname in linked}))
            action_names = ";".join(sorted({lname for lid, lname in linked}))
            targeting_records.append({
                "ID": cond["id"],
                "Full Path": name,
                "Category": category,
                "Action ID": action_ids,
                "Value_action": action_names
            })
    df_targeting = pd.DataFrame(targeting_records).drop_duplicates()
    logger.info(f"Extracted {len(df_targeting)} targeting/toggle records")
    return df_targeting
