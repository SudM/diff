import pandas as pd
from .utils import logger

# -------------------------
# Helpers
# -------------------------
def resolve_constants(node_id, id_lookup, attr_defs, seen=None):
    """Recursively resolve constants from node references."""
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


def collect_condition_def_ids_from_tree(id_map, root_node_id):
    """Collect all ConditionDefinition IDs from a tree rooted at root_node_id."""
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
    """Select the most relevant condition path string from candidate names."""
    targeting = [c for c in cond_names if c.startswith("POLICY.TARGETING")]
    toggles = [c for c in cond_names if c.startswith("POLICY.TOGGLES")]

    def longest(cands): return max(cands, key=lambda s: len(s.split("."))) if cands else ""

    if targeting:
        action_first = [c for c in targeting if ".ACTION." in c]
        return longest(action_first) if action_first else longest(targeting)
    if toggles:
        return longest(toggles)
    return ""


# -------------------------
# Merge + Propagation
# -------------------------
def merge_datasets(df_policy_tree, df_action, df_policy_targeting) -> pd.DataFrame:
    """Merge policy tree, action, and targeting datasets,
    propagate condition values, then filter for entitlement check nodes only."""
    try:
        merged_df = df_policy_tree.merge(
            df_policy_targeting.rename(columns={"ID": "Condition ID"}),
            on="Condition ID", how="left"
        )
        merged_df = merged_df.merge(
            df_action.rename(columns={"ID": "Action ID"}),
            on="Action ID", how="left", suffixes=("_targeting", "_action")
        )
    except KeyError:
        logger.error("Merge failed: missing expected keys")
        raise

    # Normalize Value_action
    if "Value_action_y" in merged_df.columns:
        action_col = "Value_action_y"
    elif "Value_action_action" in merged_df.columns:
        action_col = "Value_action_action"
    else:
        action_col = "Value_action"

    merged_df["Value_action"] = (
        merged_df[action_col].fillna("").astype(str).str.strip()
    )
    merged_df.drop(
        columns=["Value_action_x", "Value_action_y", "Value_action_action"],
        inplace=True,
        errors="ignore"
    )

    logger.info(f"Merged dataset created with {len(merged_df)} rows")

    # ---------------------------
    # Step 1: Propagation first
    # ---------------------------
    merged_df = propagate_conditions(merged_df)

    # ---------------------------
    # Step 2: Filter for entitlement check nodes only
    # ---------------------------
    mask_entitlement = merged_df["Policy FullPath"].str.endswith("Entitlement Check", na=False)
    entitlement_df = merged_df[mask_entitlement].copy()

    if entitlement_df.empty:
        logger.warning("No entitlement check nodes found. Returning full merged dataset.")
        return merged_df

    logger.info(f"Filtered merged dataset to {len(entitlement_df)} entitlement check rows (children excluded).")
    return entitlement_df


def propagate_conditions(merged_df: pd.DataFrame) -> pd.DataFrame:
    """Propagate Value_action down the policy tree hierarchy."""
    from .utils import pos_tuple

    merged_df["_pos_tuple"] = merged_df["Position"].map(pos_tuple)
    merged_df.sort_values("_pos_tuple", inplace=True)
    merged_df.reset_index(drop=True, inplace=True)

    stack = []
    for idx, row in merged_df.iterrows():
        segs = row["_pos_tuple"]
        while stack and not (len(stack[-1][0]) < len(segs) and stack[-1][0] == segs[:len(stack[-1][0])]):
            stack.pop()
        row_val = row["Value_action"].strip()
        if row.get("Condition ID") and row_val:
            stack.append((segs, row_val))
        elif stack and not row_val:
            merged_df.at[idx, "Value_action"] = stack[-1][1]
    merged_df.drop(columns="_pos_tuple", inplace=True)

    logger.info("Condition values propagated down the policy tree")
    return merged_df

def filter_to_entitlement_checks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep ONLY rows where:
      - an earlier segment is exactly 'Check Permissions'
      - the LAST segment starts with 'Entitlement Check' (case-insensitive).
    This excludes children like '/.../Entitlement Check.../...'.
    """
    if df.empty or "Policy FullPath" not in df.columns:
        logger.warning("filter_to_entitlement_checks: input empty or missing 'Policy FullPath'; returning as-is.")
        return df.copy()

    segs_series = df["Policy FullPath"].astype(str).str.split(" / ")

    def segment_label(seg: str) -> str:
        return seg.split(":", 1)[1].strip() if ":" in seg else seg.strip()

    # Check for "Check Permissions" in any earlier segment
    has_check_permissions = segs_series.apply(
        lambda parts: any(segment_label(p).lower() == "check permissions" 
                          for p in parts[:-1]) if len(parts) > 1 else False
    )

    # Last segment must start with "Entitlement Check"
    last_labels = segs_series.apply(lambda parts: segment_label(parts[-1]) if parts else "")
    last_is_entitlement = last_labels.str.lower().str.startswith("entitlement check")

    mask = has_check_permissions & last_is_entitlement
    filtered = df.loc[mask].copy()

    logger.info(f"Entitlement filter applied: kept {len(filtered)} of {len(df)} rows.")
    return filtered

