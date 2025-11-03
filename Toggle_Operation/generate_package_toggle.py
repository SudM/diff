#!/usr/bin/env python3
"""
generate_package_toggle.py
----------------------------------------------
This script processes a Policy Tree Excel sheet and generates a hierarchical JSON
representation of feature toggles used in access-control policies.

"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
from collections import defaultdict

# ---------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------
LOG_FILE = "toggle_operation.log"
logging.basicConfig(
    filename=LOG_FILE,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------
def safe_str(val) -> str:
    """Safely convert value to lowercase stripped string."""
    return str(val).strip().lower() if pd.notna(val) else ""


# ---------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------
def load_data(path: Path) -> pd.DataFrame:
    """
    Load Excel file and extract relevant policy tree information.
    
    Args:
        path (Path): Path to Excel file.

    Returns:
        pd.DataFrame: Cleaned DataFrame with required columns.
    """
    logging.info(f"Loading Excel data from {path}")
    try:
        df = pd.read_excel(path, sheet_name="Policy_Tree", dtype=str).fillna("")
    except Exception as e:
        logging.error(f"Error loading Excel file: {e}")
        raise

    df.columns = [c.strip() for c in df.columns]

    # Required columns validation
    required = ["ID", "Action", "Value_action", "Type", "Toggle Type",
                "Toggle Name", "Version", "Status", "Policy FullPath"]
    cols = {k: next((c for c in df.columns if k.lower() in c.lower()), None) for k in required}

    if missing := [k for k, v in cols.items() if v is None]:
        msg = f"Missing required columns: {missing}"
        logging.error(msg)
        raise ValueError(msg)

    # Filter only checkpermission actions
    df = df[df[cols["Action"]].apply(safe_str) == "checkpermission"].copy()
    df = df.assign(
        clean_action=df[cols["Action"]].apply(safe_str),
        clean_value=df[cols["Value_action"]].apply(safe_str),
        clean_status=df[cols["Status"]].apply(safe_str),
        clean_category=df[cols["Type"]].apply(safe_str),
        clean_type=df[cols["Toggle Type"]].apply(safe_str),
        clean_name=df[cols["Toggle Name"]].apply(safe_str),
        clean_ver=df[cols["Version"]].apply(safe_str),
        clean_path=df[cols["Policy FullPath"]].apply(lambda x: x.strip() if pd.notna(x) else "")
    )
    df.attrs["cols"] = cols
    logging.info(f"Loaded {len(df)} filtered rows with checkpermission actions")
    return df


# ---------------------------------------------------------------------
# Grouping by Action
# ---------------------------------------------------------------------
def get_action_groups(df: pd.DataFrame, cols: Dict) -> List[pd.DataFrame]:
    """
    Split dataframe into subgroups by Value_action (5 path segments).
    
    Args:
        df (pd.DataFrame): Cleaned policy DataFrame.
        cols (Dict): Column mapping.

    Returns:
        List[pd.DataFrame]: List of grouped DataFrames.
    """
    logging.info("Grouping data by Value_action...")
    groups = []
    action_rows = df[df["clean_value"].ne("")]
    for _, act_row in action_rows.iterrows():
        prefix = " / ".join(act_row["clean_path"].split(" / ")[:5])
        group = df[df["clean_path"].str.startswith(prefix)].copy()
        group.attrs["cols"] = cols
        group.attrs["action_id"] = act_row[cols["ID"]]
        group.attrs["action_value"] = act_row["clean_value"]
        groups.append(group)

    logging.info(f"Created {len(groups)} action-based groups")
    return groups


# ---------------------------------------------------------------------
# Tree Building
# ---------------------------------------------------------------------
def build_tree_from_paths(root_obj: Dict, node_map: Dict, path_df: pd.DataFrame,
                          structural_types: set, cols: Dict):
    """
    Construct nested hierarchy from path-based relationships.

    Args:
        root_obj (Dict): Root JSON structure being built.
        node_map (Dict): Mapping of paths → node objects.
        path_df (pd.DataFrame): Policy path dataframe.
        structural_types (set): Structural node types (e.g. PolicySet, Folder).
        cols (Dict): Column mapping.
    """
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
            tt = row["clean_type"]
            tn = row["clean_name"].upper()
            if tt not in structural_types:
                continue

            if tt not in current:
                current[tt] = []
            parent_node = next((n for n in current[tt] if n["name"] == tn), None)
            if not parent_node:
                parent_node = {"id": row[cols["ID"]], "name": tn, "versions": []}
                current[tt].append(parent_node)
            current = parent_node

        row = path_df.loc[path]
        tt = row["clean_type"]
        if tt in structural_types:
            if tt not in current:
                current[tt] = []
            current[tt].append(node)


# ---------------------------------------------------------------------
# Build Tree
# ---------------------------------------------------------------------
def build_tree(group: pd.DataFrame, debug_full_tree: bool = False) -> Any:
    """
    Build a hierarchical tree for a single Value_action group.

    Args:
        group (pd.DataFrame): Grouped subset of policy rows.
        debug_full_tree (bool): If True, return complete tree for debugging.

    Returns:
        Dict | None: Constructed JSON-ready action tree.
    """
    cols = group.attrs["cols"]
    act_id = group.attrs["action_id"]
    act_value = group.attrs["action_value"]

    version_rows = group[group["clean_ver"].ne("")]
    toggle_rows = group[group["clean_type"].ne("") & group["clean_type"].notna()]

    if toggle_rows.empty:
        logging.warning(f"No toggle rows found for action {act_value}")
        return None

    structural_types = set(toggle_rows["clean_type"].unique()) - {"version", "status", ""}
    path_df = group.set_index("clean_path", drop=False)
    node_map = {}

    # Build initial node map
    for _, r in toggle_rows.iterrows():
        p = r["clean_path"]
        node_map[p] = {
            "id": r[cols["ID"]],
            "name": r["clean_name"].upper(),
            "isEnabled": True,
            "versions": []
        }

    # Group versions by toggle path
    version_by_toggle: Dict[str, List[pd.Series]] = defaultdict(list)
    for _, vr in version_rows.iterrows():
        p = vr["clean_path"]
        toggle_paths = [tp for tp in node_map.keys() if p.startswith(tp + " / ") or p == tp]
        if toggle_paths:
            target_path = max(toggle_paths, key=len)
            version_by_toggle[target_path].append(vr)

    # Attach versions
    for target_path, vrs in version_by_toggle.items():
        node = node_map[target_path]
        for vr in vrs:
            node["versions"].append({
                "id": vr[cols["ID"]],
                "name": vr["clean_ver"].strip().upper(),
                "isEnabled": vr["clean_status"].strip().lower() != "off",
                "Category": vr["clean_category"].strip().lower()
            })

    # Build tree structure
    root_obj = {}
    build_tree_from_paths(root_obj, node_map, path_df, structural_types, cols)
    if not root_obj:
        logging.warning(f"Empty root object for {act_value}")
        return None

    # Compute enablement recursively
    def compute(obj: Dict) -> bool:
        vers = obj.get("versions", [])
        children = [obj[k] for k in obj if k not in {"id", "name", "versions", "isEnabled"} and isinstance(obj[k], list)]
        has_vers = len(vers) > 0
        all_off = has_vers and all(v["isEnabled"] is False for v in vers)
        child_off = all(all(not compute(c) for c in ch) for ch in children) if children else False
        enabled = not (all_off and child_off) if has_vers else not child_off
        obj["isEnabled"] = enabled
        return enabled

    def apply(obj: Dict):
        for k in obj:
            if isinstance(obj[k], list) and k != "versions":
                for item in obj[k]:
                    if isinstance(item, dict):
                        apply(item)
        compute(obj)

    apply(root_obj)

    # Ensure 'isEnabled' comes after 'name'
    def reorder(obj: Dict):
        if "name" in obj and "isEnabled" in obj:
            ordered = {"id": obj.get("id"), "name": obj["name"], "isEnabled": obj["isEnabled"]}
            for k, v in obj.items():
                if k not in {"id", "name", "isEnabled"}:
                    ordered[k] = v
            obj.clear()
            obj.update(ordered)
        for k, v in obj.items():
            if isinstance(v, list):
                for child in v:
                    if isinstance(child, dict):
                        reorder(child)

    reorder(root_obj)

    action_obj = {
        "id": act_id,
        "action": act_value,
        "isEnabled": root_obj.get("isEnabled", True)
    }
    action_obj.update(root_obj)
    logging.info(f"Tree built successfully for {act_value}")
    return action_obj


# ---------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------
def main(input_xlsx="input.xlsx", output_json="package_toggle.json", full_tree=False):
    """
    Main entry point. Loads Excel, groups by action, builds trees, and exports JSON.

    Args:
        input_xlsx (str): Path to Excel file.
        package_toggle (str): Output JSON filename.
        full_tree (bool): Include full hierarchy for debugging.
    """
    logging.info(f"Starting toggle tree generation for {input_xlsx}")
    try:
        df = load_data(Path(input_xlsx))
        cols = df.attrs["cols"]
        result = {
            "schemaVersion": "1.0.0",
            "strategy": "blacklist",
            "toggles": {"checkPermissions": []}
        }

        for g in get_action_groups(df, cols):
            tree = build_tree(g, debug_full_tree=full_tree)
            if tree:
                result["toggles"]["checkPermissions"].append(tree)

        Path(output_json).write_text(json.dumps(result, indent=2))
        logging.info(f"JSON successfully written to {output_json}")
        print(f"Generated: {output_json}")
    except Exception as e:
        logging.exception(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    import sys
    full = "--full" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--full"]
    main(*args if len(args) >= 2 else ("input.xlsx", "full_tree.json" if full else "package_toggle.json"), full_tree=full)
