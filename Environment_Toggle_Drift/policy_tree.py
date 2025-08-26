import pandas as pd
from .utils import logger
from .merge import collect_condition_def_ids_from_tree, pick_single_condition_path

def build_policy_tree(data: list) -> pd.DataFrame:
    """
    Traverse metadata and build policy tree dataset.
    Root is determined from:
    1. 'Package' or 'DeploymentPackage' header (preferred)
    2. Metadata with name == 'NAB Policies' (fallback)
    """
    metadata_nodes = [d for d in data if d.get("class") == "Metadata"]
    condition_defs = [d for d in data if d.get("class") == "ConditionDefinition"]
    id_lookup = {d["id"]: d for d in data if "id" in d}
    cd_nodes = {d["id"]: d for d in data if d.get("class") == "CombinedDecisionNode"}

    origin_to_cdnode = {}
    for cd in cd_nodes.values():
        if cd.get("originLink"):
            origin_to_cdnode.setdefault(cd["originLink"], []).append(cd)

    metadata_lookup = {m["originId"]: m for m in metadata_nodes}
    policy_tree_records = []

    def traverse(origin_id, path_names, path_ids, position):
        node = metadata_lookup.get(origin_id)
        if not node:
            return
        fullpath_parts = path_names + [f"{node['originType']}:{node['name']}"]
        fullpath_id_parts = path_ids + [f"{node['originType']}:{node['originId']}"]

        cond_names_parent = []
        for cd in origin_to_cdnode.get(origin_id, []):
            if cd.get("guardNode"):
                for cid in collect_condition_def_ids_from_tree(id_lookup, cd["guardNode"]):
                    cond = next((c for c in condition_defs if c["id"] == cid), None)
                    if cond and cond.get("name"):
                        cond_names_parent.append(cond["name"])
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
                if not tmn or tmn["class"] != "TargetMatchNode":
                    continue
                child_id = tmn.get("metadataId")
                if child_id:
                    traverse(child_id, fullpath_parts, fullpath_id_parts, f"{position}.{i}")

    # ---------------------------
    # Determine root
    # ---------------------------
    root_id, root_name = None, None

    # Case 1: look for Package header
    package_meta = next((m for m in data if m.get("class") in ("Package", "DeploymentPackage")), None)
    if package_meta:
        root_id = package_meta.get("rootEntityId")
        root_name = package_meta.get("rootEntityName")
        logger.info(f"Root from {package_meta['class']}: {root_name} ({root_id})")

    # Case 2: fallback to Metadata named "NAB Policies"
    if not root_id:
        fallback = next((m for m in metadata_nodes if m.get("name") == "NAB Policies"), None)
        if fallback:
            root_id = fallback.get("originId")
            root_name = "NAB Policies"
            logger.warning("Using fallback root from Metadata node with name='NAB Policies'")
        else:
            logger.error("No rootEntityId or Root metadata found. Policy tree cannot be built.")
            return pd.DataFrame(columns=["Position", "ID", "Policy FullPath", "Policy FullPath ID", "Condition Path", "Condition ID"])

    traverse(root_id, [], [], "1")
    logger.info(f"Policy tree built with {len(policy_tree_records)} records")
    return pd.DataFrame(policy_tree_records)
