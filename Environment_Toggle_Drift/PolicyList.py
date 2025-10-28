import json
import pandas as pd
import sys
from pathlib import Path

def build_policy_tree(data: list) -> pd.DataFrame:
    metadata_nodes = [d for d in data if d.get("class") == "Metadata"]
    id_lookup = {d["id"]: d for d in data if "id" in d}
    cd_nodes = {d["id"]: d for d in data if d.get("class") == "CombinedDecisionNode"}
    origin_to_cdnode = {}
    for cd in cd_nodes.values():
        if cd.get("originLink"):
            origin_to_cdnode.setdefault(cd["originLink"], []).append(cd)
    metadata_lookup = {m["originId"]: m for m in metadata_nodes}
    policy_tree_records = []

    def clean_value(v):
        """Flatten list values and ensure string output."""
        if isinstance(v, list):
            return ", ".join(str(x) for x in v)
        return str(v) if v is not None else ""

    def traverse(origin_id, path_names, path_ids, position):
        node = metadata_lookup.get(origin_id)
        if not node:
            return

        fullpath_parts = path_names + [f"{node.get('originType')}:{node.get('name')}"]
        fullpath_id_parts = path_ids + [f"{node.get('originType')}:{node.get('originId')}"]

        # Extract Epic, Feature, Version only for PolicySet with "Entitlement Check"
        epic = feature = version = ""
        if node.get("originType") == "PolicySet" and "entitlement check" in node.get("name", "").lower():
            props = node.get("properties", {})
            if isinstance(props, dict):
                epic = clean_value(props.get("Epic", ""))
                feature = clean_value(props.get("Feature", ""))
                version = clean_value(props.get("Version", ""))

        policy_tree_records.append({
            "Position": position,
            "ID": node.get("originId"),
            "Policy FullPath": " / ".join(fullpath_parts),
            "Policy FullPath ID": " / ".join(fullpath_id_parts),
            "Epic": epic,
            "Feature": feature,
            "Version": version
        })

        for cd in origin_to_cdnode.get(origin_id, []):
            for i, inp_id in enumerate(cd.get("inputNodes", []), 1):
                tmn = id_lookup.get(inp_id)
                if not tmn or tmn.get("class") != "TargetMatchNode":
                    continue
                child_id = tmn.get("metadataId")
                if child_id:
                    traverse(child_id, fullpath_parts, fullpath_id_parts, f"{position}.{i}")

    # Determine root
    root_id = None
    package_meta = next((m for m in data if m.get("class") in ("Package", "DeploymentPackage")), None)
    if package_meta:
        root_id = package_meta.get("rootEntityId")
    if not root_id:
        fallback = next((m for m in metadata_nodes if m.get("name") == "NAB Policies"), None)
        if fallback:
            root_id = fallback.get("originId")
        else:
            return pd.DataFrame(columns=["Position", "ID", "Policy FullPath", "Policy FullPath ID", "Epic", "Feature", "Version"])

    traverse(root_id, [], [], "1")
    return pd.DataFrame(policy_tree_records)


def main():
    if len(sys.argv) < 2:
        print("Usage: python policy_tree_dynamic.py <path_to_deploymentpackage>")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    if not input_path.exists():
        print(f"❌ Error: File not found -> {input_path}")
        sys.exit(1)

    # Load JSON file
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Build DataFrame
    df = build_policy_tree(data)
    if df.empty:
        print("⚠️ No policy tree data extracted.")
        sys.exit(0)

    # Create output directory
    output_dir = Path("out")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define output file name based on input file
    base_name = input_path.stem.replace(".deploymentpackage", "")
    output_file = output_dir / f"{base_name}_epic_flat.xlsx"

    # Save Excel file
    df.to_excel(output_file, index=False, sheet_name="Policy_Tree")
    print(f"✅ Policy tree saved to: {output_file.resolve()}")


if __name__ == "__main__":
    main()
