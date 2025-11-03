#!/usr/bin/env python3
"""
update_release_toggle_input.py
------------------------------
Reads Sheet1 of a release Excel file and updates toggle version enablement
status in a target JSON file. Then, it recomputes the `isEnabled` flag
for all parent toggle nodes recursively.

"""

import json
import pandas as pd
import logging
from pathlib import Path
import sys

# ---------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------
LOG_FILE = "toggle_operation.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------
def load_sheet1(excel_path: str) -> pd.DataFrame:
    """
    Load 'Sheet1' from the given Excel file, filter only 'Decision' service rows,
    and drop incomplete rows.

    Args:
        excel_path (str): Path to Excel file (release.xlsx)

    Returns:
        pd.DataFrame: Filtered DataFrame ready for processing.

    Raises:
        FileNotFoundError: If the Excel file is missing.
        KeyError: If required columns are missing.
    """
    try:
        logging.info(f"Loading Excel sheet: {excel_path}")
        df = pd.read_excel(excel_path, sheet_name="Sheet1")
        required_cols = {"Service", "Action", "Toggle", "Version", "Status"}
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            raise KeyError(f"Missing required columns: {missing}")

        df = df[df["Service"] == "Decision"]
        df = df.dropna(subset=["Action", "Toggle", "Version", "Status"])
        logging.info(f"Loaded {len(df)} valid rows from Sheet1")
        return df

    except FileNotFoundError:
        logging.exception(f"Excel file not found: {excel_path}")
        print(f"❌ Excel file not found: {excel_path}")
        sys.exit(1)

    except Exception as e:
        logging.exception(f"Error loading sheet: {e}")
        print(f"❌ Error reading Excel sheet. Check {LOG_FILE} for details.")
        sys.exit(1)


def find_action(data: dict, action_value: str):
    """
    Find an action node in the JSON data by its action name.

    Args:
        data (dict): Parsed JSON toggle data.
        action_value (str): Action name to locate.

    Returns:
        dict | None: The matching action node or None if not found.
    """
    try:
        for action in data.get("toggles", {}).get("checkPermissions", []):
            if action.get("action") == action_value:
                return action
        return None
    except Exception as e:
        logging.exception(f"Error finding action '{action_value}': {e}")
        return None


def traverse_toggle_path(action: dict, path: list) -> dict:
    """
    Traverse toggle hierarchy (Action → Toggle → SubToggle...) to reach the
    desired toggle node.

    Args:
        action (dict): Action object from JSON data.
        path (list): List of toggle path segments (e.g., ["ACCOUNT", "INFO"]).

    Returns:
        dict | None: Found toggle node or None if not found.
    """
    try:
        current = action
        for name in path:
            found = False
            for k, v in current.items():
                if k not in {"id", "action", "isEnabled"} and isinstance(v, list):
                    for child in v:
                        if child.get("name", "").upper() == name.upper():
                            current = child
                            found = True
                            break
                    if found:
                        break
            if not found:
                logging.warning(f"Path not found: {name}")
                print(f"  ⚠️ Path not found: {name}")
                return None
        return current
    except Exception as e:
        logging.exception(f"Error traversing path {path}: {e}")
        return None


def recompute_isEnabled(node: dict) -> bool:
    """
    Recursively recompute 'isEnabled' status for each toggle node.
    A node is enabled if any of its versions or children are enabled.

    Args:
        node (dict): Node to recompute.

    Returns:
        bool: Updated isEnabled value for the node.
    """
    try:
        versions = node.get("versions", [])
        children = [
            item for k, v in node.items()
            if k not in {"id", "name", "versions", "isEnabled"} and isinstance(v, list)
            for item in v if isinstance(item, dict)
        ]
        has_on = any(v.get("isEnabled", False) for v in versions)
        has_on_child = any(recompute_isEnabled(c) for c in children)
        node["isEnabled"] = has_on or has_on_child
        return node["isEnabled"]
    except Exception as e:
        logging.exception(f"Error recomputing isEnabled for node {node.get('name','')} : {e}")
        return node.get("isEnabled", False)

# ---------------------------------------------------------------------
# Main Logic
# ---------------------------------------------------------------------
def main(excel_file="release.xlsx", json_file="package_toggle.json"):
    """
    Main function to:
    1. Load Sheet1 from Excel.
    2. Update toggle versions' isEnabled based on 'Status'.
    3. Recompute hierarchical isEnabled values.
    4. Save the updated JSON.

    Args:
        excel_file (str): Path to Excel input file.
        json_file (str): Path to target JSON toggle file.
    """
    logging.info("=== Starting toggle update process ===")
    try:
        df = load_sheet1(excel_file)
        path = Path(json_file)

        if not path.exists():
            logging.error(f"JSON file not found: {json_file}")
            print(f"❌ JSON file not found: {json_file}")
            sys.exit(1)

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        updated = 0
        for _, row in df.iterrows():
            action_value = row["Action"]
            toggle_path = [s.strip() for s in row["Toggle"].split(":")]
            version_name = row["Version"]
            status = str(row["Status"]).strip().lower()
            is_enabled = status != "off"

            logging.info(f"Updating Action={action_value}, Path={toggle_path}, Version={version_name}, Status={status}")
            print(f"\nUpdating: {action_value}\n  Path: {' → '.join(toggle_path)}\n  Version: {version_name} = {is_enabled}")

            action = find_action(data, action_value)
            if not action:
                print("  ⚠️ Action not found.")
                logging.warning(f"Action not found: {action_value}")
                continue

            toggle_node = traverse_toggle_path(action, toggle_path)
            if not toggle_node:
                continue

            versions = toggle_node.get("versions", [])
            version_node = next((v for v in versions if v.get("name", "").upper() == version_name.upper()), None)
            if not version_node:
                print(f"  ⚠️ Version {version_name} not found.")
                logging.warning(f"Version not found: {version_name} in {toggle_path}")
                continue

            version_node["isEnabled"] = is_enabled
            updated += 1
            logging.info(f"Set {version_name}.isEnabled = {is_enabled}")
            print(f"  ✅ Set {version_name}.isEnabled = {is_enabled}")

        # Recompute all isEnabled values
        print("\nRecomputing isEnabled...")
        logging.info("Recomputing parent isEnabled flags...")
        for action in data.get("toggles", {}).get("checkPermissions", []):
            for k, v in action.items():
                if k not in {"id", "action", "isEnabled"} and isinstance(v, list):
                    for node in v:
                        if isinstance(node, dict):
                            recompute_isEnabled(node)

            root_nodes = [
                item for k, v in action.items()
                if k not in {"id", "action", "isEnabled"} and isinstance(v, list)
                for item in v
            ]
            action["isEnabled"] = any(n.get("isEnabled", False) for n in root_nodes)

        # Save updated JSON
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        logging.info(f"✅ Updated {updated} version entries. Saved to {json_file}")
        print(f"\n✅ Updated {updated} versions → saved to {json_file}")

    except Exception as e:
        logging.exception(f"Critical error in main(): {e}")
        print(f"❌ Critical error occurred. Check {LOG_FILE} for details.")
        sys.exit(1)
    finally:
        logging.info("=== Toggle update process completed ===")


# ---------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------
if __name__ == "__main__":
    try:
        excel_file = sys.argv[1] if len(sys.argv) > 1 else "release.xlsx"
        json_file = sys.argv[2] if len(sys.argv) > 2 else "package_toggle.json"
        main(excel_file, json_file)
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user.")
        print("\n⚠️ Process interrupted by user.")
        sys.exit(0)
