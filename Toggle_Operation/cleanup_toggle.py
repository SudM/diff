#!/usr/bin/env python3
"""
cleanup_toggle.py
-----------------
Purpose:
    Cleans a toggle JSON file by keeping ONLY nodes (actions, toggles, and versions)
    that contain or descend from at least one "OFF" version (isEnabled=False).

Features:
    - Recursive pruning of all branches without OFF versions.
    - Preserves structure for OFF-related nodes only.
    - Robust error handling for every function.
    - Logs all actions and errors to toggle_operation.log.
    - Maintains all original logic and behavior.

Author: Vikas
"""

import json
import logging
from pathlib import Path
import sys

# ---------------------------------------------------------------------
# Logging Setup
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
def has_off_version(obj):
    """
    Recursively check if the given node or any of its descendants
    contain a version marked as OFF (isEnabled=False).

    Args:
        obj (dict): The toggle node to inspect.

    Returns:
        bool: True if this node or any descendant has an OFF version, else False.
    """
    try:
        versions = obj.get("versions", [])
        # If any version within this node is OFF
        if any(not v.get("isEnabled", True) for v in versions):
            return True

        # Recursively check children
        for k, v in obj.items():
            if k not in {"id", "name", "type", "versions", "isEnabled"} and isinstance(v, list):
                if any(has_off_version(c) for c in v if isinstance(c, dict)):
                    return True
        return False
    except Exception as e:
        logging.exception(f"Error in has_off_version for node: {obj.get('name', 'unknown')} | {e}")
        return False


def prune_non_off_paths(obj):
    """
    Recursively prune nodes that do not contain any OFF version.
    Keeps only branches leading to OFF versions.

    Args:
        obj (dict): The toggle node (action or child) to prune.

    Returns:
        bool: True if this node or its descendants contain OFF versions, else False.
    """
    try:
        for k in list(obj.keys()):
            # Skip non-list and irrelevant keys
            if k in {"id", "name", "type", "versions", "isEnabled"} or not isinstance(obj[k], list):
                continue

            new_list = []
            for child in obj[k]:
                if isinstance(child, dict):
                    if prune_non_off_paths(child):
                        new_list.append(child)

            obj[k] = new_list
            if not obj[k]:
                del obj[k]

        return has_off_version(obj)
    except Exception as e:
        logging.exception(f"Error pruning node {obj.get('name', 'unknown')}: {e}")
        return False


# ---------------------------------------------------------------------
# Main Function
# ---------------------------------------------------------------------
def main(input_path="output.json", output_path="final_toggle.json"):
    """
    Load a toggle JSON file, prune all nodes without OFF versions,
    and save the cleaned structure to a new JSON file.

    Args:
        input_path (str): Path to input toggle JSON.
        output_path (str): Path to cleaned output JSON.

    Behavior:
        - Reads toggle data from input_path.
        - Removes all actions, toggles, and versions that are ON.
        - Keeps only branches containing OFF versions.
        - Saves the resulting structure to output_path.
    """
    logging.info("=== Starting cleanup_toggle process ===")

    try:
        path = Path(input_path)
        if not path.exists():
            logging.error(f"Input file not found: {input_path}")
            print(f"❌ Error: {input_path} not found!")
            sys.exit(1)

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        new_actions = []
        total_actions = len(data.get("toggles", {}).get("checkPermissions", []))
        logging.info(f"Loaded {total_actions} actions from {input_path}")

        # Process each action node
        for action in data.get("toggles", {}).get("checkPermissions", []):
            try:
                # Identify top-level keys that hold children
                root_keys = [
                    k for k in action
                    if k not in {"id", "action", "isEnabled"} and isinstance(action[k], list)
                ]
                new_root = {}

                # Prune each root branch
                for k in root_keys:
                    new_list = []
                    for node in action[k]:
                        if isinstance(node, dict):
                            if prune_non_off_paths(node):
                                new_list.append(node)
                    if new_list:
                        new_root[k] = new_list

                # Skip entire action if no OFF descendants
                if not new_root:
                    logging.info(f"Removed action '{action.get('action', 'unknown')}' (no OFF paths)")
                    continue

                # Update kept action and mark enabled
                action.update(new_root)
                action["isEnabled"] = True
                new_actions.append(action)

            except Exception as inner_e:
                logging.exception(f"Error processing action {action.get('action', 'unknown')}: {inner_e}")
                continue

        # Replace original list with cleaned one
        data["toggles"]["checkPermissions"] = new_actions

        # Save the cleaned JSON file
        out_path = Path(output_path)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        logging.info(f"✅ Cleaned {len(new_actions)} actions (from {total_actions}) → saved to {output_path}")
        print(f"✅ Cleaned {len(new_actions)} actions (from {total_actions}) → saved to {output_path}")

    except json.JSONDecodeError as jde:
        logging.exception(f"Invalid JSON structure: {jde}")
        print("❌ Error: Input file is not a valid JSON. Check toggle_operation.log for details.")
        sys.exit(1)

    except Exception as e:
        logging.exception(f"Critical error during cleanup: {e}")
        print("❌ Unexpected error occurred. Check toggle_operation.log for details.")
        sys.exit(1)

    finally:
        logging.info("=== Cleanup process completed ===")


# ---------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------
if __name__ == "__main__":
    try:
        input_file = sys.argv[1] if len(sys.argv) > 1 else "output.json"
        output_file = sys.argv[2] if len(sys.argv) > 2 else "final_toggle.json"
        main(input_file, output_file)
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user.")
        print("\n⚠️ Process interrupted by user.")
        sys.exit(0)
