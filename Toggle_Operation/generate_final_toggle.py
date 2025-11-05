#!/usr/bin/env python3
"""
FINAL cleanup_toggle.py (Replace Mode)
--------------------------------------
Steps:
1. Propagate OFF upward (safe, never flips ON incorrectly)
2. Prune ON versions and empty arrays
3. Remove ON-only toggle nodes without versions/subtoggles
4. Load prod_toggle.json as base, replace its checkPermissions with
   the cleaned version from final_package_toggle.json.
   Preserve key order identical to prod_toggle.json.
"""

import json
import sys
import logging
from typing import Any, Dict, List, Optional
from collections import OrderedDict

LOG_FILE = "toggle_operation.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# -----------------------------------------------------
# Phase 1 — propagate OFF upward safely
# -----------------------------------------------------
def propagate_off(node: Dict[str, Any]) -> bool:
    """Mark node OFF only if all descendants are OFF."""
    try:
        if "versions" in node:
            vers = node.get("versions", [])
            if not vers:
                node["isEnabled"] = False
                return True
            all_off = all(not v.get("isEnabled", True) for v in vers)
            if all_off:
                node["isEnabled"] = False
            return any(not v.get("isEnabled", True) for v in vers)

        has_off = False
        all_off = True
        for k, v in node.items():
            if k in {"id", "name", "isEnabled", "action", "Category", "type"}:
                continue
            if not isinstance(v, list):
                continue
            for c in v:
                if isinstance(c, dict):
                    child_off = propagate_off(c)
                    has_off |= child_off
                    if c.get("isEnabled", True):
                        all_off = False
        if all_off and has_off:
            node["isEnabled"] = False
            logging.info(f"Propagated OFF → {node.get('name')}")
        return has_off
    except Exception as e:
        logging.exception(f"propagate_off error: {e}")
        return False


# -----------------------------------------------------
# Phase 2 — prune ON versions and empty arrays
# -----------------------------------------------------
def prune_versions_and_empty(node: Any) -> Any:
    """Remove ON versions and empty toggle arrays recursively."""
    if isinstance(node, dict):
        if "versions" in node:
            before = len(node["versions"])
            node["versions"] = [v for v in node["versions"] if not v.get("isEnabled", True)]
            removed = before - len(node["versions"])
            if removed:
                logging.info(f"Removed {removed} ON versions under {node.get('name')}")

        to_delete = []
        for k, v in node.items():
            if isinstance(v, list):
                new_list = []
                for item in v:
                    cleaned = prune_versions_and_empty(item)
                    if cleaned and (not isinstance(cleaned, dict) or cleaned):
                        new_list.append(cleaned)
                if new_list:
                    node[k] = new_list
                else:
                    to_delete.append(k)
            elif isinstance(v, dict):
                node[k] = prune_versions_and_empty(v)
        for k in to_delete:
            del node[k]
            logging.info(f"Removed empty branch '{k}' under {node.get('name')}")
        return node
    elif isinstance(node, list):
        return [prune_versions_and_empty(i) for i in node if i]
    else:
        return node


# -----------------------------------------------------
# Phase 3 — remove ON toggle nodes without versions/subtoggles
# -----------------------------------------------------
def remove_empty_on_toggles(node: Any) -> Any:
    """Remove ON nodes that have no versions and no sub-toggles."""
    if isinstance(node, dict):
        keys_to_delete = []
        for k, v in list(node.items()):
            if isinstance(v, list):
                cleaned_list = []
                for item in v:
                    if isinstance(item, dict):
                        cleaned = remove_empty_on_toggles(item)
                        if cleaned:
                            cleaned_list.append(cleaned)
                    else:
                        cleaned_list.append(item)
                if cleaned_list:
                    node[k] = cleaned_list
                else:
                    keys_to_delete.append(k)
        for k in keys_to_delete:
            node.pop(k, None)
            logging.info(f"Removed empty ON branch '{k}' under {node.get('name')}")

        has_versions = "versions" in node and bool(node["versions"])
        has_sub_lists = any(
            isinstance(v, list) and v
            for kk, v in node.items()
            if kk not in {"id", "name", "isEnabled", "action", "Category", "type", "versions"}
        )
        if node.get("isEnabled", True) and not has_versions and not has_sub_lists:
            logging.info(f"Pruned ON node {node.get('name')} (no versions/subtoggles)")
            return None
        return node

    elif isinstance(node, list):
        cleaned = [remove_empty_on_toggles(n) for n in node if n]
        return [x for x in cleaned if x]
    return node


# -----------------------------------------------------
# Per-action cleanup
# -----------------------------------------------------
def cleanup_action(action: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Clean one action node completely."""
    try:
        act = json.loads(json.dumps(action))
        has_off = False

        for k, v in act.items():
            if k in {"id", "action", "isEnabled"}:
                continue
            if isinstance(v, list):
                for c in v:
                    if isinstance(c, dict) and propagate_off(c):
                        has_off = True

        if not has_off:
            logging.info(f"{act.get('action')} → all ON, dropped")
            return None

        prune_versions_and_empty(act)
        act = remove_empty_on_toggles(act)

        if not act:
            logging.info(f"{action.get('action')} → fully pruned")
            return None
        return act
    except Exception as e:
        logging.exception(f"cleanup_action error: {e}")
        return None


# -----------------------------------------------------
# Final Replace: load prod_toggle.json and replace checkPermissions
# -----------------------------------------------------
def replace_checkpermissions_in_prod(prod_path: str, cleaned_final_path: str, output_path: str):
    """
    Read prod_toggle.json and final_package_toggle.json.
    Replace the 'checkPermissions' subtree in PROD with the cleaned version,
    preserving the order and saving back into final_package_toggle.json.
    """
    try:
        with open(prod_path, "r", encoding="utf-8") as f:
            prod_data = json.load(f, object_pairs_hook=OrderedDict)

        with open(cleaned_final_path, "r", encoding="utf-8") as f:
            cleaned_data = json.load(f, object_pairs_hook=OrderedDict)

        cleaned_check = cleaned_data.get("toggles", {}).get("checkPermissions", [])
        prod_toggles = prod_data.get("toggles", OrderedDict())

        if "checkPermissions" in prod_toggles:
            prod_toggles["checkPermissions"] = cleaned_check
            logging.info("Replaced existing checkPermissions in PROD")
        else:
            # If PROD has no checkPermissions, insert at the start
            prod_toggles = OrderedDict([("checkPermissions", cleaned_check), *prod_toggles.items()])
            logging.info("Inserted new checkPermissions at start")

        # Rebuild full structure
        final_out = OrderedDict([
            ("schemaVersion", prod_data.get("schemaVersion")),
            ("strategy", prod_data.get("strategy")),
            ("toggles", prod_toggles)
        ])

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(final_out, f, indent=2)
        
        print(f"✅ checkPermissions replaced successfully in order → {output_path}")
        logging.info(f"✅ checkPermissions replaced successfully → {output_path}")

    except Exception as e:
        logging.exception(f"replace_checkpermissions_in_prod error: {e}")
        print("❌ Replacement error — check log")


# -----------------------------------------------------
# Main driver
# -----------------------------------------------------
def main(inp="package_toggle.json", prod="prod_toggle.json", out="final_package_toggle.json"):
    logging.info("=== CLEANUP START ===")
    try:
        # Step 1–3: clean the package_toggle.json
        with open(inp, "r", encoding="utf-8") as f:
            data = json.load(f, object_pairs_hook=OrderedDict)

        actions = data.get("toggles", {}).get("checkPermissions", [])
        cleaned_actions = []

        for a in actions:
            cleaned = cleanup_action(a)
            if cleaned:
                cleaned_actions.append(cleaned)

        # Write intermediate cleaned file (same as old final)
        interim = "final_package_toggle_temp.json"
        cleaned_data = OrderedDict([
            ("schemaVersion", data.get("schemaVersion")),
            ("strategy", data.get("strategy")),
            ("toggles", OrderedDict([("checkPermissions", cleaned_actions)]))
        ])
        with open(interim, "w", encoding="utf-8") as f:
            json.dump(cleaned_data, f, indent=2)

        # Step 4: replace into PROD preserving order
        replace_checkpermissions_in_prod(prod, interim, out)

                # cleanup temporary file
        import os
        try:
            if os.path.exists(interim):
                os.remove(interim)
                logging.info(f"Deleted temporary file {interim}")
        except Exception as e:
            logging.warning(f"Could not delete temporary file {interim}: {e}")


    except Exception as e:
        logging.exception(f"main error: {e}")
        print("❌ Error, check log")
    finally:
        logging.info("=== CLEANUP END ===")


if __name__ == "__main__":
    i = sys.argv[1] if len(sys.argv) > 1 else "./output/package_toggle.json"
    p = sys.argv[2] if len(sys.argv) > 2 else "./input/prod_toggle.json"
    o = sys.argv[3] if len(sys.argv) > 3 else "./output/final_package_toggle.json"
    main(i, p, o)
