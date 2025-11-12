#!/usr/bin/env python3
"""
post_release_pap_update.py (Unified Version with PAP Execution and Version ID Mapping)
==================================================================================
- Generates correct hierarchical package_toggle.json structure.
- Maps Version IDs from package_toggle.json to release.xlsx.
- Fetches branch ID from PAP.
- Updates Status in PROD for each Policy/PolicySet based on Excel.
- Commits the branch with comment '<branch_name> - Status in PROD Update'.
"""

import os
import sys
import json
import time
import pandas as pd
import requests
import logging
import getpass
from pathlib import Path
from cryptography.fernet import Fernet
from urllib3.exceptions import InsecureRequestWarning
from collections import defaultdict
import argparse
import re
# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------
BASE_URL_PAP = "https://my-pap:8443"
BASE_URL_ALT = "https://172.29.206.92:8443"
BASE_URL = BASE_URL_ALT
PAGE_SIZE = 100
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
VERIFY_SSL = False
if not VERIFY_SSL:
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

LOG_FILE = "toggle_operation.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, filemode="a", format="%(asctime)s [%(levelname)s] %(message)s")

# ----------------------------------------------------------------------
# ENCRYPTION HELPERS
# ----------------------------------------------------------------------
def generate_key() -> bytes:
    key_file = Path("auth.key")
    if key_file.exists():
        return key_file.read_bytes()
    key = Fernet.generate_key()
    key_file.write_bytes(key)
    return key

def encrypt_credentials(username: str, password: str):
    key = generate_key()
    fernet = Fernet(key)
    return fernet.encrypt(username.encode()), fernet.encrypt(password.encode())

def decrypt_credentials(enc_user: bytes, enc_pass: bytes):
    key = generate_key()
    fernet = Fernet(key)
    return fernet.decrypt(enc_user).decode(), fernet.decrypt(enc_pass).decode()

# ----------------------------------------------------------------------
# PACKAGE PARSING & MAPPING
# ----------------------------------------------------------------------
def safe_load_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.exception(f"Failed to load deploymentpackage: {e}")
        sys.exit(1)

def flatten_toggle_mapping(toggle_json_path: str):
    """Traverse hierarchical package_toggle.json and flatten mapping of (action, path, version) -> (category, id)."""
    with open(toggle_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    mapping = {}
    for toggle in data.get("toggles", {}).get("checkPermissions", []):
        action = toggle.get("action", "").strip()
        for tenant in toggle.get("Tenants", []) + toggle.get("tenants", []):
            tenant_name = tenant.get("name", "").strip().upper()
            # tenant-level versions
            for ver in tenant.get("versions", []):
                key = (action, tenant_name, ver.get("name", "").strip().upper())
                mapping[key] = (ver.get("Category", ""), ver.get("id", ""))

            # audience nested versions
            for audience in tenant.get("audience", []):
                aud_name = audience.get("name", "").strip().upper()
                full_path = f"{tenant_name}:{aud_name}".upper()
                for ver in audience.get("versions", []):
                    key = (action, full_path, ver.get("name", "").strip().upper())
                    mapping[key] = (ver.get("Category", ""), ver.get("id", ""))
    return mapping

def build_policy_tree(data):
    metadata_nodes = [d for d in data if d.get("class") == "Metadata" and d.get("originType") in ("PolicySet", "Policy")]
    id_lookup = {d.get("id"): d for d in data if "id" in d}
    cd_nodes = {d["id"]: d for d in data if d.get("class") == "CombinedDecisionNode"}
    metadata_lookup = {m["originId"]: m for m in metadata_nodes}
    origin_to_cdnode = defaultdict(list)
    for cd in cd_nodes.values():
        if cd.get("originLink"):
            origin_to_cdnode[cd["originLink"]].append(cd)

    records = []

    def traverse(origin_id, path_stack, position):
        node = metadata_lookup.get(origin_id)
        if not node:
            return
        origin_type = node.get("originType", "")
        name = node.get("name", "")
        full_path = path_stack + [f"{origin_type}:{name}"]
        props = node.get("properties", {}) or {}
        record = {
            "Position": position,
            "ID": node.get("originId", ""),
            "Policy FullPath": " / ".join(full_path),
            "Action": props.get("action", ""),
            "Toggle Type": props.get("Toggle Type", ""),
            "Toggle Name": props.get("Toggle Name", name),
            "Status In Prod": props.get("Status in PROD", ""),
            "Category": origin_type,
        }
        records.append(record)
        for cd in origin_to_cdnode.get(origin_id, []):
            for i, inp_id in enumerate(cd.get("inputNodes", []), 1):
                tmn = id_lookup.get(inp_id)
                if tmn and tmn.get("class") == "TargetMatchNode" and tmn.get("metadataId"):
                    traverse(tmn["metadataId"], full_path, f"{position}.{i}")

    package_meta = next((m for m in data if m.get("class") in ("Package", "DeploymentPackage")), None)
    if not package_meta or not package_meta.get("rootEntityId"):
        print("Invalid .deploymentpackage: no rootEntityId found.")
        sys.exit(1)

    traverse(package_meta["rootEntityId"], [], "1")
    return pd.DataFrame(records)

def build_package_toggle(deployment_file: str, output_json: str):
    data = safe_load_json(deployment_file)
    df = build_policy_tree(data)
    return df

# ----------------------------------------------------------------------
# PAP API FUNCTIONS
# ----------------------------------------------------------------------
def get_branch_id(session: requests.Session, branch_name: str) -> str:
    """
    Retrieve branch ID from PAP by branch name, with full pagination support.

    Args:
        session: Authenticated requests.Session
        branch_name: Exact branch name (case-sensitive)

    Returns:
        str: The branch ID

    Raises:
        SystemExit: If branch not found or API error
    """
    url = f"{BASE_URL}/api/version-control/branches"
    params = {"pageSize": PAGE_SIZE, "page": 1}
    page = 1

    while True:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logging.debug(f"Fetching page {page} (attempt {attempt})...")
                resp = session.get(url, params=params, verify=VERIFY_SSL, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                break  # Success → exit retry loop
            except requests.exceptions.RequestException as e:
                if attempt == MAX_RETRIES:
                    logging.error(f"Failed to fetch branches after {MAX_RETRIES} attempts: {e}")
                    raise SystemExit(1)
                logging.warning(f"Retry {attempt}/{MAX_RETRIES} after error: {e}")
                time.sleep(RETRY_DELAY)
        else:
            raise SystemExit(1)  # Should never reach here

        # -----------------------------------------------------------------
        # Parse response
        # -----------------------------------------------------------------
        if not isinstance(data, dict) or "data" not in data:
            logging.error(f"Unexpected response format: {data}")
            raise SystemExit(1)

        branches = data["data"]
        if not isinstance(branches, list):
            logging.error(f"'data' is not a list: {branches}")
            raise SystemExit(1)

        # Search in current page
        for branch in branches:
            if not isinstance(branch, dict):
                continue
            name = branch.get("name")
            branch_id = branch.get("id")
            if name == branch_name:
                logging.info(f"Found branch '{branch_name}' → ID: {branch_id}")
                return branch_id

        # -----------------------------------------------------------------
        # Pagination check
        # -----------------------------------------------------------------
        pagination = data.get("pagination", {})
        total_pages = pagination.get("totalPages", 1)
        current_page = pagination.get("page", 1)

        if current_page >= total_pages:
            break  # Done

        page += 1
        params["page"] = page

    # -----------------------------------------------------------------
    # Not found
    # -----------------------------------------------------------------
    logging.error(f"Branch '{branch_name}' not found after {page} page(s).")
    raise SystemExit(1)

def build_url(category: str, obj_id: str, branch_id: str) -> str:
    kind = "policies" if category.lower() == "policy" else "policysets"
    return f"{BASE_URL}/api/v2/policy-manager/{kind}/{obj_id}?branch={branch_id}"

def update_status_in_payload(payload, new_status):
    if isinstance(payload, list):
        payload = payload[0]
    found = False
    for prop in payload.get("properties", []):
        if prop.get("key") == "Status in PROD":
            prop["value"] = new_status.strip()
            found = True
    if not found:
        payload.setdefault("properties", []).append({"key": "Status in PROD", "value": new_status.strip()})
    return payload

def get_object(session, url):
    try:
        r = session.get(url, verify=VERIFY_SSL, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"GET failed for {url}: {e}")
        return None

def put_object(session, url, payload):
    try:
        r = session.put(url, data=json.dumps(payload), verify=VERIFY_SSL, timeout=20)
        r.raise_for_status()
        logging.info(f"Updated successfully: {url}")
        return True
    except Exception as e:
        logging.error(f"PUT failed for {url}: {e}")
        return False

def commit_branch(session: requests.Session, branch_id: str, message: str) -> bool:
    """
    Commit branch — EXACTLY matches your working cURL.
    """
    url = f"{BASE_URL}/api/version-control/branches/{branch_id}/commit"

    # -----------------------------------------------------------------
    # CLEAN & VALIDATE MESSAGE
    # -----------------------------------------------------------------
    clean_msg = (message or "").strip()
    if not clean_msg:
        logging.error("Commit message is empty or whitespace only")
        return False

    if len(clean_msg) > 255:
        clean_msg = clean_msg[:255]
        logging.warning(f"Message truncated to 255 chars: {clean_msg}")

    # Remove any control characters
    clean_msg = ''.join(c for c in clean_msg if ord(c) >= 32)

    # -----------------------------------------------------------------
    # PAYLOAD: MUST BE "message", NOT "commitMessage"
    # -----------------------------------------------------------------
    payload = {"message": clean_msg}  # ← EXACT FIELD FROM cURL

    headers = {
        "Content-Type": "application/json",
        "x-user-id": "admin",
        "Accept": "application/json"
    }

    logging.info(f"Committing branch {branch_id}")
    logging.info(f"Message: '{clean_msg}'")
    logging.debug(f"URL: {url}")
    logging.debug(f"Headers: {headers}")
    logging.debug(f"Payload: {payload}")

    try:
        r = session.post(
            url,
            json=payload,           # ← Let requests serialize to JSON
            headers=headers,
            verify=VERIFY_SSL,
            timeout=30
        )

        logging.info(f"Status: {r.status_code}")
        logging.debug(f"Response: {r.text}")

        r.raise_for_status()
        print(f"Branch committed: '{clean_msg}'")
        return True

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP {r.status_code}: {r.text}")
        return False
    except Exception as e:
        logging.error(f"Request failed: {e}")
        return False    
    
# ----------------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Post Release PAP Update Script")
    parser.add_argument("--release", required=False, default="./input/release.xlsx", help="Path to release.xlsx")
    parser.add_argument("--deployment", required=False, default="./input/Sudha_Test2.deploymentpackage", help="Path to .deploymentpackage file")
    parser.add_argument("--branch", required=False, default="test", help="Release branch name")
    parser.add_argument("--username", required=False, default="administrator", help="PAP username")
    parser.add_argument("--password", required=False,default="password123",  help="PAP password")
    args = parser.parse_args()

    release_xlsx = args.release
    deployment_file = args.deployment
    branch_name = args.branch

    username = args.username or os.getenv("PAP_USER") or "admin"
    password = args.password or os.getenv("PAP_PASS") or getpass.getpass("🔒 Enter PAP password: ")

    enc_user, enc_pass = encrypt_credentials(username, password)
    dec_user, dec_pass = decrypt_credentials(enc_user, enc_pass)

    session = requests.Session()
    session.auth = (dec_user, dec_pass)
    session.headers.update({"Content-Type": "application/json", "x-user-id": dec_user})

    # Step 1: Build package_toggle.json mapping
    package_json = "package_toggle.json"
    toggle_df = build_package_toggle(deployment_file, package_json)
    mapping = flatten_toggle_mapping(package_json)

    # Step 2: Update Excel with correct Version ID mapping
    release_df = pd.read_excel(release_xlsx)

    def get_cat_id(row):
        key = (str(row.get("Action", "")).strip(), str(row.get("Path", "")).strip().upper(), str(row.get("Version", "")).strip().upper())
        return mapping.get(key, ("", ""))

    release_df[["Category", "ID"]] = release_df.apply(lambda r: pd.Series(get_cat_id(r)), axis=1)
    release_df.to_excel(release_xlsx, index=False)
    print(f"✅ Updated release.xlsx with correct Version ID mapping.")

    # Step 3: Get branch ID from PAP
    branch_id = get_branch_id(session, branch_name)
    print(f"🌿 Branch ID: {branch_id}")

    # Step 4: Update Status in PAP
    for i, row in release_df.iterrows():
        cat, obj_id, status = row.get("Category"), row.get("ID"), str(row.get("Status", "")).strip()
        if not obj_id or not cat:
            continue
        url = build_url(cat, obj_id, branch_id)
        payload = get_object(session, url)
        if not payload:
            continue
        updated_payload = update_status_in_payload(payload, status)
        success = put_object(session, url, updated_payload)
        if success:
            print(f"Row {i+2}: ✅ {cat} {obj_id} → {status}")

    # Step 5: Commit the branch
    commit_branch(session, branch_id, f"{branch_name} - Status in PROD Properties Update")

    print("\n🎉 All done! Check toggle_operation.log for details.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user.")
        sys.exit(0)