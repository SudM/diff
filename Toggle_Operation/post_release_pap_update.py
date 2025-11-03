#!/usr/bin/env python3
"""
post_release_pap_update.py
--------------------------
Purpose:
    Reads status update data from an Excel sheet and updates the 'Status'
    property of Policy or PolicySet objects in a PingAuthorize PAP instance
    via REST API calls (GET → modify → PUT).

"""

import pandas as pd
import requests
import json
import logging
import getpass
from cryptography.fernet import Fernet
from urllib3.exceptions import InsecureRequestWarning
from pathlib import Path
import sys
import os

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------
EXCEL_FILE = "post_release.xlsx"
SHEET_NAME = "Sheet1"

BASE_URL_PAP = "https://my-pap:8443"
BASE_URL_ALT = "https://172.29.206.92:8443"
BASE_URL = BASE_URL_PAP  # Switch here if needed

VERIFY_SSL = False
if not VERIFY_SSL:
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# ----------------------------------------------------------------------
# LOGGING
# ----------------------------------------------------------------------
LOG_FILE = "toggle_operation.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ----------------------------------------------------------------------
# ENCRYPTION HELPERS
# ----------------------------------------------------------------------
def generate_key() -> bytes:
    """
    Generate or load an encryption key (auth.key).
    Returns:
        bytes: Encryption key.
    """
    try:
        key_file = Path("auth.key")
        if key_file.exists():
            return key_file.read_bytes()
        key = Fernet.generate_key()
        key_file.write_bytes(key)
        logging.info("Encryption key generated and saved to auth.key")
        return key
    except Exception as e:
        logging.exception(f"Error generating encryption key: {e}")
        sys.exit(1)


def encrypt_credentials(username: str, password: str) -> tuple[bytes, bytes]:
    """
    Encrypt username and password with Fernet.

    Args:
        username (str): Username in plain text.
        password (str): Password in plain text.

    Returns:
        tuple(bytes, bytes): Encrypted username and password.
    """
    try:
        key = generate_key()
        fernet = Fernet(key)
        return fernet.encrypt(username.encode()), fernet.encrypt(password.encode())
    except Exception as e:
        logging.exception(f"Error encrypting credentials: {e}")
        sys.exit(1)


def decrypt_credentials(enc_user: bytes, enc_pass: bytes) -> tuple[str, str]:
    """
    Decrypt encrypted username and password.

    Args:
        enc_user (bytes): Encrypted username.
        enc_pass (bytes): Encrypted password.

    Returns:
        tuple(str, str): Decrypted username and password.
    """
    try:
        key = generate_key()
        fernet = Fernet(key)
        return fernet.decrypt(enc_user).decode(), fernet.decrypt(enc_pass).decode()
    except Exception as e:
        logging.exception(f"Error decrypting credentials: {e}")
        sys.exit(1)

# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------
def build_url(category: str, obj_id: str, branch_id: str) -> str:
    """
    Construct a PingAuthorize REST API URL for the given object.

    Args:
        category (str): 'Policy' or 'PolicySet'.
        obj_id (str): Object ID.
        branch_id (str): Branch name or ID.

    Returns:
        str: Fully constructed API URL.
    """
    try:
        kind = "policies" if category.lower() == "policy" else "policysets"
        return f"{BASE_URL}/api/v2/policy-manager/{kind}/{obj_id}?branch={branch_id}"
    except Exception as e:
        logging.exception(f"Error building URL: {e}")
        raise


def update_status_in_payload(payload: dict | list, new_status: str) -> dict | list:
    """
    Update or insert the 'Status' property in the given object payload.

    Args:
        payload (dict | list): JSON object or list from API response.
        new_status (str): New status value.

    Returns:
        dict | list: Updated payload ready for PUT request.
    """
    try:
        if isinstance(payload, list):
            if not payload:
                raise ValueError("Empty payload list from API")
            obj = payload[0]
        else:
            obj = payload

        found = False
        for prop in obj.get("properties", []):
            if prop.get("key") == "Status":
                prop["value"] = new_status.strip().upper()
                found = True
                break

        if not found:
            obj.setdefault("properties", []).append({
                "key": "Status",
                "value": new_status.strip().upper()
            })

        return payload if isinstance(payload, list) else obj
    except Exception as e:
        logging.exception(f"Error updating status in payload: {e}")
        raise


def load_excel_data(excel_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Load Excel sheet data for PAP updates.

    Args:
        excel_path (str): Path to Excel file.
        sheet_name (str): Sheet name to read.

    Returns:
        pd.DataFrame: Cleaned dataframe with required columns.
    """
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        required_cols = {"Service", "Action", "Toggle", "Version",
                         "Status", "ID", "Category", "branchid"}
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            raise KeyError(f"Missing columns: {missing}")

        logging.info(f"Excel loaded successfully ({len(df)} rows)")
        return df
    except FileNotFoundError:
        logging.exception(f"Excel file not found: {excel_path}")
        print(f"❌ Excel file not found: {excel_path}")
        sys.exit(1)
    except Exception as e:
        logging.exception(f"Error loading Excel data: {e}")
        print("❌ Failed to read Excel. Check log for details.")
        sys.exit(1)


def get_object(session: requests.Session, url: str) -> dict | None:
    """
    Fetch a policy or policy set from the PingAuthorize PAP API.

    Args:
        session (requests.Session): Authenticated session.
        url (str): API URL for GET.

    Returns:
        dict | None: JSON object or None on failure.
    """
    try:
        resp = session.get(url, verify=VERIFY_SSL, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logging.error(f"GET failed for {url}: {e}")
        print(f"  ❌ GET failed: {e}")
        return None
    except json.JSONDecodeError:
        logging.exception(f"Invalid JSON response from {url}")
        print("  ❌ Invalid JSON response.")
        return None
    except Exception as e:
        logging.exception(f"Error during GET for {url}: {e}")
        return None


def put_object(session: requests.Session, url: str, payload: dict) -> bool:
    """
    PUT updated policy or policy set to the PAP.

    Args:
        session (requests.Session): Authenticated session.
        url (str): API URL for PUT.
        payload (dict): Updated object data.

    Returns:
        bool: True if update succeeded, False otherwise.
    """
    try:
        resp = session.put(url, data=json.dumps(payload),
                           verify=VERIFY_SSL, timeout=20)
        resp.raise_for_status()
        logging.info(f"✅ PUT success ({resp.status_code}) for {url}")
        print(f"  ✅ Update successful ({resp.status_code})")
        return True
    except requests.RequestException as e:
        logging.error(f"PUT failed for {url}: {e}")
        print(f"  ❌ PUT failed: {e}")
        return False
    except Exception as e:
        logging.exception(f"Unexpected error during PUT for {url}: {e}")
        return False

# ----------------------------------------------------------------------
# MAIN PROCESS
# ----------------------------------------------------------------------
def main():
    """
    Main function:
        1. Prompt for credentials securely.
        2. Load Excel updates.
        3. Iterate over rows: GET → update 'Status' → PUT.
        4. Log all progress and failures.
    """
    logging.info("=== Starting post-release PAP update ===")

    try:
        # Step 1: Secure credential input
        username = input("👤 Enter username: ").strip()
        password = getpass.getpass("🔒 Enter password: ")

        # Encrypt/decrypt credentials
        enc_user, enc_pass = encrypt_credentials(username, password)
        dec_user, dec_pass = decrypt_credentials(enc_user, enc_pass)

        # Step 2: Load Excel data
        df = load_excel_data(EXCEL_FILE, SHEET_NAME)

        # Step 3: Configure HTTP session
        session = requests.Session()
        session.headers.update({
            "Content-Type": "application/json",
            "x-user-id": dec_user
        })
        session.auth = (dec_user, dec_pass)

        # Step 4: Iterate and update objects
        for idx, row in df.iterrows():
            try:
                obj_id = row["ID"]
                category = str(row["Category"]).strip()
                branch_id = row["branchid"]
                new_status = str(row["Status"]).strip()

                url = build_url(category, obj_id, branch_id)
                print(f"\n--- Row {idx + 2} ---")
                print(f"  Category : {category}")
                print(f"  URL      : {url}")
                print(f"  New Status: {new_status}")

                payload = get_object(session, url)
                if not payload:
                    continue

                payload = update_status_in_payload(payload, new_status)
                put_object(session, url, payload)

            except Exception as inner_e:
                logging.exception(f"Error processing row {idx + 2}: {inner_e}")
                print(f"  ❌ Error in row {idx + 2}. Check log for details.")
                continue

        print("\n✅ All rows processed.")
        logging.info("✅ Completed all updates successfully.")

    except Exception as e:
        logging.exception(f"Critical error in main(): {e}")
        print("❌ Critical error. Check toggle_operation.log for details.")
        sys.exit(1)
    finally:
        logging.info("=== Post-release PAP update process complete ===")

# ----------------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user.")
        print("\n⚠️ Process interrupted by user.")
        sys.exit(0)
