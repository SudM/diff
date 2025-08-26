import pandas as pd
import json
import re
from .utils import logger, safe_load_json


def normalize_tenant_name(name: str) -> str:
    """Normalize tenant names to human-friendly form with WL expansion, hardcoded mappings, and heuristics."""
    if not name:
        return name

    lower_name = name.lower()

    # --- Hardcoded special mappings ---
    special_map = {
        "wlopenbanking": "White Label Open Banking",
        "wlexternaloperator": "White Label External Operator",
        "ibprospect": "IB Prospect",
        "opendata": "Open Data",
    }
    if lower_name in special_map:
        return special_map[lower_name]

    # --- Generic WL expansion ---
    if lower_name.startswith("wl"):
        rest = name[2:]
        # Capitalize first letter and try to split if it's a compound
        return "White Label " + generic_split(rest)

    # --- Generic beautifier ---
    return generic_split(name)


def generic_split(word: str) -> str:
    """
    Try to split compressed single words into more meaningful tokens.
    Rules:
      - Split before uppercase letters
      - Split known substrings (ib, open, data, bank, prospect, external, operator)
      - Capitalize each token
    """
    lower_word = word.lower()

    # Dictionary-based splitting
    tokens = []
    known_tokens = ["ib", "open", "data", "bank", "prospect", "external", "operator"]
    i = 0
    while i < len(lower_word):
        matched = False
        for t in sorted(known_tokens, key=len, reverse=True):
            if lower_word.startswith(t, i):
                tokens.append(t.capitalize())
                i += len(t)
                matched = True
                break
        if not matched:
            tokens.append(lower_word[i].upper())
            i += 1

    # Join tokens together with spaces
    return " ".join(tokens)


def build_regex_path_for_toggle(tenant_name, nested_names=None, version=None):
    """
    Build entitlement-style regex path:
    Check Permissions*/ *<tenant>*/ [*nested names*/] *Entitlement Check* [*version*]
    """
    nested_names = nested_names or []

    parts = []
    parts.append("Check Permissions*/")

    if tenant_name:
        parts.append(f"*{normalize_tenant_name(tenant_name)}*/")

    for n in nested_names:
        parts.append(f"*{n}*/")

    # Always end with entitlement check
    parts.append("*Entitlement Check*")

    if version:
        parts.append(f"*{version}*")

    return "".join(parts)


def collect_disabled_regex(env, action_val, node, path_parts, results, tenant=None):
    """
    Recursively collect regex paths + JSON snippets wherever isEnabled == False.
    - Tenant-driven path if tenant exists
    - Fallback to '*/ *Entitlement Check*' if action itself is disabled
    """
    if isinstance(node, dict):
        new_parts = path_parts[:]
        node_name = node.get("name")

        # If this node is a tenant
        if tenant is None and node_name:
            tenant = node_name
        else:
            # Only non-tenant names go into nested path parts
            if node_name:
                new_parts.append(node_name)

        # If disabled → record regex path + snippet
        if "isEnabled" in node and not node.get("isEnabled", True):
            version = node_name if node_name and node_name.lower().startswith("v") else None

            if tenant:
                regex_path = build_regex_path_for_toggle(
                    tenant,
                    new_parts[:-1] if version else new_parts,
                    version
                )
            else:
                # Action-level OFF fallback
                regex_path = "*/ *Entitlement Check*"
                if version:
                    regex_path += f"*{version}*"

            results.append({
                "Env": env,
                "Action": action_val,
                "Path": regex_path,
                "Snippet": json.dumps(node, indent=2)
            })

        # Recurse into children
        for k, v in node.items():
            if isinstance(v, (dict, list)):
                collect_disabled_regex(env, action_val, v, new_parts, results, tenant)

    elif isinstance(node, list):
        for item in node:
            collect_disabled_regex(env, action_val, item, path_parts, results, tenant)


def regex_from_path(regex_path: str) -> str:
    """
    Convert our RegexPath with * wildcards into a real regex pattern.
    Example: "*edge*/ *ACCOUNT*/ *Entitlement Check* *v2*" -> ".*edge.*/ .*ACCOUNT.*/ .*Entitlement Check.* .*v2.*"
    """
    pattern = regex_path.replace("*/", ".*")   # handle segment separators
    pattern = pattern.replace("*", ".*")       # handle remaining wildcards
    return pattern


def integrate_toggles(merged_df: pd.DataFrame, toggle_files: dict):
    """
    Integrate environment toggle states into merged_df and return toggle datasets.

    Args:
        merged_df (pd.DataFrame): Dataset after merge + propagation
        toggle_files (dict): Mapping {ENV: file_path or UploadedFile}

    Returns:
        (pd.DataFrame, pd.DataFrame, pd.DataFrame):
            - updated merged_df
            - df_toggles (all OFF toggle records from files, with RegexPath + Snippet)
            - df_missing (toggle records that didn't match any row)
    """
    if not toggle_files:
        logger.info("No toggle files provided, skipping toggle integration.")
        return merged_df, pd.DataFrame(columns=["Env", "Action", "Path", "Snippet"]), pd.DataFrame(columns=["Env", "Action", "Path", "Snippet"])

    all_results = []

    # Traverse each toggle file
    for env, path in toggle_files.items():
        try:
            tdata = safe_load_json(path)
        except Exception as e:
            logger.warning(f"Skipping {env} ({path}): {e}")
            continue

        for cp in tdata.get("toggles", {}).get("checkPermissions", []):
            action_val = cp.get("action", "")

            # If action itself disabled (and no tenants), handle it
            if "isEnabled" in cp and not cp.get("isEnabled", True) and not cp.get("tenants"):
                all_results.append({
                    "Env": env,
                    "Action": action_val,
                    "Path": "*/ *Entitlement Check*",
                    "Snippet": json.dumps(cp, indent=2)
                })

            # Otherwise traverse tenants
            for tenant in cp.get("tenants", []):
                collect_disabled_regex(env, action_val, tenant, [], all_results)

    df_toggles = pd.DataFrame(all_results)

    # Ensure env columns exist and default to blank (no ONs)
    for env in toggle_files.keys():
        if env not in merged_df.columns:
            merged_df[env] = ""

    if df_toggles.empty:
        logger.info("No OFF toggle records found in provided files.")
        return merged_df, df_toggles, pd.DataFrame(columns=df_toggles.columns)

    missing_records = []

    # Apply OFF values
    for _, trow in df_toggles.iterrows():
        env = trow["Env"]
        action_val = trow["Action"]
        regex_path = str(trow["Path"])

        # Convert regex path
        pattern = regex_from_path(regex_path)

        # Build mask
        try:
            mask = merged_df["Value_action"].eq(action_val) & \
                   merged_df["Policy FullPath"].str.contains(pattern, case=False, na=False, regex=True)
        except re.error as e:
            logger.error(f"Invalid regex built from {regex_path}: {e}")
            continue

        if mask.any():
            merged_df.loc[mask, env] = "OFF"
        else:
            # no matching row found -> collect as missing
            missing_records.append(trow.to_dict())

    df_missing = pd.DataFrame(missing_records, columns=df_toggles.columns)

    applied_count = len(df_toggles) - len(df_missing)
    logger.info(
        f"Toggles integrated: {applied_count} applied, {len(df_missing)} unmatched."
    )

    # Preview unmatched in logs
    if not df_missing.empty:
        preview = df_missing.head(5).to_dict(orient="records")
        logger.warning(f"First {len(preview)} unmatched toggle(s):")
        for row in preview:
            logger.warning(f"  Env={row['Env']} | Action={row['Action']} | Path={row['Path']}")

    return merged_df, df_toggles, df_missing

