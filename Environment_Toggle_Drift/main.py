import argparse
from .utils import safe_load_json, logger
from .policy_tree import build_policy_tree
from .actions import extract_actions
from .targeting import extract_targeting
from .merge import merge_datasets, propagate_conditions, filter_to_entitlement_checks
from .toggles import integrate_toggles
from .exporters import export_to_excel, export_environment_drift_html


def main():
    parser = argparse.ArgumentParser(description="Policy Toggle Environment Drift Pipeline")
    parser.add_argument(
        "--deployment", "-d",
        required=True,
        help="Path to the deployment package (.deploymentpackage)"
    )
    parser.add_argument(
        "--toggle", "-t",
        action="append",
        help="Toggle file mapping in format ENV=PATH (e.g. SIT1=Toggle1.json). "
             "Can be passed multiple times."
    )
    parser.add_argument(
        "--output", "-o",
        default="All_Datasets_Patched.xlsx",
        help="Output Excel filename (default: All_Datasets_Patched.xlsx)"
    )
    parser.add_argument(
        "--html", "-H",
        default="Environment Drift.html",
        help="Output HTML drift report filename (default: Environment Drift.html)"
    )

    args = parser.parse_args()

    # ---------------- Load Deployment Package ----------------
    try:
        data = safe_load_json(args.deployment)
    except Exception as e:
        logger.error(f"Cannot load deployment package: {e}")
        return

    # ---------------- Build datasets ----------------
    condition_defs = [d for d in data if d.get("class") == "ConditionDefinition"]
    attr_defs = {o["id"]: o for o in data if o.get("class") == "AttributeDefinition"}
    id_lookup = {d["id"]: d for d in data if "id" in d}

    df_policy_tree = build_policy_tree(data)
    df_action = extract_actions(condition_defs, id_lookup, attr_defs)
    df_policy_targeting = extract_targeting(condition_defs, id_lookup)

    if df_policy_tree.empty:
        logger.error("Policy tree is empty. Stopping pipeline.")
        return

    # ... inside main() after merging:
    merged_df = merge_datasets(df_policy_tree, df_action, df_policy_targeting)
    if merged_df.empty:
        logger.error("Merged dataset is empty. Stopping pipeline.")
        return

    # 1) Propagate first
    merged_df = propagate_conditions(merged_df)

    # 2) THEN filter to entitlement checks (and exclude children)
    merged_df = filter_to_entitlement_checks(merged_df)
    if merged_df.empty:
        logger.warning("After entitlement filter, no rows remain.")

    # ---------------- Toggle files ----------------
    toggle_files = {}
    if args.toggle:
        for toggle_arg in args.toggle:
            if "=" not in toggle_arg:
                logger.warning(f"Invalid toggle argument (ignored): {toggle_arg}")
                continue
            env, path = toggle_arg.split("=", 1)
            toggle_files[env.strip()] = path.strip()

    merged_df, df_toggles, df_missing = integrate_toggles(merged_df, toggle_files)

    # ---------------- Export ----------------
    export_to_excel({
        "Policy_Tree": df_policy_tree,
        "Action": df_action,
        "Policy_Targeting": df_policy_targeting,
        "Merged": merged_df,
        "Toggles": df_toggles,
        "Missing Toggles": df_missing
    }, filename=args.output)

    export_environment_drift_html(merged_df, output_file=args.html)

    # ---------------- Summary Log ----------------
    logger.info("========== PIPELINE SUMMARY ==========")
    logger.info(f"Deployment package: {args.deployment}")
    logger.info(f"Policy tree records:   {len(df_policy_tree)}")
    logger.info(f"Action records:        {len(df_action)}")
    logger.info(f"Targeting records:     {len(df_policy_targeting)}")
    logger.info(f"Merged records:        {len(merged_df)}")
    logger.info(f"Toggles OFF records:   {len(df_toggles)}")
    logger.info(f"Toggle environments:   {', '.join(toggle_files.keys()) if toggle_files else 'None'}")
    logger.info(f"Excel output:          {args.output}")
    logger.info(f"HTML report:           {args.html}")
    logger.info("=====================================")


if __name__ == "__main__":
    main()
