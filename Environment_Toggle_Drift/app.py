import streamlit as st
import pandas as pd
from io import BytesIO
import sys, os

# Ensure parent folder is on sys.path so package imports resolve
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import pipeline functions
from Environment_Toggle_Drift.utils import safe_load_json
from Environment_Toggle_Drift.policy_tree import build_policy_tree
from Environment_Toggle_Drift.actions import extract_actions
from Environment_Toggle_Drift.targeting import extract_targeting
from Environment_Toggle_Drift.merge import (
    merge_datasets,
    propagate_conditions,
    filter_to_entitlement_checks,
)
from Environment_Toggle_Drift.toggles import integrate_toggles


st.set_page_config(page_title="Environment Drift Explorer", layout="wide")
st.title("🔎 Environment Drift Explorer")

# ---------------- Upload Inputs ----------------
deployment_file = st.file_uploader(
    "Upload Deployment Package (.deploymentpackage)", 
    type=["deploymentpackage", "json"]
)
toggle_files = st.file_uploader(
    "Upload Toggle Files (JSON)", 
    type=["json"], 
    accept_multiple_files=True
)

if toggle_files and len(toggle_files) > 10:
    st.error("⚠️ You can upload a maximum of 10 toggle files at once.")
    st.stop()


toggle_map = {}
missing_envs = []

# Require all inputs before proceeding
if deployment_file and toggle_files:
    st.subheader("🔧 Map Toggle Files to Environments")

    for f in toggle_files:
        col1, col2 = st.columns([2, 1])
        with col1:
            st.write(f"📄 {f.name}")
        with col2:
            env = st.text_input(f"Environment for {f.name}", value="", key=f.name)

        if env.strip():
            toggle_map[env.strip().upper()] = f
        else:
            missing_envs.append(f.name)

    if missing_envs:
        st.warning(f"⚠️ Please provide environment names for: {', '.join(missing_envs)}")
        st.stop()   # stop execution until user fills all envs
elif not deployment_file:
    st.info("👆 Upload a deployment package to begin")
    st.stop()
elif not toggle_files:
    st.info("👆 Upload one or more toggle files to continue")
    st.stop()

# ---------------- Build Pipeline ----------------
data = safe_load_json(deployment_file)

condition_defs = [d for d in data if d.get("class") == "ConditionDefinition"]
attr_defs = {o["id"]: o for o in data if o.get("class") == "AttributeDefinition"}
id_lookup = {d["id"]: d for d in data if "id" in d}

# Keep full policy tree (must include Condition ID for merge)
df_policy_tree = build_policy_tree(data)
df_action = extract_actions(condition_defs, id_lookup, attr_defs)
df_policy_targeting = extract_targeting(condition_defs, id_lookup)

# Merge + process
merged_df = merge_datasets(df_policy_tree, df_action, df_policy_targeting)
merged_df = propagate_conditions(merged_df)
merged_df = filter_to_entitlement_checks(merged_df)

# Toggle integration
merged_df, df_toggles, df_missing, toggle_drift_df, policies_with_targets_df = integrate_toggles(merged_df, toggle_map)


# Rename Value_action → Action for display
if "Value_action" in merged_df.columns:
    merged_df = merged_df.rename(columns={"Value_action": "Action"})

# Drift view = core + envs
env_cols = [c for c in merged_df.columns if c.upper().startswith(("SIT", "UAT", "PROD"))]
drift_df = merged_df[["Position", "ID", "Policy FullPath", "Action"] + env_cols]

# Define tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
    ["🌀 Toggle Drift", "📊 Policies with Targets", "📂 Policy Tree", "⚡ Actions", "🚦 Toggles", "⚠️ Missing Toggles"]
)

# --- Toggle Drift tab with row coloring ---
with tab1:
    st.subheader("Toggle Drift")

    def drift_styler(row):
        env_cols = [c for c in toggle_drift_df.columns if c.upper().startswith(("SIT", "UAT", "PROD"))]
        env_values = [str(row[c]).upper() for c in env_cols if pd.notna(row[c]) and str(row[c]).strip() != ""]
        if not env_values:
            return [""] * len(row)
        if all(v == "OFF" for v in env_values):
            return ['background-color: #FFCCCC'] * len(row)   # light red
        elif len(set(env_values)) > 1:
            return ['background-color: #FFBF00'] * len(row)   # amber drift
        return [""] * len(row)

    if not toggle_drift_df.empty:
        styled = toggle_drift_df.style.apply(drift_styler, axis=1)
        st.dataframe(styled, use_container_width=True)
    else:
        st.info("✅ No toggle drift found!")

# --- Policies with Targets tab (plain) ---
with tab2:
    st.subheader("Policies with Targets")
    if not policies_with_targets_df.empty:
        st.dataframe(policies_with_targets_df, use_container_width=True)
    else:
        st.info("✅ All policies were covered by toggles.")

with tab3:
    st.subheader("Policy Tree")
    st.dataframe(df_policy_tree[["Position", "ID", "Policy FullPath"]], use_container_width=True)

with tab4:
    st.subheader("Actions")
    st.dataframe(df_action, use_container_width=True)

with tab5:
    st.subheader("Applied Toggles")
    st.dataframe(df_toggles, use_container_width=True)

with tab6:
    st.subheader("Missing Toggles")
    if df_missing.empty:
        st.info("✅ No missing toggles found!")
    else:
        st.dataframe(df_missing, use_container_width=True)
# ---------------- Downloads ----------------
st.subheader("📥 Download Results")

def to_excel(dfs: dict):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for sheet, df in dfs.items():
            df.to_excel(writer, sheet_name=sheet, index=False)
    return output.getvalue()

excel_data = to_excel({
    "Toggle Drift": toggle_drift_df,
    "Policies with Targets": policies_with_targets_df,
    "Policy_Tree": df_policy_tree,
    "Action": df_action,
    "Toggles": df_toggles,
    "Missing Toggles": df_missing,
})

st.download_button("💾 Download Excel", data=excel_data, file_name="Environment_Drift.xlsx")

# Silent cleanup
try:
    if hasattr(deployment_file, "name") and os.path.exists(deployment_file.name):
        os.remove(deployment_file.name)
    for f in toggle_files or []:
        if hasattr(f, "name") and os.path.exists(f.name):
            os.remove(f.name)
except:
    pass
