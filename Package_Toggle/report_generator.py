from parsers import parse_deployment_package, collect_links_from_json
from html_utils import build_html
import json
from typing import List, Tuple, Dict, Any
from pathlib import Path

def index_check_permissions(toggle_obj: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    cp = (toggle_obj.get("toggles") or {}).get("checkPermissions") or []
    return {entry.get("action"): entry for entry in cp if entry.get("action")}

def expand_rows_for_action(action: str, entry: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    if not entry:
        return [("", "", "ON")]
    a_en = entry.get("isEnabled", True)
    tenants = entry.get("tenants") or []
    if not tenants:
        return [("", "", "OFF" if a_en is False else "ON")]
    rows = []
    for t in tenants:
        t_name = t.get("name", "")
        t_en = t.get("isEnabled", True)
        versions = t.get("versions") or []
        if not versions:
            rows.append((t_name, "", "OFF" if (not a_en or not t_en) else "ON"))
        else:
            for v in versions:
                v_name = v.get("name", "")
                v_en = v.get("isEnabled", True)
                status = "OFF" if (not a_en or not t_en or not v_en) else "ON"
                rows.append((t_name, v_name, status))
    return rows

def status_for(env_index, action, tenant, version) -> str:
    entry = env_index.get(action)
    if not entry:
        return "ON"
    if entry.get("isEnabled", True) is False:
        return "OFF"
    tenants = entry.get("tenants") or []
    if tenant == "":
        return "ON" if any(
            t.get("isEnabled", True) and any(v.get("isEnabled", True) for v in (t.get("versions") or []))
            for t in tenants
        ) else "OFF"
    t = next((tt for tt in tenants if tt.get("name") == tenant), None)
    if not t:
        return "ON"
    if t.get("isEnabled", True) is False:
        return "OFF"
    versions = t.get("versions") or []
    if version == "":
        return "ON" if any(v.get("isEnabled", True) for v in versions) else "OFF"
    v = next((vv for vv in versions if vv.get("name") == version), None)
    return "OFF" if (v and v.get("isEnabled", True) is False) else "ON"

def generate_toggle_report(dp_path: Path, toggle_files: List[Tuple[str, Path]], output_path: Path):
    dp_parsed = parse_deployment_package(dp_path)
    if dp_parsed["format"] not in ("json", "zip-json"):
        raise ValueError("Unsupported deployment package format.")

    links = collect_links_from_json(dp_parsed["json"])
    env_names = []
    env_indexes = {}

    for env_name, file_path in toggle_files:
        file_path = Path(file_path)
        toggle_obj = json.loads(file_path.read_text(encoding="utf-8"))
        env_indexes[env_name] = index_check_permissions(toggle_obj)
        env_names.append(env_name)

    row_keys = set()
    actions_in_dp = sorted({l.get("action") for l in links if l.get("action")})
    for action in actions_in_dp:
        combos = set()
        for env in env_names:
            entry = env_indexes[env].get(action)
            for (tenant, version, _status) in expand_rows_for_action(action, entry):
                combos.add((tenant or "", version or ""))
        if not combos:
            combos.add(("", ""))
        action_links = [l for l in links if l.get("action") == action] or [{"policySetName":"","policySetId":"","policyName":"","policyId":""}]
        for (tenant, version) in combos:
            for link in action_links:
                row_keys.add((
                    action,
                    tenant, version,
                    str(link.get("policySetName","") or ""),
                    str(link.get("policySetId","") or ""),
                    str(link.get("policyName","") or ""),
                    str(link.get("policyId","") or "")
                ))

    rows = []
    for (action, tenant, version, ps_name, ps_id, p_name, p_id) in sorted(row_keys):
        env_map = {env: status_for(env_indexes[env], action, tenant, version) for env in env_names}
        rows.append({
            "Action": action, "Tenant": tenant, "Version": version, **env_map
        })

    dp_actions_set = set(actions_in_dp)
    missing_by_env = {env: sorted(set(env_indexes[env].keys()) - dp_actions_set) for env in env_names}
    html = build_html(rows, missing_by_env, env_names)
    output_path.write_text(html, encoding="utf-8")