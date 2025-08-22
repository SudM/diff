#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, io, json, os, re, zipfile
from collections import defaultdict
from typing import Dict, List, Optional
import pandas as pd

# ============================================================
# Config / constants (ALL previously agreed conditions preserved)
# ============================================================

TENANCY_KEYWORDS = {"edge","ib","nabc","nabone","opendata","whitelabel","wl"}
DROP_COLS = [
    "Action Condition Path",
    "source_package",
    "Matched Type",
    "Tenancy_From_USER",
    "User Tenancy Condition Path",
    "Tenancy",
]

# Abbreviations used only when deriving from .ACTION.<...> (we keep underscores within segments)
AC_SEG_MAP = {
    "account_management": "acctmgmt",
    "accountmanagement": "acctmgmt",
    "acct_mgmt": "acctmgmt",
    "acctmgmt": "acctmgmt",
    "account_info": "acctinfo",
    "account_information": "acctinfo",
    "accountinfo": "acctinfo",
    "acct_info": "acctinfo",
    "acctinfo": "acctinfo",
}

# The ONLY allowed / final action constants:
ACTION_WHITELIST = {
    "action:acctmgmt/acctinfo/view",
    "action:acctmgmt/acctinfo/view_balance",
    "action:acctmgmt/acctinfo/view_details",
    "action:party/arrangement/view",
}

OUTPUT_COL_ORDER = [
    "position_path","originId","type","name",
    "full_path_from_root","full_path_ids_from_root",
    "ConditionID","Linked POLICY.Target","Linked PolicySet/Policy","Matched OriginID",
    "Action Constant","Tenancy_Mapped","SubTenancy_Mapped","subtenancy_subtype",
    "Version","toggle_path"
]

# ==========================
# Small utilities
# ==========================

def esc(s):
    return "" if s is None else str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def write_html_table(df: pd.DataFrame, title: str, out_html: str):
    cols = list(df.columns)
    html = [f"<!doctype html><html><head><meta charset='utf-8'><title>{esc(title)}</title>",
            "<style>body{font-family:Arial,sans-serif;margin:20px}table{border-collapse:collapse;width:100%}",
            "th,td{border:1px solid #ccc;padding:6px 10px;text-align:left}th{background:#f2f2f2;position:sticky;top:0}",
            "tr:nth-child(even){background:#fafafa}code{font-family:Consolas,monospace}</style></head><body>",
            f"<h2>{esc(title)}</h2><table><thead><tr>",
            "".join(f"<th>{esc(c)}</th>" for c in cols),
            "</tr></thead><tbody>"]
    for _, r in df.iterrows():
        html.append("<tr>")
        for c in cols:
            sval = "" if pd.isna(r[c]) else str(r[c])
            html.append(f"<td>{esc(sval) if not c.lower().endswith('id') else '<code>'+esc(sval)+'</code>'}</td>")
        html.append("</tr>")
    html.append("</tbody></table></body></html>")
    with open(out_html, "w", encoding="utf-8") as f:
        f.write("\n".join(html))

def load_json_or_zipped_json(path: str):
    with open(path, "rb") as f:
        head = f.read(4); f.seek(0); blob = f.read()
    if head == b"PK\x03\x04":
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            js = [n for n in zf.namelist() if n.lower().endswith(".json")]
            if not js: return []
            with zf.open(js[0]) as jf:
                return json.loads(jf.read().decode("utf-8", "ignore"))
    for enc in ("utf-8","utf-8-sig","utf-16","latin-1"):
        try:
            return json.loads(blob.decode(enc))
        except Exception:
            pass
    raise ValueError(f"Failed to parse {path}")

def iter_nodes(data):
    if isinstance(data, list):
        for o in data:
            if isinstance(o, dict): yield o
    elif isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                for o in v:
                    if isinstance(o, dict): yield o
        yield data

def index_by_id(objs: List[dict]) -> Dict[str, dict]:
    return {o.get("id"): o for o in objs if o.get("id")}

def get_name(o: dict) -> Optional[str]:
    for k in ("name","qualifiedName","fullName","path","label"):
        v = o.get(k)
        if isinstance(v,str): return v
    return None

# ==========================
# PolicyPath (PolicySet-only chain from Root; attach Policies as children)
# ==========================

def build_metadata_maps(objs: List[dict]):
    md = [o for o in objs if o.get("class")=="Metadata"]
    md_by_origin = {o["originId"]: o for o in md if "originId" in o}
    policysets = {o["originId"] for o in md if o.get("originType")=="PolicySet"}
    policies   = {o["originId"] for o in md if o.get("originType")=="Policy"}
    return md_by_origin, policysets, policies

def resolve_policyset_graph(objs, id_index, policysets):
    parent_of, children = {}, defaultdict(set)
    for cdn in (o for o in objs if o.get("class")=="CombinedDecisionNode"):
        owner = cdn.get("originLink")
        if not owner or owner not in policysets: continue
        for in_id in (cdn.get("inputNodes") or []):
            node = id_index.get(in_id, {})
            child_meta = None
            if node.get("class")=="StatementNode":
                child_meta = node.get("metadataId")
                if not child_meta:
                    tgt_id = node.get("inputNode")
                    tgt = id_index.get(tgt_id, {})
                    if tgt.get("class")=="TargetMatchNode":
                        child_meta = tgt.get("metadataId")
            elif node.get("class")=="TargetMatchNode":
                child_meta = node.get("metadataId")
            if child_meta and child_meta in policysets and child_meta!=owner:
                parent_of[child_meta] = owner
                children[owner].add(child_meta)
    return parent_of, children

def find_root_policyset(policysets, md_by_origin, parent_of):
    roots = [pid for pid in policysets if pid not in parent_of]
    for r in roots:
        md = md_by_origin.get(r) or {}
        if md.get("originType")=="PolicySet" and str(md.get("name","")).strip().lower()=="root":
            return r
    return roots[0] if roots else None

def appearance_order(objs, policysets):
    order = {}
    for i,o in enumerate(objs):
        if o.get("class")=="Metadata":
            oid=o.get("originId")
            if oid in policysets and oid not in order:
                order[oid]=i
    return order

def children_sorted(pid, children, order):
    kids = list(children.get(pid, set()))
    kids.sort(key=lambda k: order.get(k, 10**9))
    return kids

def traverse_policysets(root, md_by_origin, children, order) -> List[dict]:
    rows, visited = [], set()
    def dfs(ps_id, path_ids, path_labels, pos_list):
        if ps_id in visited: return
        visited.add(ps_id)
        md = md_by_origin.get(ps_id, {})
        label = f"PolicySet:{md.get('name')}"
        level = len(path_ids)
        my_pos = 1 if level==0 else children_sorted(path_ids[-1], children, order).index(ps_id)+1
        new_pos = pos_list+[my_pos]
        new_ids = path_ids+[ps_id]
        new_labels = path_labels+[label]
        rows.append({
            "position_path": ".".join(map(str,new_pos)),
            "originId": ps_id,
            "type": "PolicySet",
            "name": md.get("name"),
            "full_path_from_root": " / ".join(new_labels),
            "full_path_ids_from_root": " / ".join(new_ids),
        })
        for child in children_sorted(ps_id, children, order):
            dfs(child, new_ids, new_labels, new_pos)
    dfs(root, [], [], [])
    return rows

def attach_policies(objs, id_index, md_by_origin, policysets, policies, rows):
    policy_parent = {}
    for cdn in (o for o in objs if o.get("class")=="CombinedDecisionNode"):
        owner = cdn.get("originLink")
        if not owner or owner not in policysets: continue
        for in_id in (cdn.get("inputNodes") or []):
            node = id_index.get(in_id, {})
            child_meta = None
            if node.get("class")=="StatementNode":
                child_meta = node.get("metadataId")
                if not child_meta:
                    tgt_id = node.get("inputNode")
                    tgt = id_index.get(tgt_id, {})
                    if tgt.get("class")=="TargetMatchNode":
                        child_meta = tgt.get("metadataId")
            elif node.get("class")=="TargetMatchNode":
                child_meta = node.get("metadataId")
            if child_meta in policies:
                policy_parent[child_meta] = owner
    path_by_ps = {r["originId"]: r["full_path_from_root"] for r in rows}
    pos_by_ps  = {r["originId"]: r["position_path"] for r in rows}
    sib_count  = defaultdict(int)
    for pol_id in policies:
        parent = policy_parent.get(pol_id)
        if not parent or parent not in path_by_ps: continue
        md = md_by_origin.get(pol_id, {})
        sib_count[parent] += 1
        pos = f"{pos_by_ps[parent]}.{sib_count[parent]}"
        rows.append({
            "position_path": pos,
            "originId": pol_id,
            "type": "Policy",
            "name": md.get("name"),
            "full_path_from_root": path_by_ps[parent],  # policyset chain only
            "full_path_ids_from_root": path_by_ps[parent],
        })
    def path_key(s): return tuple(int(x) for x in str(s).split("."))
    rows.sort(key=lambda r: path_key(r["position_path"]))
    return rows

def build_policypath_from_package(pkg_path: str) -> pd.DataFrame:
    objs = list(iter_nodes(load_json_or_zipped_json(pkg_path)))
    id_index = index_by_id(objs)
    md_by_origin, policysets, policies = build_metadata_maps(objs)
    parent_of, children = resolve_policyset_graph(objs, id_index, policysets)
    root = find_root_policyset(policysets, md_by_origin, parent_of)
    if not root: raise SystemExit("Could not identify Root PolicySet.")
    order = appearance_order(objs, policysets)
    rows = traverse_policysets(root, md_by_origin, children, order)
    rows = attach_policies(objs, id_index, md_by_origin, policysets, policies, rows)
    return pd.DataFrame(rows, columns=["position_path","originId","type","name","full_path_from_root","full_path_ids_from_root"])

# ==========================
# Actionlink (strict link + ALL actions + Version)
# ==========================

def build_indices(objs):
    by_id, fwd, rev = {}, defaultdict(set), defaultdict(set)
    for o in objs:
        oid = o.get("id")
        if oid: by_id[oid]=o
    singles = ("inputNode","guardNode","lhsInputNode","rhsInputNode","condition","definitionId")
    for o in objs:
        oid = o.get("id")
        if not oid: continue
        for k in singles:
            v = o.get(k)
            if isinstance(v, str) and v in by_id:
                fwd[oid].add(v); rev[v].add(oid)
        for v in (o.get("inputNodes") or []):
            if isinstance(v, str) and v in by_id:
                fwd[oid].add(v); rev[v].add(oid)
    return by_id, fwd, rev

def strictly_match_metadata(cond_id, by_id, fwd, rev, objs):
    meta = {"PolicySet":{}, "Policy":{}}
    for o in objs:
        if o.get("class")=="Metadata" and o.get("originId") and o.get("originType") in ("PolicySet","Policy"):
            meta[o["originType"]][o["originId"]] = o
    crs = [nid for nid in rev.get(cond_id,set()) if by_id.get(nid,{}).get("class")=="ConditionReferenceNode"]
    if not crs: return []
    bools=set()
    for cr in crs:
        bools |= {nid for nid in rev.get(cr,set()) if by_id.get(nid,{}).get("class")=="BooleanLogicNode"}
    combs=set()
    for b in bools:
        for nid in rev.get(b,set()):
            n = by_id.get(nid,{})
            if n.get("class")=="CombinedDecisionNode" and n.get("guardNode")==b:
                combs.add(nid)
    statements=set()
    for c in combs:
        statements |= {nid for nid in rev.get(c,set())
                       if by_id.get(nid,{}).get("class")=="StatementNode" and by_id[nid].get("inputNode")==c}
    targets=set()
    for s in statements:
        targets |= {nid for nid in rev.get(s,set())
                    if by_id.get(nid,{}).get("class")=="TargetMatchNode" and by_id[nid].get("inputNode")==s}
    holders = [by_id[c] for c in combs] + [by_id[s] for s in statements] + [by_id[t] for t in targets]
    out, seen=set(), set()
    for node in holders:
        if node.get("class")=="CombinedDecisionNode":
            v = node.get("originLink")
            if isinstance(v,str):
                for t in ("PolicySet","Policy"):
                    m = meta[t].get(v)
                    if m and v not in seen:
                        out.add((get_name(m), m.get("originType"), v)); seen.add(v)
        v = node.get("metadataId")
        if isinstance(v,str):
            for t in ("PolicySet","Policy"):
                m = meta[t].get(v)
                if m and v not in seen:
                    out.add((get_name(m), m.get("originType"), v)); seen.add(v)
    return list(out)

def const_str(node):
    for k in ("constant","value"):
        v = node.get(k)
        if isinstance(v,str): return v
    return None

def find_action_inside_action_condition(action_cd, by_id):
    root = action_cd.get("condition")
    if not isinstance(root,str): return [], get_name(action_cd) or ""
    stack, seen = [root], set(); found=set()
    while stack:
        nid = stack.pop()
        if nid in seen: continue
        seen.add(nid)
        node = by_id.get(nid)
        if not node: continue
        if node.get("class")=="ComparisonNode":
            for side in ("rhsInputNode","lhsInputNode"):
                sid = node.get(side)
                cnode = by_id.get(sid)
                if cnode and cnode.get("class")=="ConstantNode":
                    val = const_str(cnode)
                    if isinstance(val,str) and val.startswith("action:"):
                        found.add(val.strip())
        if node.get("class")=="ConstantNode":
            val = const_str(node)
            if isinstance(val,str) and val.startswith("action:"):
                found.add(val.strip())
        for k in ("inputNode","guardNode","lhsInputNode","rhsInputNode","condition"):
            v = node.get(k)
            if isinstance(v,str): stack.append(v)
        for v in (node.get("inputNodes") or []):
            if isinstance(v,str): stack.append(v)
    return sorted(found), get_name(action_cd) or ""

def find_action_for_policy_condition(policy_cd, by_id):
    root = policy_cd.get("condition")
    if not isinstance(root,str): return [], ""
    stack, seen = [root], set(); found=set(); last_path=""
    while stack:
        nid = stack.pop()
        if nid in seen: continue
        seen.add(nid)
        node = by_id.get(nid)
        if not node: continue
        if node.get("class")=="ConditionReferenceNode":
            def_id = node.get("definitionId")
            target = by_id.get(def_id)
            if target and target.get("class")=="ConditionDefinition":
                nm = get_name(target) or ""
                if "ACTION" in nm:
                    consts, p = find_action_inside_action_condition(target, by_id)
                    for c in consts: found.add(c)
                    if p: last_path = p
                inner = target.get("condition")
                if isinstance(inner,str): stack.append(inner)
        for k in ("inputNode","guardNode","lhsInputNode","rhsInputNode","condition"):
            v = node.get(k)
            if isinstance(v,str): stack.append(v)
        for v in (node.get("inputNodes") or []):
            if isinstance(v,str): stack.append(v)
    return sorted(found), last_path

def extract_version_from_target(name: str) -> str:
    if not isinstance(name, str): return ""
    m = re.search(r"(?i)\.VERSIONS\.([^.]+)", name)
    return (m.group(1) if m else "")

def normalize_linked_target(name: str) -> str:
    if not isinstance(name, str): return ""
    return re.sub(r"(?i)\.is_?enabled$", "", name)

def map_segments_to_canonical_after_action(path_after_action: str) -> str:
    # Split on '.' only, keep underscores within a segment, lower-case, map by AC_SEG_MAP
    segs = [s.strip().lower() for s in (path_after_action or "").split(".") if s.strip()]
    mapped = [AC_SEG_MAP.get(s, s) for s in segs]
    return "/".join(mapped)

# --- NEW: normalize any found/derived action into the whitelist of four values ---
def normalize_to_whitelist(ac_value: str, cd_name: str = "") -> str:
    """
    Take any 'action:...' or hint from ConditionDefinition name and force it to one of the four allowed actions.
    If ambiguous or not matched, return "".
    """
    candidates = set()

    # 1) If we already have action:..., try to normalize segments first
    if isinstance(ac_value, str) and ac_value.strip().lower().startswith("action:"):
        payload = ac_value.split(":", 1)[1].strip().lower()
        # allow dots or slashes or underscores in source; produce canonical slash + underscores preserved in last segments
        payload = payload.replace("\\", "/").replace(" ", "")
        payload = re.sub(r"/{2,}", "/", payload)
        # If payload contains dots, prefer treating as ACTION path (map segments)
        if "." in payload:
            payload = map_segments_to_canonical_after_action(payload)
        # Quick synonyms
        payload = payload.replace("account/management", "acctmgmt").replace("accountmanagement", "acctmgmt")
        payload = payload.replace("account/info", "acctinfo").replace("accountinfo", "acctinfo")
        # Accept as candidate
        candidates.add(f"action:{payload}")

    # 2) Also derive from cd_name (fallback) after .ACTION.
    if isinstance(cd_name, str) and ".ACTION." in cd_name:
        m = re.search(r"(?i)\.ACTION\.([^.]+(?:\.[^.]+)*)", cd_name)
        if m:
            suffix = m.group(1)
            mapped = map_segments_to_canonical_after_action(suffix)
            if mapped:
                candidates.add(f"action:{mapped}")

    # Try simple harmonizations on candidates (map endings)
    fixed = set()
    for c in candidates:
        p = c
        p = p.replace("account_management", "acctmgmt").replace("accountmanagement", "acctmgmt")
        p = p.replace("account_info", "acctinfo").replace("accountinformation", "acctinfo").replace("accountinfo", "acctinfo")
        # normalize ending variants
        p = p.replace("view-balance", "view_balance").replace("viewbalance", "view_balance")
        p = p.replace("view-details", "view_details").replace("viewdetails", "view_details")
        fixed.add(p)

    # Match to whitelist by exact match OR loose patterns to land on the four
    for c in list(fixed) + list(candidates):
        base = c.lower()
        # direct exact
        if base in ACTION_WHITELIST:
            return base
        # map 'acctmgmt/acctinfo/view...' endings
        if base.startswith("action:acctmgmt/acctinfo/"):
            tail = base.split("action:acctmgmt/acctinfo/",1)[1]
            if tail.startswith("view_details"):
                return "action:acctmgmt/acctinfo/view_details"
            if tail.startswith("view_balance"):
                return "action:acctmgmt/acctinfo/view_balance"
            if tail.startswith("view"):
                return "action:acctmgmt/acctinfo/view"
        # party/arrangement/view family
        if "party" in base and "arrangement" in base and "view" in base:
            return "action:party/arrangement/view"

    # No confident match -> empty (so toggle_path won't build)
    return ""

def build_actionlink_from_package(pkg_path: str) -> pd.DataFrame:
    objs = list(iter_nodes(load_json_or_zipped_json(pkg_path)))
    by_id, fwd, rev = build_indices(objs)
    conds = [o for o in objs if o.get("class")=="ConditionDefinition"
             and isinstance(o.get("name"),str) and o.get("id")
             and ("CHECK_PERMISSIONS" in o["name"] and ".ACTION." in o["name"])]

    # Strict links to Policy/PolicySet
    rows_links=[]
    for cd in conds:
        cid, cname = cd["id"], get_name(cd) or ""
        for nm,tp,oid in strictly_match_metadata(cid, by_id, fwd, rev, objs):
            rows_links.append({"ConditionID": cid, "Linked POLICY.Target": cname,
                               "Linked PolicySet/Policy": nm or "", "Matched Type": tp or "",
                               "Matched OriginID": oid or ""})
    df_links = pd.DataFrame(rows_links).drop_duplicates(subset=["ConditionID","Matched OriginID"]).reset_index(drop=True)

    # Find ALL actions (prefer graph constants; fallback to name parsing)
    rows_actions=[]
    for cd in conds:
        consts, _ = find_action_for_policy_condition(cd, by_id)
        if not consts:
            lt = get_name(cd) or ""
            m = re.search(r"(?i)\.ACTION\.([^.]+(?:\.[^.]+)*)", lt)
            if m:
                suffix = m.group(1)
                mapped = map_segments_to_canonical_after_action(suffix)
                if mapped:
                    consts = [f"action:{mapped}"]
        # Normalize EVERY found/derived action into the allowed four
        if not consts:
            rows_actions.append({"ConditionID": cd["id"], "Linked POLICY.Target": get_name(cd), "Action Constant": ""})
        else:
            for ac in consts:
                ac_norm = normalize_to_whitelist(ac, get_name(cd) or "")
                rows_actions.append({"ConditionID": cd["id"], "Linked POLICY.Target": get_name(cd), "Action Constant": ac_norm})
    df_actions = pd.DataFrame(rows_actions).drop_duplicates(subset=["ConditionID","Action Constant"]).reset_index(drop=True)

    # Merge and add Version; normalize Linked POLICY.Target
    df = pd.merge(df_links, df_actions, on=["ConditionID","Linked POLICY.Target"], how="left")
    df["Linked POLICY.Target"] = df["Linked POLICY.Target"].astype(str).apply(normalize_linked_target)
    df["Version"] = df["Linked POLICY.Target"].apply(extract_version_from_target)

    # Ensure only whitelisted actions remain; others -> ""
    def enforce_whitelist(v: str) -> str:
        v = (v or "").strip().lower()
        return v if v in ACTION_WHITELIST else ""
    df["Action Constant"] = df["Action Constant"].apply(enforce_whitelist)

    return df[["ConditionID","Linked POLICY.Target","Linked PolicySet/Policy","Matched OriginID","Action Constant","Version"]]

# ==========================
# Mapping / propagation rules
# ==========================

def trim_users_suffix(s: str) -> str:
    if not isinstance(s, str): return ""
    return re.sub(r"(?i)(?:\.?users?)$", "", s.strip())

def tenancy_from_path(path: str) -> str:
    if not isinstance(path, str): return ""
    p = path.lower()
    if ":white label" in p or ":whitelabel" in p: return "edge"
    if ":edge" in p: return "edge"
    if ":ib " in p or ":ib/" in p or p.endswith(":ib"): return "ib"
    if ":nabc" in p: return "nabc"
    if ":nabone" in p: return "nabone"
    if ":open data" in p or ":opendata" in p: return "opendata"
    return ""

def white_label_subtenancy(path: str) -> str:
    if not isinstance(path, str): return ""
    m = re.search(r":\s*White\s*Label([^/]+)", path, flags=re.IGNORECASE)
    if not m: return ""
    seg = m.group(1).strip().lower()
    seg = seg.split(" / ")[0]
    seg = seg.replace(" ", "")
    seg = trim_users_suffix(seg)
    seg = seg.replace("whitelabel", "wl")
    if not seg.startswith("wl"):
        seg = "wl" + seg
    return seg

def subtype_from_linked_target(target: str) -> str:
    if not isinstance(target, str): return ""
    m = re.search(r"(?i)TENANT\.(.*?)(?:\.VERSIONS\b|$)", target)
    if not m: return ""
    val = m.group(1).replace("_","").lower().strip()
    val = re.sub(r"(?i)\.is_?enabled$", "", val)
    val = trim_users_suffix(val)
    if any(k in val for k in TENANCY_KEYWORDS):
        return ""
    return val

def extract_version_from_target(name: str) -> str:
    if not isinstance(name, str): return ""
    m = re.search(r"(?i)\.VERSIONS\.([^.]+)", name)
    return (m.group(1) if m else "")

def path_key(s):
    try:
        return tuple(int(x) for x in str(s).split("."))
    except Exception:
        return (999999,)

def norm_action(v):
    return v.strip() if isinstance(v, str) and v.strip().lower().startswith("action:") else ""

def propagate_action_in_place(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(by="position_path", key=lambda c: c.map(path_key)).reset_index(drop=True)
    stack, out_vals = [], []
    for _, row in df.iterrows():
        depth = len(str(row["position_path"]).split("."))
        stack = (stack[: depth-1]) if depth > 1 else []
        parent_val = stack[-1] if stack else ""
        cur = norm_action(row.get("Action Constant",""))
        out = cur or parent_val
        out_vals.append(out)
        stack.append(out)
    df["Action Constant"] = out_vals
    return df

def build_toggle_path(row) -> str:
    ac = str(row.get("Action Constant","") or "").strip()
    if not ac:
        return ""
    parts = [
        ac,
        str(row.get("Tenancy_Mapped","") or "").strip(),
        str(row.get("SubTenancy_Mapped","") or "").strip(),
        str(row.get("subtenancy_subtype","") or "").strip(),
    ]
    ver = str(row.get("Version","") or "").strip()
    if ver:
        parts.append(ver)
    # EXACTLY no spaces around "~", skip empties
    return "~".join([p for p in parts if p])

# ==========================
# Orchestration
# ==========================

def run_from_package(pkg_path: str, out_dir: str):
    # Build datasets
    df_policy = build_policypath_from_package(pkg_path)
    df_action = build_actionlink_from_package(pkg_path)

    # Join
    df = df_policy.merge(df_action, left_on="originId", right_on="Matched OriginID", how="left")

    # Deduplicate by originId (keep first by position_path)
    if df["originId"].duplicated().any():
        df = df.sort_values(["position_path"]).drop_duplicates(subset=["originId"], keep="first")

    # Tenancy/Subtenancy/subtype
    df["Tenancy_Mapped"] = df["full_path_from_root"].apply(tenancy_from_path).apply(trim_users_suffix)
    df["SubTenancy_Mapped"] = df["full_path_from_root"].apply(white_label_subtenancy)
    if "Linked POLICY.Target" not in df.columns: df["Linked POLICY.Target"] = ""
    df["subtenancy_subtype"] = df["Linked POLICY.Target"].apply(subtype_from_linked_target)

    # Overwrite SubTenancy_Mapped with subtype if present and not arrangementtype.*
    mask_subtype = df["subtenancy_subtype"].astype(str).str.len() > 0
    mask_not_arr = ~df["subtenancy_subtype"].astype(str).str.startswith("arrangementtype")
    df.loc[mask_subtype & mask_not_arr, "SubTenancy_Mapped"] = df.loc[mask_subtype & mask_not_arr, "subtenancy_subtype"]

    # Never allow raw tenancy keywords as entire SubTenancy_Mapped
    df["SubTenancy_Mapped"] = df["SubTenancy_Mapped"].apply(
        lambda s: ("" if isinstance(s, str) and s.strip().lower() in {"edge","ib","nabc","nabone","opendata"} else s)
    )

    # Propagate Action Constant
    if "Action Constant" not in df.columns: df["Action Constant"] = ""
    df = propagate_action_in_place(df)

    # Drop columns per request (if present)
    for c in DROP_COLS:
        if c in df.columns:
            df.drop(columns=[c], inplace=True)

    # Ensure Version (from Linked POLICY.Target)
    df["Version"] = df["Linked POLICY.Target"].apply(extract_version_from_target)

    # No NaN
    df = df.fillna("")

    # toggle_path (only when AC exists), no spaces around "~"
    df["toggle_path"] = df.apply(build_toggle_path, axis=1)

    # Order columns (keep extras at end)
    ordered = [c for c in OUTPUT_COL_ORDER if c in df.columns] + [c for c in df.columns if c not in OUTPUT_COL_ORDER]
    df = df[ordered]

    # Save
    final_csv  = os.path.join(out_dir, "final_output.csv")
    final_html = os.path.join(out_dir, "final_output.html")
    df.to_csv(final_csv, index=False)
    write_html_table(df, "Final Output", final_html)
    print(f"Final CSV : {final_csv}\nFinal HTML: {final_html}")

def run_from_csvs(pp_csv: str, al_csv: str, out_dir: str):
    df_policy = pd.read_csv(pp_csv)
    df_action = pd.read_csv(al_csv)

    # Enforce whitelist on incoming actionlink too (safety)
    def enforce_whitelist(v: str) -> str:
        v = (str(v or "")).strip().lower()
        return v if v in ACTION_WHITELIST else ""
    if "Action Constant" in df_action.columns:
        df_action["Action Constant"] = df_action["Action Constant"].apply(enforce_whitelist)

    df = df_policy.merge(df_action, left_on="originId", right_on="Matched OriginID", how="left")

    if "position_path" in df.columns and df["originId"].duplicated().any():
        df = df.sort_values(["position_path"]).drop_duplicates(subset=["originId"], keep="first")

    df["Tenancy_Mapped"] = df["full_path_from_root"].apply(tenancy_from_path).apply(trim_users_suffix)
    df["SubTenancy_Mapped"] = df["full_path_from_root"].apply(white_label_subtenancy)
    if "Linked POLICY.Target" not in df.columns: df["Linked POLICY.Target"] = ""
    df["subtenancy_subtype"] = df["Linked POLICY.Target"].apply(subtype_from_linked_target)
    mask_subtype = df["subtenancy_subtype"].astype(str).str.len() > 0
    mask_not_arr = ~df["subtenancy_subtype"].astype(str).str.startswith("arrangementtype")
    df.loc[mask_subtype & mask_not_arr, "SubTenancy_Mapped"] = df.loc[mask_subtype & mask_not_arr, "subtenancy_subtype"]
    df["SubTenancy_Mapped"] = df["SubTenancy_Mapped"].apply(
        lambda s: ("" if isinstance(s, str) and s.strip().lower() in {"edge","ib","nabc","nabone","opendata"} else s)
    )

    if "Action Constant" not in df.columns: df["Action Constant"] = ""
    df = propagate_action_in_place(df)

    for c in DROP_COLS:
        if c in df.columns:
            df.drop(columns=[c], inplace=True)

    df["Version"] = df["Linked POLICY.Target"].apply(extract_version_from_target)
    df = df.fillna("")
    df["toggle_path"] = df.apply(build_toggle_path, axis=1)

    ordered = [c for c in OUTPUT_COL_ORDER if c in df.columns] + [c for c in df.columns if c not in OUTPUT_COL_ORDER]
    df = df[ordered]

    final_csv  = os.path.join(out_dir, "final_output.csv")
    final_html = os.path.join(out_dir, "final_output.html")
    df.to_csv(final_csv, index=False)
    write_html_table(df, "Final Output", final_html)
    print(f"Final CSV : {final_csv}\nFinal HTML: {final_html}")

# ==========================
# CLI
# ==========================

def main():
    ap = argparse.ArgumentParser(description="All-in-one pipeline with Action Constant forced to 4 whitelisted values.")
    ap.add_argument("--package", help=".deploymentpackage (zip/JSON)")
    ap.add_argument("--policy-path-csv", help="PolicyPath_dataset.csv (optional mode)")
    ap.add_argument("--actionlink-csv", help="actionlink_dataset.csv (optional mode)")
    ap.add_argument("--out-dir", default=".", help="Output directory")
    args = ap.parse_args()

    out_dir = os.path.abspath(args.out_dir); os.makedirs(out_dir, exist_ok=True)

    if args.policy_path_csv and args.actionlink_csv:
        run_from_csvs(args.policy_path_csv, args.actionlink_csv, out_dir)
        return

    if args.package:
        run_from_package(args.package, out_dir)
        return

    raise SystemExit("Provide either --package OR both --policy-path-csv and --actionlink-csv.")

if __name__ == "__main__":
    main()
