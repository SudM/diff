import json, io
from typing import Any, Dict, List
from pathlib import Path
import zipfile
import xml.etree.ElementTree as ET

def _load_json_bytes(b: bytes) -> Any:
    for enc in ("utf-8", "utf-16", "utf-8-sig"):
        try:
            return json.loads(b.decode(enc))
        except Exception:
            continue
    return None

def parse_deployment_package(path: Path) -> Dict[str, Any]:
    data = path.read_bytes()
    js = _load_json_bytes(data)
    if js is not None:
        return {"format": "json", "json": js, "xml": None}
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            names = z.namelist()
            json_name = max((n for n in names if n.endswith(".json")), key=lambda n: z.getinfo(n).file_size, default=None)
            if json_name:
                js2 = _load_json_bytes(z.read(json_name))
                if js2:
                    return {"format": "zip-json", "json": js2, "xml": None}
    except:
        pass
    return {"format": "unknown", "json": None, "xml": None}

def _find_condition_pairs(obj: Any) -> List:
    pairs = []
    if isinstance(obj, dict):
        const_keys = ["constant", "const", "field", "left", "attribute", "name", "key"]
        val_keys = ["value", "right", "equals", "constantValue"]
        c = v = None
        for k in const_keys:
            if k in obj and isinstance(obj[k], str):
                c = obj[k]
                break
        for k in val_keys:
            if k in obj and isinstance(obj[k], (str, int, float, bool)):
                v = obj[k]
                break
        if c is not None:
            pairs.append((c, v))
        for lk in ["conditions", "condition", "rules", "expressions", "children", "operands"]:
            if lk in obj and isinstance(obj[lk], list):
                for child in obj[lk]:
                    pairs.extend(_find_condition_pairs(child))
    elif isinstance(obj, list):
        for child in obj:
            pairs.extend(_find_condition_pairs(child))
    return pairs

def collect_links_from_json(d: Any) -> List[Dict[str, Any]]:
    out = []
    if isinstance(d, dict):
        node_type = d.get("type") or d.get("nodeType") or d.get("kind")
        action_value = d.get("action") or d.get("name") or d.get("actionName")
        if not isinstance(action_value, str):
            for v in d.values():
                if isinstance(v, str) and v.startswith("action:"):
                    action_value = v
                    break
        is_action = (isinstance(node_type, str) and node_type.upper() == "ACTION") or                     (isinstance(action_value, str) and action_value.startswith("action:"))
        if is_action:
            out.append({
                "action": action_value,
                "tenant": d.get("tenant", ""),
                "version": d.get("version", ""),
                "policySetName": d.get("policySet", ""),
                "policySetId": d.get("policySetId", ""),
                "policyName": d.get("policy", ""),
                "policyId": d.get("policyId", ""),
                "constantPairs": _find_condition_pairs(d)
            })
        for v in d.values():
            out.extend(collect_links_from_json(v))
    elif isinstance(d, list):
        for v in d:
            out.extend(collect_links_from_json(v))
    return out