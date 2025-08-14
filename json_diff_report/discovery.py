
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

def load_json(path: Path) -> Any:
    """Load a JSON file with UTF-8 (lenient)."""
    return json.loads(path.read_text(encoding="utf-8", errors="replace"))

def find_first_key(root: Any, key: str) -> Optional[Any]:
    """DFS: return the first value for 'key' anywhere in the JSON."""
    if isinstance(root, dict):
        if key in root:
            return root[key]
        for v in root.values():
            found = find_first_key(v, key)
            if found is not None:
                return found
    elif isinstance(root, list):
        for item in root:
            found = find_first_key(item, key)
            if found is not None:
                return found
    return None

def locate_arbitrary_services(root: Any) -> Optional[List[Dict[str, Any]]]:
    """Prefer toggles.arbitrary.services; else any 'arbitrary' with a 'services' list."""
    try:
        s = root["toggles"]["arbitrary"]["services"]
        if isinstance(s, list):
            return s  # type: ignore[return-value]
    except Exception:
        pass
    arb = find_first_key(root, "arbitrary")
    if isinstance(arb, dict):
        s = arb.get("services")
        if isinstance(s, list):
            return s  # type: ignore[return-value]
    return None

def locate_check_permissions(root: Any) -> Optional[List[Dict[str, Any]]]:
    """Prefer toggles.checkPermissions; else any 'checkPermissions' list."""
    try:
        cp = root["toggles"]["checkPermissions"]
        if isinstance(cp, list):
            return cp  # type: ignore[return-value]
    except Exception:
        pass
    cp = find_first_key(root, "checkPermissions")
    return cp if isinstance(cp, list) else None

def locate_get_permissions(root: Any) -> Optional[List[Dict[str, Any]]]:
    """Prefer toggles.getPermissions; else any 'getPermissions' list."""
    try:
        gp = root["toggles"]["getPermissions"]
        if isinstance(gp, list):
            return gp  # type: ignore[return-value]
    except Exception:
        pass
    gp = find_first_key(root, "getPermissions")
    return gp if isinstance(gp, list) else None
