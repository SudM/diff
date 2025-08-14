
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Tuple

def dicts_by_key(items: Iterable[dict], key: str) -> Dict[str, dict]:
    """Map list of dicts -> {item[key]: item} (skip non-dicts/missing key)."""
    out: Dict[str, dict] = {}
    for it in items or []:
        if isinstance(it, dict) and key in it:
            out[str(it[key])] = it
    return out

def paths_changed(a: Any, b: Any, prefix: str = "") -> List[Tuple[str, Any, Any]]:
    """
    Recursively compute differences between a and b.
    Returns [(path, old, new)]. Uses '(missing)' sentinel for adds/deletes.
    Paths look like: key.sub[2].name
    """
    diffs: List[Tuple[str, Any, Any]] = []

    if type(a) is not type(b):
        diffs.append((prefix or "(root)", a, b))
        return diffs

    if isinstance(a, dict):
        keys = sorted(set(a.keys()) | set(b.keys()), key=lambda x: str(x))
        for k in keys:
            p = f"{prefix}.{k}" if prefix else k
            if k not in a:
                diffs.append((p, "(missing)", b[k]))
            elif k not in b:
                diffs.append((p, a[k], "(missing)"))
            else:
                diffs.extend(paths_changed(a[k], b[k], p))
        return diffs

    if isinstance(a, list):
        m = min(len(a), len(b))
        for i in range(m):
            p = f"{prefix}[{i}]" if prefix else f"[{i}]"
            diffs.extend(paths_changed(a[i], b[i], p))
        for i in range(m, len(a)):
            diffs.append((f"{prefix}[{i}]", a[i], "(missing)"))
        for i in range(m, len(b)):
            diffs.append((f"{prefix}[{i}]", "(missing)", b[i]))
        return diffs

    if a != b:
        diffs.append((prefix or "(value)", a, b))
    return diffs
