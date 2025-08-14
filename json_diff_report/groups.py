
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from .discovery import locate_arbitrary_services, locate_check_permissions, locate_get_permissions

@dataclass(frozen=True)
class GroupSpec:
    title: str
    id_key_label: str
    id_key: str
    locator: Callable[[Any], Optional[List[Dict[str, Any]]]]

GROUPS: List[GroupSpec] = [
    GroupSpec(title="arbitrary → services", id_key_label="Service Name", id_key="name",         locator=locate_arbitrary_services),
    GroupSpec(title="checkPermissions",      id_key_label="Action",        id_key="action",      locator=locate_check_permissions),
    GroupSpec(title="getPermissions",        id_key_label="Resource Type", id_key="resourceType",locator=locate_get_permissions),
]
