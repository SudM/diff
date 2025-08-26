import pandas as pd
from .utils import logger
from .merge import resolve_constants

def extract_actions(condition_defs, id_lookup, attr_defs) -> pd.DataFrame:
    """Extract ACTION.* conditions and constants."""
    action_records = []
    for cond in condition_defs:
        if cond.get("name", "").startswith("ACTION."):
            vals = resolve_constants(cond["id"], id_lookup, attr_defs)
            action_records.append({
                "ID": cond["id"],
                "Full Path": cond.get("name", ""),
                "Value_action": ";".join(vals) if vals else ""
            })
    df_action = pd.DataFrame(action_records)
    logger.info(f"Extracted {len(df_action)} ACTION records")
    return df_action
