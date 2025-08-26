import json
import logging
from typing import Tuple,Union
from io import IOBase

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("policy_pipeline")

def safe_load_json(file_or_path: Union[str, IOBase]):
    """Safely load JSON from a file path or a file-like object (Streamlit upload)."""
    try:
        if isinstance(file_or_path, str):
            with open(file_or_path, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            # Streamlit UploadedFile or file-like
            return json.load(file_or_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_or_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {file_or_path}: {e}")
        raise

def pos_tuple(position: str) -> Tuple[int, ...]:
    """Convert '1.2.3' -> (1,2,3)."""
    return tuple(int(x) for x in str(position).split(".") if x.isdigit())
