import json
from pathlib import Path

_SCHEMA_PATH = Path(__file__).parent.parent / "data" / "master_schema.json"

with open(_SCHEMA_PATH) as f:
    SCHEMA: dict = json.load(f)


def get_mapping(loinc_code: str) -> dict | None:
    """Return SDTM mapping for a LOINC code, or None if not found."""
    return SCHEMA.get(loinc_code)


def all_codes() -> list[str]:
    return list(SCHEMA.keys())
