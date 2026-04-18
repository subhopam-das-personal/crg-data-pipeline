import re
from dataclasses import dataclass
from typing import Any


ISO8601_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


@dataclass
class QualityResult:
    passed: bool
    assertion: str
    reason: str | None = None


def check_not_null(value: Any, field_name: str) -> QualityResult:
    """Check that a value is not None or empty string."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return QualityResult(False, f"not_null:{field_name}", f"NULL_{field_name.upper().replace('.', '_')}")
    return QualityResult(True, f"not_null:{field_name}")


def check_iso8601(value: str | None, field_name: str) -> QualityResult:
    """Check that a value is a valid ISO 8601 date."""
    if not value or not ISO8601_RE.match(value):
        return QualityResult(False, f"iso8601:{field_name}", f"INVALID_DATE:{value!r}")
    return QualityResult(True, f"iso8601:{field_name}")


def check_loinc_in_schema(loinc_code: str | None, schema: dict) -> QualityResult:
    """Check that a LOINC code exists in the master schema."""
    if not loinc_code:
        return QualityResult(False, "loinc_in_schema", "NULL_LOINC_CODE")
    if loinc_code not in schema:
        return QualityResult(False, "loinc_in_schema", f"LOINC_NOT_FOUND:{loinc_code}")
    return QualityResult(True, "loinc_in_schema")


def check_reference_range(value: float | None, mapping: dict) -> str:
    """
    Annotate value against reference range.

    Returns:
        'H' - value is above reference range
        'L' - value is below reference range
        'N' - value is within reference range
        'UN' - unable to determine (value is None OR ref range is absent)
    """
    if value is None:
        return "UN"
    low = mapping.get("ref_range_low")
    high = mapping.get("ref_range_high")
    if low is None or high is None:
        return "UN"
    if value < low:
        return "L"
    if value > high:
        return "H"
    return "N"


def run_bronze_gates(obs: dict) -> list[QualityResult]:
    """Quality gates at Bronze layer: structural integrity."""
    results = []
    patient_id = obs.get("patient_id")
    results.append(check_not_null(patient_id, "Patient.identifier"))
    results.append(check_not_null(obs.get("status"), "Observation.status"))
    results.append(check_not_null(obs.get("loinc_code"), "Observation.code.coding"))
    results.append(check_iso8601(obs.get("effective_datetime"), "Observation.effectiveDateTime"))
    return results


def run_silver_gates(row: dict, schema: dict) -> list[QualityResult]:
    """
    Quality gates at Silver layer: semantic validity.

    Note: check_reference_range is NOT included here as it is an annotation,
    not a gate that causes quarantine.
    """
    results = []
    loinc_code = row.get("loinc_code")
    results.append(check_loinc_in_schema(loinc_code, schema))
    return results


def all_passed(results: list[QualityResult]) -> bool:
    """Check if all quality gates passed."""
    return all(r.passed for r in results)


def first_failure_reason(results: list[QualityResult]) -> str | None:
    """Get the reason for the first failing gate."""
    for r in results:
        if not r.passed:
            return r.reason
    return None
