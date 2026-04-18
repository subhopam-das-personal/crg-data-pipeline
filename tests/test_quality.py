"""Unit tests for quality gate functions."""

import pytest
from app.quality import (
    check_not_null,
    check_iso8601,
    check_loinc_in_schema,
    check_reference_range,
    run_bronze_gates,
    run_silver_gates,
    all_passed,
    first_failure_reason,
)

# Sample schema for tests
TEST_SCHEMA = {
    "2345-7": {
        "LBTESTCD": "GLUC",
        "LBTEST": "Glucose",
        "LBSPEC": "SERUM",
        "ref_range_low": 40,
        "ref_range_high": 500,
        "unit": "mg/dL"
    },
    "718-7": {
        "LBTESTCD": "HGB",
        "LBTEST": "Hemoglobin",
        "LBSPEC": "BLOOD",
        "ref_range_low": 5,
        "ref_range_high": 25,
        "unit": "g/dL"
    }
}


class TestCheckNotNull:
    """Tests for check_not_null function."""

    def test_passes_valid_string(self):
        result = check_not_null("valid_value", "TestField")
        assert result.passed is True
        assert result.assertion == "not_null:TestField"

    def test_fails_none(self):
        result = check_not_null(None, "TestField")
        assert result.passed is False
        assert result.assertion == "not_null:TestField"
        assert "NULL_TESTFIELD" in result.reason

    def test_fails_empty_string(self):
        result = check_not_null("", "TestField")
        assert result.passed is False

    def test_fails_whitespace_only(self):
        result = check_not_null("   ", "TestField")
        assert result.passed is False


class TestCheckIso8601:
    """Tests for check_iso8601 function."""

    def test_passes_valid_datetime_string(self):
        result = check_iso8601("2024-04-18T10:30:00Z", "TestField")
        assert result.passed is True
        assert result.assertion == "iso8601:TestField"

    def test_passes_date_only(self):
        result = check_iso8601("2024-04-18", "TestField")
        assert result.passed is True

    def test_fails_non_date_string(self):
        result = check_iso8601("not-a-date", "TestField")
        assert result.passed is False
        assert result.assertion == "iso8601:TestField"

    def test_fails_none(self):
        result = check_iso8601(None, "TestField")
        assert result.passed is False


class TestCheckLoincInSchema:
    """Tests for check_loinc_in_schema function."""

    def test_passes_known_code(self):
        result = check_loinc_in_schema("2345-7", TEST_SCHEMA)
        assert result.passed is True
        assert result.assertion == "loinc_in_schema"

    def test_fails_unknown_code(self):
        result = check_loinc_in_schema("99999-9", TEST_SCHEMA)
        assert result.passed is False
        assert "LOINC_NOT_FOUND:99999-9" in result.reason

    def test_fails_none(self):
        result = check_loinc_in_schema(None, TEST_SCHEMA)
        assert result.passed is False
        assert "NULL_LOINC_CODE" in result.reason


class TestCheckReferenceRange:
    """Tests for check_reference_range function."""

    def test_returns_n_for_in_range_value(self):
        mapping = TEST_SCHEMA["2345-7"]
        result = check_reference_range(100.0, mapping)
        assert result == "N"

    def test_returns_l_for_below_low_range(self):
        mapping = TEST_SCHEMA["2345-7"]
        result = check_reference_range(20.0, mapping)
        assert result == "L"

    def test_returns_h_for_above_high_range(self):
        mapping = TEST_SCHEMA["2345-7"]
        result = check_reference_range(600.0, mapping)
        assert result == "H"

    def test_returns_un_when_value_is_none(self):
        mapping = TEST_SCHEMA["2345-7"]
        result = check_reference_range(None, mapping)
        assert result == "UN"

    def test_returns_un_when_mapping_lacks_ref_range_keys(self):
        mapping = {"LBTESTCD": "GLUC", "LBTEST": "Glucose", "LBSPEC": "SERUM", "unit": "mg/dL"}
        result = check_reference_range(100.0, mapping)
        assert result == "UN"


class TestRunBronzeGates:
    """Tests for run_bronze_gates function."""

    def test_all_pass(self):
        obs = {
            "patient_id": "SUBJ001",
            "status": "final",
            "loinc_code": "2345-7",
            "effective_datetime": "2024-04-18T10:30:00Z",
        }
        results = run_bronze_gates(obs)
        assert all_passed(results) is True

    def test_fails_on_null_loinc_code(self):
        obs = {
            "patient_id": "SUBJ001",
            "status": "final",
            "loinc_code": None,
            "effective_datetime": "2024-04-18T10:30:00Z",
        }
        results = run_bronze_gates(obs)
        assert all_passed(results) is False
        # The reason uses underscores instead of dots
        assert any(not r.passed and "CODE" in r.reason for r in results)


class TestRunSilverGates:
    """Tests for run_silver_gates function."""

    def test_all_pass(self):
        row = {
            "loinc_code": "2345-7",
        }
        results = run_silver_gates(row, TEST_SCHEMA)
        assert all_passed(results) is True
        assert len(results) == 1  # Only loinc_in_schema, not reference_range

    def test_fails_on_unmapped_loinc_code(self):
        row = {
            "loinc_code": "99999-9",
        }
        results = run_silver_gates(row, TEST_SCHEMA)
        assert all_passed(results) is False
        assert any(not r.passed for r in results)


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_all_passed_with_all_true(self):
        from app.quality import QualityResult
        results = [
            QualityResult(True, "test1"),
            QualityResult(True, "test2"),
        ]
        assert all_passed(results) is True

    def test_all_passed_with_one_false(self):
        from app.quality import QualityResult
        results = [
            QualityResult(True, "test1"),
            QualityResult(False, "test2", "fail_reason"),
        ]
        assert all_passed(results) is False

    def test_first_failure_reason(self):
        from app.quality import QualityResult
        results = [
            QualityResult(True, "test1"),
            QualityResult(False, "test2", "first_fail"),
            QualityResult(False, "test3", "second_fail"),
        ]
        assert first_failure_reason(results) == "first_fail"

    def test_first_failure_reason_none_when_all_pass(self):
        from app.quality import QualityResult
        results = [
            QualityResult(True, "test1"),
            QualityResult(True, "test2"),
        ]
        assert first_failure_reason(results) is None
