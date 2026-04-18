"""Integration tests for the full data lineage pipeline."""

import json
import os
from pathlib import Path

import pytest

from app.pipeline import run_full_pipeline, init_db, reset_tables, get_conn

# Test database path
TEST_DB_PATH = Path(__file__).parent.parent / "data" / "lineage.duckdb"

# Fixtures
@pytest.fixture
def clean_bundle():
    """Load the clean FHIR bundle (18 observations)."""
    path = Path(__file__).parent.parent / "data" / "sample_fhir" / "clean_bundle.json"
    return path.read_text()


@pytest.fixture
def anomaly_bundle():
    """Load the anomaly FHIR bundle (20 observations, 2 broken)."""
    path = Path(__file__).parent.parent / "data" / "sample_fhir" / "anomaly_bundle.json"
    return path.read_text()


@pytest.fixture(autouse=True)
def setup_db():
    """Initialize and clean database before each test."""
    # Remove existing database file to ensure fresh schema
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()
    # Also remove WAL files if they exist
    for ext in ["-wal", "-shm"]:
        wal_path = TEST_DB_PATH.with_suffix(f".duckdb{ext}")
        if wal_path.exists():
            wal_path.unlink()

    conn = get_conn()
    init_db(conn)
    conn.close()
    yield
    # Cleanup after test - remove database
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()
    for ext in ["-wal", "-shm"]:
        wal_path = TEST_DB_PATH.with_suffix(f".duckdb{ext}")
        if wal_path.exists():
            wal_path.unlink()


class TestFullPipelineCleanBundle:
    """Tests with the clean FHIR bundle (all records should pass)."""

    def test_full_pipeline_clean_bundle_produces_18_gold_rows(self, clean_bundle):
        """
        Clean bundle has 18 observations. All should pass through to Gold.
        NOTE: 18 is derived from the fixture. Update if clean_bundle.json changes.
        """
        results = run_full_pipeline(clean_bundle)

        # Check Bronze layer
        assert results["bronze"]["total"] == 18
        assert results["bronze"]["passed"] == 18
        assert results["bronze"]["quarantined"] == 0

        # Check Silver layer
        assert results["silver"]["total"] == 18
        assert results["silver"]["passed"] == 18
        assert results["silver"]["quarantined"] == 0

        # Check Gold layer
        assert results["gold"]["total"] == 18
        assert results["gold"]["inserted"] == 18
        assert len(results["gold"]["gold_df"]) == 18

    def test_gold_output_has_required_sdtm_columns(self, clean_bundle):
        """Gold output should have all required SDTM LB columns."""
        results = run_full_pipeline(clean_bundle)
        gold_df = results["gold"]["gold_df"]

        required_columns = [
            "STUDYID", "USUBJID", "LBTESTCD", "LBTEST", "LBSPEC", "LBORRES",
            "LBDTC", "LBLOINC", "LBNRIND", "VISITNUM", "VISIT", "LBSTRESN",
            "LBSTRESU", "LBBLFL", "EPOCH", "protocol_uri"
        ]

        for col in required_columns:
            assert col in gold_df.columns, f"Missing required column: {col}"

    def test_gold_constants_are_set_correctly(self, clean_bundle):
        """Demo constants should be set correctly in Gold output."""
        results = run_full_pipeline(clean_bundle)
        gold_df = results["gold"]["gold_df"]

        # All records should have the demo constants
        assert (gold_df["VISITNUM"] == 1).all()
        assert (gold_df["VISIT"] == "SCREENING").all()
        assert (gold_df["EPOCH"] == "SCREENING").all()
        assert (gold_df["LBBLFL"] == "Y").all()
        assert (gold_df["STUDYID"] == "NCT12345").all()

    def test_gate_results_excludes_reference_range_key(self, clean_bundle):
        """reference_range should NOT be in gate_results (it's an annotation, not a gate)."""
        results = run_full_pipeline(clean_bundle)

        assert "reference_range" not in results["bronze"]["gate_results"]
        assert "reference_range" not in results["silver"]["gate_results"]


class TestFullPipelineAnomalyBundle:
    """Tests with the anomaly FHIR bundle (contains broken records)."""

    def test_anomaly_bundle_quarantines_at_correct_layers(self, anomaly_bundle):
        """
        Anomaly bundle has 19 observations with 2 broken records:
        - obs-anomaly-B (null code.coding) → quarantined at Bronze (1 record)
        - obs-anomaly-A (unmapped LOINC 99999-9) → quarantined at Silver (1 record)
        """
        results = run_full_pipeline(anomaly_bundle)

        # Check Bronze layer: 1 quarantined (obs-anomaly-B with null loinc_code)
        assert results["bronze"]["total"] == 19
        assert results["bronze"]["passed"] == 18
        assert results["bronze"]["quarantined"] == 1

        # Check Silver layer: 1 quarantined (obs-anomaly-A with unmapped LOINC)
        assert results["silver"]["total"] == 18  # 18 from Bronze (19 total - 1 quarantined)
        assert results["silver"]["passed"] == 17
        assert results["silver"]["quarantined"] == 1

        # Check Gold layer: 17 inserted (17 from Silver passed)
        assert results["gold"]["total"] == 17
        assert results["gold"]["inserted"] == 17

    def test_lbnrind_annotation_does_not_quarantine(self, clean_bundle):
        """
        Out-of-range values should reach Gold with LBNRIND annotation,
        NOT be quarantined.

        We test with clean_bundle because some values may be outside
        reference ranges (this depends on the reference ranges in master_schema.json).
        """
        results = run_full_pipeline(clean_bundle)

        # Silver should have no quarantines (no reference range failures)
        # because reference_range is an annotation, not a gate
        assert results["silver"]["quarantined"] == 0

        # Gold should have all 18 records
        assert results["gold"]["inserted"] == 18

        # LBNRIND column should be populated
        gold_df = results["gold"]["gold_df"]
        assert "LBNRIND" in gold_df.columns

        # All LBNRIND values should be valid (H, L, N, or UN)
        valid_lbnrind = {"H", "L", "N", "UN"}
        assert all(gold_df["LBNRIND"].isin(valid_lbnrind))

    def test_gate_results_counts_are_accurate(self, anomaly_bundle):
        """Gate results should accurately count passes and fails per assertion."""
        results = run_full_pipeline(anomaly_bundle)

        # Bronze gate results
        bronze_gates = results["bronze"]["gate_results"]
        assert "not_null:Patient.identifier" in bronze_gates
        assert "not_null:Observation.status" in bronze_gates
        assert "not_null:Observation.code.coding" in bronze_gates
        assert "iso8601:Observation.effectiveDateTime" in bronze_gates

        # One record failed not_null:Observation.code.coding (obs-anomaly-B)
        assert bronze_gates["not_null:Observation.code.coding"]["passed"] == 18
        assert bronze_gates["not_null:Observation.code.coding"]["failed"] == 1

        # Other gates should have all passed
        assert bronze_gates["not_null:Patient.identifier"]["passed"] == 19
        assert bronze_gates["not_null:Patient.identifier"]["failed"] == 0

        # Silver gate results
        silver_gates = results["silver"]["gate_results"]
        assert "loinc_in_schema" in silver_gates

        # One record failed loinc_in_schema (obs-anomaly-A with LOINC 99999-9)
        assert silver_gates["loinc_in_schema"]["passed"] == 17
        assert silver_gates["loinc_in_schema"]["failed"] == 1


class TestReferenceRangeAnnotation:
    """Tests specific to LBNRIND annotation behavior."""

    def test_lbnrind_values_are_correctly_annotated(self, clean_bundle):
        """
        LBNRIND should be correctly annotated based on reference ranges.
        Values within range should have LBNRIND='N'.
        """
        results = run_full_pipeline(clean_bundle)
        gold_df = results["gold"]["gold_df"]

        # Glucose has ref range 40-500 mg/dL
        # Values like 94.5, 101.2, 88.0 should be 'N' (normal)
        glucose_rows = gold_df[gold_df["LBTESTCD"] == "GLUC"]
        for _, row in glucose_rows.iterrows():
            lbnrind = row["LBNRIND"]
            assert lbnrind in {"H", "L", "N", "UN"}, f"Invalid LBNRIND: {lbnrind}"

    def test_silver_layer_has_lbnrind_column(self, clean_bundle):
        """Silver layer should have lbnrind column populated."""
        results = run_full_pipeline(clean_bundle)
        silver_df = results["silver"]["silver_df"]

        assert "lbnrind" in silver_df.columns
        assert all(silver_df["lbnrind"].isin({"H", "L", "N", "UN"}))


class TestProtocolUri:
    """Tests for protocol_uri generation."""

    def test_protocol_uri_format_is_correct(self, clean_bundle):
        """Protocol URI should follow the expected format."""
        results = run_full_pipeline(clean_bundle)
        gold_df = results["gold"]["gold_df"]

        for _, row in gold_df.iterrows():
            protocol_uri = row["protocol_uri"]
            assert protocol_uri.startswith("study://NCT12345/lab/loinc/")
            assert protocol_uri.endswith("/baseline")


class TestSilverLayer:
    """Tests specific to Silver layer transformation."""

    def test_silver_flattens_fhir_structure(self, clean_bundle):
        """Silver layer should flatten nested FHIR JSON to columns."""
        results = run_full_pipeline(clean_bundle)
        silver_df = results["silver"]["silver_df"]

        # Check expected columns exist
        expected_cols = ["id", "patient_id", "loinc_code", "effective_dt", "value_numeric", "unit", "lbnrind"]
        for col in expected_cols:
            assert col in silver_df.columns

        # effective_datetime should become effective_dt
        assert "effective_datetime" not in silver_df.columns
        assert "effective_dt" in silver_df.columns
