"""
Tests for OpenLineage facet construction in lineage.py.

These tests verify that all event emitter functions construct valid OpenLineage
objects without raising TypeError/AttributeError. This catches API mismatches
(wrong kwarg names, wrong argument types) that are invisible at import time.

Each test runs with OPENLINEAGE_URL unset so _get_client() returns None and
the emit() call is skipped — but the facet construction happens before that
guard, so any constructor bugs will raise here.
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Ensure app/ is on the path (mirrors how Streamlit runs it)
sys.path.insert(0, str(Path(__file__).parent.parent / "app"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

import uuid

GATE_RESULTS_OK = {
    "schema_check": {"passed": 10, "failed": 0},
    "range_check": {"passed": 10, "failed": 0},
}
GATE_RESULTS_PARTIAL = {
    "schema_check": {"passed": 8, "failed": 2},
}

RUN_ID = str(uuid.uuid4())


def _make_client():
    """Return a mock OpenLineage client that records emit calls."""
    client = MagicMock()
    client.emit = MagicMock()
    return client


# ---------------------------------------------------------------------------
# _get_data_source
# ---------------------------------------------------------------------------

def test_data_source_defaults_to_hospital_ehr():
    from lineage import _get_data_source
    result = _get_data_source("unknown_file.json")
    assert result["name"] == "Hospital EHR"
    assert result["uri"] == "https://hospital.example.com/ehr"


def test_data_source_detects_labcorp():
    from lineage import _get_data_source
    result = _get_data_source("labcorp_bundle.json")
    assert result["name"] == "LabCorp"


def test_data_source_detects_quest():
    from lineage import _get_data_source
    assert _get_data_source("quest_diagnostics.json")["name"] == "Quest Diagnostics"


def test_data_source_detects_mayo():
    from lineage import _get_data_source
    assert _get_data_source("mayo_clinic_results.json")["name"] == "Mayo Clinic Labs"


def test_data_source_detects_research():
    from lineage import _get_data_source
    assert _get_data_source("research_cohort.json")["name"] == "Clinical Research Database"


def test_data_source_case_insensitive():
    from lineage import _get_data_source
    assert _get_data_source("LABCORP_2024.JSON")["name"] == "LabCorp"


def test_data_source_empty_string():
    from lineage import _get_data_source
    result = _get_data_source("")
    assert result["name"] == "Hospital EHR"  # falls through to default


def test_data_source_returns_slug():
    from lineage import _get_data_source
    assert _get_data_source("labcorp_bundle.json")["slug"] == "labcorp"


def test_data_source_default_slug():
    from lineage import _get_data_source
    assert _get_data_source("unknown.json")["slug"] == "hospital_ehr"


def test_data_source_quest_slug():
    from lineage import _get_data_source
    assert _get_data_source("quest_results.json")["slug"] == "quest_diagnostics"


# ---------------------------------------------------------------------------
# emit_bronze_event — per-source dataset naming
# ---------------------------------------------------------------------------

def test_emit_bronze_event_uses_source_slug_as_dataset_name():
    """Input dataset name should reflect the source — Marquez shows distinct upstream nodes."""
    captured = []
    mock_client = _make_client()
    mock_client.emit = lambda event: captured.append(event)
    with patch("lineage._get_client", return_value=mock_client):
        from lineage import emit_bronze_event
        emit_bronze_event(RUN_ID, 10, 0, GATE_RESULTS_OK, bundle_name="labcorp_samples.json")
    assert len(captured) == 1
    assert captured[0].inputs[0].name == "labcorp.fhir_bundle"


def test_emit_bronze_event_default_source_dataset_name():
    """Unknown bundle falls through to hospital_ehr slug."""
    captured = []
    mock_client = _make_client()
    mock_client.emit = lambda event: captured.append(event)
    with patch("lineage._get_client", return_value=mock_client):
        from lineage import emit_bronze_event
        emit_bronze_event(RUN_ID, 10, 0, GATE_RESULTS_OK, bundle_name="unknown.json")
    assert captured[0].inputs[0].name == "hospital_ehr.fhir_bundle"


# ---------------------------------------------------------------------------
# _build_assertions
# ---------------------------------------------------------------------------

def test_build_assertions_all_passed():
    from lineage import _build_assertions
    assertions = _build_assertions({"schema_check": {"passed": 10, "failed": 0}})
    assert len(assertions) == 1
    assert assertions[0].success is True


def test_build_assertions_with_failures():
    from lineage import _build_assertions
    assertions = _build_assertions({"range_check": {"passed": 8, "failed": 2}})
    assert assertions[0].success is False


def test_build_assertions_multiple_gates():
    from lineage import _build_assertions
    assertions = _build_assertions(GATE_RESULTS_OK)
    assert len(assertions) == 2


def test_build_assertions_malformed_gate_raises():
    """Confirm KeyError when gate_results dict is missing the 'failed' key."""
    from lineage import _build_assertions
    import pytest
    with pytest.raises(KeyError):
        _build_assertions({"broken_gate": {"passed": 5}})  # no "failed" key


def test_build_assertions_empty():
    from lineage import _build_assertions
    assert _build_assertions({}) == []


# ---------------------------------------------------------------------------
# emit_bronze_event — full facet construction
# ---------------------------------------------------------------------------

def test_emit_bronze_event_constructs_without_error():
    with patch("lineage._get_client", return_value=_make_client()):
        from lineage import emit_bronze_event
        # Must not raise TypeError, AttributeError, or any other exception
        emit_bronze_event(
            run_id=RUN_ID,
            passed=10,
            quarantined=2,
            gate_results=GATE_RESULTS_OK,
            bundle_name="clean_bundle.json",
        )


def test_emit_bronze_event_with_labcorp_bundle():
    with patch("lineage._get_client", return_value=_make_client()):
        from lineage import emit_bronze_event
        emit_bronze_event(
            run_id=RUN_ID,
            passed=5,
            quarantined=0,
            gate_results=GATE_RESULTS_OK,
            bundle_name="labcorp_samples.json",
        )


def test_emit_bronze_event_default_bundle_name():
    """Default bundle_name should not raise."""
    with patch("lineage._get_client", return_value=_make_client()):
        from lineage import emit_bronze_event
        emit_bronze_event(
            run_id=RUN_ID,
            passed=18,
            quarantined=0,
            gate_results=GATE_RESULTS_OK,
        )


def test_emit_bronze_event_emits_once():
    mock_client = _make_client()
    with patch("lineage._get_client", return_value=mock_client):
        from lineage import emit_bronze_event
        emit_bronze_event(RUN_ID, 10, 2, GATE_RESULTS_OK)
    mock_client.emit.assert_called_once()


def test_emit_bronze_event_no_client_skips_gracefully():
    """If Marquez is unreachable, emit should silently return."""
    with patch("lineage._get_client", return_value=None):
        from lineage import emit_bronze_event
        emit_bronze_event(RUN_ID, 10, 0, GATE_RESULTS_OK)  # must not raise


# ---------------------------------------------------------------------------
# emit_silver_event — full facet construction including column lineage
# ---------------------------------------------------------------------------

def test_emit_silver_event_constructs_without_error():
    with patch("lineage._get_client", return_value=_make_client()):
        from lineage import emit_silver_event
        emit_silver_event(
            run_id=RUN_ID,
            passed=10,
            quarantined=0,
            gate_results=GATE_RESULTS_OK,
        )


def test_emit_silver_event_emits_once():
    mock_client = _make_client()
    with patch("lineage._get_client", return_value=mock_client):
        from lineage import emit_silver_event
        emit_silver_event(RUN_ID, 10, 0, GATE_RESULTS_OK)
    mock_client.emit.assert_called_once()


def test_emit_silver_event_no_client_skips_gracefully():
    with patch("lineage._get_client", return_value=None):
        from lineage import emit_silver_event
        emit_silver_event(RUN_ID, 10, 0, GATE_RESULTS_OK)


# ---------------------------------------------------------------------------
# emit_gold_event — column lineage with CDISC SDTM fields
# ---------------------------------------------------------------------------

def test_emit_gold_event_constructs_without_error():
    with patch("lineage._get_client", return_value=_make_client()):
        from lineage import emit_gold_event
        emit_gold_event(run_id=RUN_ID, row_count=15)


def test_emit_gold_event_emits_once():
    mock_client = _make_client()
    with patch("lineage._get_client", return_value=mock_client):
        from lineage import emit_gold_event
        emit_gold_event(RUN_ID, row_count=15)
    mock_client.emit.assert_called_once()


def test_emit_gold_event_no_client_skips_gracefully():
    with patch("lineage._get_client", return_value=None):
        from lineage import emit_gold_event
        emit_gold_event(RUN_ID, row_count=0)


# ---------------------------------------------------------------------------
# emit_pipeline_events — bundle_name threading
# ---------------------------------------------------------------------------

def test_emit_pipeline_events_threads_bundle_name():
    """Verify bundle_name reaches emit_bronze_event."""
    with patch("lineage._get_client", return_value=_make_client()):
        with patch("lineage._get_data_source", wraps=__import__("lineage")._get_data_source) as spy:
            from lineage import emit_pipeline_events
            results = {
                "bronze": {"passed": 10, "quarantined": 0, "gate_results": GATE_RESULTS_OK},
                "silver": {"passed": 10, "quarantined": 0, "gate_results": GATE_RESULTS_OK},
                "gold": {"inserted": 10},
            }
            emit_pipeline_events(RUN_ID, results, bundle_name="labcorp_bundle.json")
            spy.assert_called_once_with("labcorp_bundle.json")


def test_emit_pipeline_events_default_bundle_name():
    with patch("lineage._get_client", return_value=_make_client()):
        from lineage import emit_pipeline_events
        results = {
            "bronze": {"passed": 5, "quarantined": 0, "gate_results": GATE_RESULTS_OK},
            "silver": {"passed": 5, "quarantined": 0, "gate_results": GATE_RESULTS_OK},
            "gold": {"inserted": 5},
        }
        emit_pipeline_events(RUN_ID, results)  # default bundle_name — must not raise
