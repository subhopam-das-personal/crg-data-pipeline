import logging
import os
import subprocess
from datetime import datetime, timezone

from openlineage.client import OpenLineageClient
from openlineage.client.run import (
    RunEvent,
    RunState,
    Run,
    Job,
    Dataset,
)
from openlineage.client.facet import (
    DataQualityAssertionsDatasetFacet,
    Assertion,
    DataQualityMetricsInputDatasetFacet,
    SchemaDatasetFacet,
    SchemaField,
    SourceCodeLocationJobFacet,
)

from .constants import (
    NAMESPACE, PRODUCER,
    JOB_FHIR_TO_BRONZE, JOB_BRONZE_TO_SILVER, JOB_SILVER_TO_GOLD,
    BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE,
    INPUT_FHIR_BUNDLE
)

logger = logging.getLogger(__name__)


def _get_client() -> OpenLineageClient | None:
    """Get OpenLineage client, returns None if Marquez is unavailable."""
    url = os.getenv("OPENLINEAGE_URL", "http://localhost:5000")
    try:
        return OpenLineageClient(url=url)
    except Exception as e:
        logger.warning(f"Failed to connect to Marquez at {url}: {e}")
        return None


def _now() -> str:
    """Get current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _git_sha() -> str:
    """
    Get git commit SHA. Priority:
    1. GIT_SHA env var (baked at Docker build time)
    2. subprocess `git rev-parse --short HEAD` (local dev)
    3. "unknown" if neither works
    """
    # First check env var (Docker build time)
    sha = os.getenv("GIT_SHA", "")
    if sha:
        return sha

    # Fall back to subprocess (local dev)
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
        ).decode().strip()
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        return "unknown"


def _build_assertions(gate_results: dict) -> list[Assertion]:
    """
    Build Assertion objects from gate results dict.

    Args:
        gate_results: Dict like {"assertion_name": {"passed": int, "failed": int}, ...}

    Returns:
        List of Assertion objects for OpenLineage
    """
    assertions = []
    for assertion_name, counts in gate_results.items():
        assertions.append(
            Assertion(
                assertion=assertion_name,
                success=(counts["failed"] == 0),
                column=None,
            )
        )
    return assertions


def emit_bronze_event(run_id: str, passed: int, quarantined: int, gate_results: dict) -> None:
    """Emit OpenLineage event for Bronze layer."""
    client = _get_client()
    if not client:
        return

    assertions = _build_assertions(gate_results)

    output_dataset = Dataset(
        namespace=NAMESPACE,
        name=BRONZE_TABLE,
        facets={
            "schema": SchemaDatasetFacet(fields=[
                SchemaField(name="id", type="TEXT"),
                SchemaField(name="patient_id", type="TEXT"),
                SchemaField(name="loinc_code", type="TEXT"),
                SchemaField(name="effective_datetime", type="TEXT"),
                SchemaField(name="value_numeric", type="DOUBLE"),
            ]),
            "dataQualityAssertions": DataQualityAssertionsDatasetFacet(assertions=assertions),
            "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                rowCount=passed + quarantined,
                bytes=None,
                columnMetrics={},
            ),
        },
    )

    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=_now(),
        run=Run(runId=run_id),
        job=Job(namespace=NAMESPACE, name=JOB_FHIR_TO_BRONZE),
        producer=PRODUCER,
        inputs=[Dataset(namespace=NAMESPACE, name=INPUT_FHIR_BUNDLE)],
        outputs=[output_dataset],
    )

    try:
        client.emit(event)
    except Exception as e:
        logger.warning(f"Failed to emit Bronze lineage event: {e}")


def emit_silver_event(run_id: str, passed: int, quarantined: int, gate_results: dict) -> None:
    """Emit OpenLineage event for Silver layer."""
    client = _get_client()
    if not client:
        return

    assertions = _build_assertions(gate_results)

    output_dataset = Dataset(
        namespace=NAMESPACE,
        name=SILVER_TABLE,
        facets={
            "schema": SchemaDatasetFacet(fields=[
                SchemaField(name="id", type="TEXT"),
                SchemaField(name="patient_id", type="TEXT"),
                SchemaField(name="loinc_code", type="TEXT"),
                SchemaField(name="effective_dt", type="TEXT"),
                SchemaField(name="value_numeric", type="DOUBLE"),
                SchemaField(name="lbnrind", type="TEXT"),
            ]),
            "dataQualityAssertions": DataQualityAssertionsDatasetFacet(
                assertions=assertions
            ),
            "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                rowCount=passed + quarantined,
                bytes=None,
                columnMetrics={},
            ),
        },
    )

    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=_now(),
        run=Run(runId=run_id),
        job=Job(namespace=NAMESPACE, name=JOB_BRONZE_TO_SILVER),
        producer=PRODUCER,
        inputs=[Dataset(namespace=NAMESPACE, name=BRONZE_TABLE)],
        outputs=[output_dataset],
    )

    try:
        client.emit(event)
    except Exception as e:
        logger.warning(f"Failed to emit Silver lineage event: {e}")


def emit_gold_event(run_id: str, row_count: int) -> None:
    """Emit OpenLineage event for Gold layer."""
    client = _get_client()
    if not client:
        return

    sha = _git_sha()

    output_dataset = Dataset(
        namespace=NAMESPACE,
        name=GOLD_TABLE,
        facets={
            "schema": SchemaDatasetFacet(fields=[
                SchemaField(name="STUDYID", type="TEXT"),
                SchemaField(name="USUBJID", type="TEXT"),
                SchemaField(name="LBTESTCD", type="TEXT"),
                SchemaField(name="LBTEST", type="TEXT"),
                SchemaField(name="LBSPEC", type="TEXT"),
                SchemaField(name="LBORRES", type="TEXT"),
                SchemaField(name="LBDTC", type="TEXT"),
                SchemaField(name="LBLOINC", type="TEXT"),
                SchemaField(name="LBNRIND", type="TEXT"),
                SchemaField(name="VISITNUM", type="INTEGER"),
                SchemaField(name="VISIT", type="TEXT"),
                SchemaField(name="LBSTRESN", type="DOUBLE"),
                SchemaField(name="LBSTRESU", type="TEXT"),
                SchemaField(name="LBBLFL", type="TEXT"),
                SchemaField(name="EPOCH", type="TEXT"),
                SchemaField(name="protocol_uri", type="TEXT"),
            ]),
            "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                rowCount=row_count,
                bytes=None,
                columnMetrics={},
            ),
        },
    )

    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=_now(),
        run=Run(runId=run_id),
        job=Job(
            namespace=NAMESPACE,
            name=JOB_SILVER_TO_GOLD,
            facets={
                "sourceCodeLocation": SourceCodeLocationJobFacet(
                    type="git",
                    url="https://github.com/subhopam/data_lineage",
                    repoUrl="https://github.com/subhopam/data_lineage",
                    branch="main",
                    version=sha,
                )
            },
        ),
        producer=PRODUCER,
        inputs=[Dataset(namespace=NAMESPACE, name=SILVER_TABLE)],
        outputs=[output_dataset],
    )

    try:
        client.emit(event)
    except Exception as e:
        logger.warning(f"Failed to emit Gold lineage event: {e}")


def emit_pipeline_events(run_id: str, results: dict) -> None:
    """Emit all three layer events for a completed pipeline run."""
    b = results["bronze"]
    s = results["silver"]
    g = results["gold"]

    emit_bronze_event(run_id, b["passed"], b["quarantined"], b["gate_results"])
    emit_silver_event(run_id, s["passed"], s["quarantined"], s["gate_results"])
    emit_gold_event(run_id, g["inserted"])
