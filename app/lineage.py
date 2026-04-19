import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

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
    ColumnLineageDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
)

# Add app directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from constants import (
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

    # Define input dataset (FHIR bundle upload)
    input_dataset = Dataset(
        namespace=NAMESPACE,
        name=INPUT_FHIR_BUNDLE,
        facets={
            "schema": SchemaDatasetFacet(fields=[
                SchemaField(name="resourceType", type="TEXT"),
                SchemaField(name="type", type="TEXT"),
                SchemaField(name="id", type="TEXT"),
            ]),
            "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                rowCount=1,  # One bundle uploaded
                bytes=None,
                columnMetrics={},
            ),
        },
    )

    # Define output dataset (bronze table)
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
                SchemaField(name="unit", type="TEXT"),
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
        inputs=[input_dataset],
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

    # Define input dataset (bronze table) - must match bronze output schema
    input_dataset = Dataset(
        namespace=NAMESPACE,
        name=BRONZE_TABLE,
        facets={
            "schema": SchemaDatasetFacet(fields=[
                SchemaField(name="id", type="TEXT"),
                SchemaField(name="patient_id", type="TEXT"),
                SchemaField(name="loinc_code", type="TEXT"),
                SchemaField(name="effective_datetime", type="TEXT"),
                SchemaField(name="value_numeric", type="DOUBLE"),
                SchemaField(name="unit", type="TEXT"),
            ]),
        },
    )

    # Define output dataset (silver table)
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
                SchemaField(name="unit", type="TEXT"),
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
            "columnLineage": ColumnLineageDatasetFacet(
                fields={
                    "id": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=BRONZE_TABLE,
                                field="id"
                            )
                        ],
                        transformationDescription="Direct copy of observation identifier",
                        transformationType="IDENTITY"
                    ),
                    "patient_id": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=BRONZE_TABLE,
                                field="patient_id"
                            )
                        ],
                        transformationDescription="Direct copy of patient identifier",
                        transformationType="IDENTITY"
                    ),
                    "loinc_code": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=BRONZE_TABLE,
                                field="loinc_code"
                            )
                        ],
                        transformationDescription="Direct copy of LOINC code",
                        transformationType="IDENTITY"
                    ),
                    "effective_dt": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=BRONZE_TABLE,
                                field="effective_datetime"
                            )
                        ],
                        transformationDescription="Renamed from effective_datetime for consistency",
                        transformationType="IDENTITY"
                    ),
                    "value_numeric": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=BRONZE_TABLE,
                                field="value_numeric"
                            )
                        ],
                        transformationDescription="Direct copy of numeric observation value",
                        transformationType="IDENTITY"
                    ),
                    "unit": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=BRONZE_TABLE,
                                field="unit"
                            )
                        ],
                        transformationDescription="Direct copy of unit",
                        transformationType="IDENTITY"
                    ),
                    "lbnrind": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=BRONZE_TABLE,
                                field="value_numeric"
                            ),
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=BRONZE_TABLE,
                                field="loinc_code"
                            )
                        ],
                        transformationDescription="Calculated reference range indicator (H/L/N/UN) based on LOINC reference ranges",
                        transformationType="DERIVATION"
                    ),
                }
            ),
        },
    )

    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=_now(),
        run=Run(runId=run_id),
        job=Job(namespace=NAMESPACE, name=JOB_BRONZE_TO_SILVER),
        producer=PRODUCER,
        inputs=[input_dataset],
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

    # Define input dataset (silver table) - must match silver output schema
    input_dataset = Dataset(
        namespace=NAMESPACE,
        name=SILVER_TABLE,
        facets={
            "schema": SchemaDatasetFacet(fields=[
                SchemaField(name="id", type="TEXT"),
                SchemaField(name="patient_id", type="TEXT"),
                SchemaField(name="loinc_code", type="TEXT"),
                SchemaField(name="effective_dt", type="TEXT"),
                SchemaField(name="value_numeric", type="DOUBLE"),
                SchemaField(name="unit", type="TEXT"),
                SchemaField(name="lbnrind", type="TEXT"),
            ]),
        },
    )

    # Define output dataset (gold table)
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
            "columnLineage": ColumnLineageDatasetFacet(
                fields={
                    "USUBJID": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=SILVER_TABLE,
                                field="patient_id"
                            )
                        ],
                        transformationDescription="Transformed to CDISC SDTM subject ID format (STUDYID-patientID)",
                        transformationType="TRANSFORMATION"
                    ),
                    "LBTESTCD": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=SILVER_TABLE,
                                field="loinc_code"
                            )
                        ],
                        transformationDescription="Mapped from LOINC code to CDISC LB test code",
                        transformationType="LOOKUP"
                    ),
                    "LBTEST": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=SILVER_TABLE,
                                field="loinc_code"
                            )
                        ],
                        transformationDescription="Mapped from LOINC code to CDISC LB test name",
                        transformationType="LOOKUP"
                    ),
                    "LBSPEC": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=SILVER_TABLE,
                                field="loinc_code"
                            )
                        ],
                        transformationDescription="Mapped from LOINC code to CDISC LB specimen",
                        transformationType="LOOKUP"
                    ),
                    "LBORRES": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=SILVER_TABLE,
                                field="value_numeric"
                            )
                        ],
                        transformationDescription="Converted to string for CDISC SDTM LB observation result",
                        transformationType="TYPE_CONVERSION"
                    ),
                    "LBDTC": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=SILVER_TABLE,
                                field="effective_dt"
                            )
                        ],
                        transformationDescription="Renamed to CDISC SDTM LB date/time format",
                        transformationType="IDENTITY"
                    ),
                    "LBLOINC": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=SILVER_TABLE,
                                field="loinc_code"
                            )
                        ],
                        transformationDescription="Renamed to CDISC SDTM LOINC field",
                        transformationType="IDENTITY"
                    ),
                    "LBNRIND": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=SILVER_TABLE,
                                field="lbnrind"
                            )
                        ],
                        transformationDescription="Renamed to CDISC SDTM LB reference range indicator",
                        transformationType="IDENTITY"
                    ),
                    "LBSTRESN": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=SILVER_TABLE,
                                field="value_numeric"
                            )
                        ],
                        transformationDescription="Renamed to CDISC SDTM LB result numeric",
                        transformationType="IDENTITY"
                    ),
                    "LBSTRESU": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=SILVER_TABLE,
                                field="unit"
                            )
                        ],
                        transformationDescription="Renamed to CDISC SDTM LB result units",
                        transformationType="IDENTITY"
                    ),
                    "STUDYID": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[],
                        transformationDescription="Added from protocol configuration (constant for this demo)",
                        transformationType="CONSTANT"
                    ),
                    "VISITNUM": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[],
                        transformationDescription="Added from protocol configuration (constant for this demo)",
                        transformationType="CONSTANT"
                    ),
                    "VISIT": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[],
                        transformationDescription="Added from protocol configuration (constant for this demo)",
                        transformationType="CONSTANT"
                    ),
                    "LBBLFL": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[],
                        transformationDescription="Added from protocol configuration (constant for this demo)",
                        transformationType="CONSTANT"
                    ),
                    "EPOCH": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[],
                        transformationDescription="Added from protocol configuration (constant for this demo)",
                        transformationType="CONSTANT"
                    ),
                    "protocol_uri": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=NAMESPACE,
                                name=SILVER_TABLE,
                                field="loinc_code"
                            )
                        ],
                        transformationDescription="Generated protocol URI linking LOINC code to study protocol",
                        transformationType="DERIVATION"
                    ),
                }
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
                )
            },
        ),
        producer=PRODUCER,
        inputs=[input_dataset],
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
