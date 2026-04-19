"""
FastMCP Server for Clinical Data Lineage.

Exposes LOINC ontology and pipeline lineage data to AI agents via the Model Context Protocol.
"""

import json
import os
import sys
from pathlib import Path

import requests
from fastmcp import FastMCP

# Add app directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from master_schema import SCHEMA, get_mapping
from constants import NAMESPACE

MARQUEZ_URL = os.getenv("OPENLINEAGE_URL", "http://localhost:5000")

mcp = FastMCP("clinical-lineage-demo")


@mcp.tool()
def get_loinc_definition(code: str) -> dict:
    """
    Get LOINC 6-axis definition for a code.

    Args:
        code: LOINC code (e.g., "2345-7")

    Returns:
        Dict with component, property, time_aspect, system, scale, and SDTM mapping.
        Returns {"error": "not_found"} if code is not in the master schema.
    """
    mapping = get_mapping(code)
    if not mapping:
        return {"error": "not_found", "code": code}

    return {
        "loinc_code": code,
        "component": mapping.get("component"),
        "property": mapping.get("property"),
        "time_aspect": mapping.get("time_aspect"),
        "system": mapping.get("system"),
        "scale": mapping.get("scale"),
        "ref_range_low": mapping.get("ref_range_low"),
        "ref_range_high": mapping.get("ref_range_high"),
        "unit": mapping.get("unit"),
        "lbtestcd": mapping.get("LBTESTCD"),
        "lbtest": mapping.get("LBTEST"),
        "lbspec": mapping.get("LBSPEC"),
    }


@mcp.tool()
def get_reference_ranges(code: str) -> dict:
    """
    Get reference range for a LOINC code.

    Args:
        code: LOINC code (e.g., "2345-7")

    Returns:
        Dict with ref_range_low, ref_range_high, and unit.
        Returns {"error": "not_found"} if code is not in the master schema.
    """
    mapping = get_mapping(code)
    if not mapping:
        return {"error": "not_found", "code": code}

    return {
        "loinc_code": code,
        "ref_range_low": mapping.get("ref_range_low"),
        "ref_range_high": mapping.get("ref_range_high"),
        "unit": mapping.get("unit"),
    }


@mcp.tool()
def get_pipeline_runs(namespace: str = NAMESPACE) -> dict:
    """
    Get recent pipeline runs from Marquez.

    Args:
        namespace: Marquez namespace (default: "clinical-lineage-demo")

    Returns:
        Dict with run information or {"error": str} if Marquez is unreachable.
    """
    try:
        # Get all datasets in the namespace
        response = requests.get(f"{MARQUEZ_URL}/api/v1/namespaces/{namespace}/jobs", timeout=5)
        response.raise_for_status()
        jobs = response.json()

        runs = []
        for job in jobs:
            job_name = job.get("name")
            # Get recent runs for this job
            runs_response = requests.get(
                f"{MARQUEZ_URL}/api/v1/namespaces/{namespace}/jobs/{job_name}/runs",
                timeout=5
            )
            if runs_response.ok:
                job_runs = runs_response.json()
                runs.extend([
                    {
                        "job": job_name,
                        "run_id": r.get("id"),
                        "state": r.get("state"),
                        "started_at": r.get("startedAt"),
                        "ended_at": r.get("endedAt"),
                    }
                    for r in job_runs[:5]  # Last 5 runs per job
                ])

        return {"runs": runs}

    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_run_quality_assertions(run_id: str) -> dict:
    """
    Get quality assertion facets for a specific pipeline run.

    Args:
        run_id: OpenLineage run ID

    Returns:
        Dict with per-gate pass/fail counts from DataQualityAssertionsFacet.
        Returns {"error": "not_found"} if run is not found.
    """
    try:
        # Get run details
        response = requests.get(f"{MARQUEZ_URL}/api/v1/runs/{run_id}", timeout=5)
        response.raise_for_status()
        run_data = response.json()

        # Extract facets
        facets = run_data.get("facets", {})
        assertion_facet = facets.get("dataQualityAssertions", {})

        assertions = []
        for assertion in assertion_facet.get("assertions", []):
            assertions.append({
                "assertion": assertion.get("assertion"),
                "success": assertion.get("success"),
                "column": assertion.get("column"),
            })

        return {
            "run_id": run_id,
            "job_name": run_data.get("job", {}).get("name"),
            "state": run_data.get("state"),
            "assertions": assertions,
        }

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return {"error": "not_found", "run_id": run_id}
        return {"error": str(e)}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_dataset_lineage(dataset: str) -> dict:
    """
    Get full lineage graph for a dataset.

    Args:
        dataset: Dataset name (e.g., "gold.sdtm_lb")

    Returns:
        Dict with upstream and downstream dependencies.
        Returns {"error": str} on failure.
    """
    try:
        response = requests.get(f"{MARQUEZ_URL}/api/v1/lineage", params={"dataset": dataset}, timeout=5)
        response.raise_for_status()
        lineage_data = response.json()

        return {
            "dataset": dataset,
            "upstream": lineage_data.get("edges", {}).get("inputs", []),
            "downstream": lineage_data.get("edges", {}).get("outputs", []),
        }

    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def list_all_loinc_codes() -> dict:
    """
    List all LOINC codes in the master schema.

    Returns:
        Dict with list of all LOINC codes and their LBTESTCD mappings.
    """
    codes = []
    for loinc_code, mapping in SCHEMA.items():
        codes.append({
            "loinc_code": loinc_code,
            "lbtestcd": mapping.get("LBTESTCD"),
            "lbtest": mapping.get("LBTEST"),
        })

    return {"codes": codes}


if __name__ == "__main__":
    # Run the MCP server
    mcp.run()
