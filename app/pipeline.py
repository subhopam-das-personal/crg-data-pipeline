import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from .master_schema import SCHEMA, get_mapping
from .quality import run_bronze_gates, run_silver_gates, check_reference_range, all_passed, first_failure_reason, QualityResult
from .constants import (
    STUDY_ID, VISITNUM, VISIT, EPOCH, LBBLFL,
    BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE, QUARANTINE_TABLE
)

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "lineage.duckdb"


def get_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            id          TEXT PRIMARY KEY,
            patient_id  TEXT,
            status      TEXT,
            loinc_code  TEXT,
            effective_datetime TEXT,
            value_numeric DOUBLE,
            unit        TEXT,
            raw_json    TEXT NOT NULL,
            loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            id               TEXT PRIMARY KEY,
            patient_id       TEXT NOT NULL,
            loinc_code       TEXT NOT NULL,
            effective_dt     TEXT NOT NULL,
            value_numeric    DOUBLE,
            unit             TEXT,
            lbnrind          TEXT
        )
    """)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            id            TEXT,
            layer         TEXT NOT NULL,
            reason        TEXT NOT NULL,
            failed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            STUDYID      TEXT,
            USUBJID      TEXT NOT NULL,
            LBTESTCD     TEXT NOT NULL,
            LBTEST       TEXT,
            LBSPEC       TEXT,
            LBORRES      TEXT,
            LBDTC        TEXT,
            LBLOINC      TEXT,
            LBNRIND      TEXT,
            VISITNUM     INTEGER,
            VISIT        TEXT,
            LBSTRESN     DOUBLE,
            LBSTRESU     TEXT,
            LBBLFL       TEXT,
            EPOCH        TEXT,
            protocol_uri TEXT,
            loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (USUBJID, LBTESTCD, LBDTC)
        )
    """)


def reset_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Clear all tables for a fresh pipeline run."""
    conn.execute(f"DELETE FROM {BRONZE_TABLE}")
    conn.execute(f"DELETE FROM {SILVER_TABLE}")
    conn.execute(f"DELETE FROM {QUARANTINE_TABLE}")
    conn.execute(f"DELETE FROM {GOLD_TABLE}")


def _extract_observations(bundle: dict) -> list[dict]:
    """Pull Observation resources from a FHIR Bundle. Returns flat dicts."""
    patient_id = None
    observations = []

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            identifiers = resource.get("identifier", [])
            patient_id = identifiers[0]["value"] if identifiers else resource.get("id")

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") != "Observation":
            continue

        coding = resource.get("code", {}).get("coding", [{}])
        loinc_code = coding[0].get("code") if coding else None

        vq = resource.get("valueQuantity", {})
        observations.append({
            "id": resource.get("id", str(uuid.uuid4())),
            "patient_id": patient_id,
            "status": resource.get("status"),
            "loinc_code": loinc_code,
            "effective_datetime": resource.get("effectiveDateTime"),
            "value_numeric": vq.get("value"),
            "unit": vq.get("unit"),
            "raw_json": json.dumps(resource),
        })
    return observations


def _aggregate_gate_results(gates_list: list[list[QualityResult]]) -> dict:
    """
    Aggregate pass/fail counts across all rows for each assertion type.

    Returns dict like:
    {
        "not_null:Patient.identifier": {"passed": int, "failed": int},
        "iso8601:Observation.effectiveDateTime": {"passed": int, "failed": int},
        ...
    }
    """
    result = {}
    for gates in gates_list:
        for gate in gates:
            key = gate.assertion
            if key not in result:
                result[key] = {"passed": 0, "failed": 0}
            if gate.passed:
                result[key]["passed"] += 1
            else:
                result[key]["failed"] += 1
    return result


def run_bronze(bundle_json: str, conn: duckdb.DuckDBPyConnection) -> dict:
    """Ingest FHIR bundle into bronze layer. Returns stats."""
    bundle = json.loads(bundle_json)
    observations = _extract_observations(bundle)

    passed_rows, quarantined_rows, all_gates = [], [], []

    for obs in observations:
        gates = run_bronze_gates(obs)
        all_gates.append(gates)
        if all_passed(gates):
            passed_rows.append(obs)
        else:
            reason = first_failure_reason(gates)
            conn.execute(
                f"INSERT INTO {QUARANTINE_TABLE} (id, layer, reason) VALUES (?, ?, ?)",
                [obs["id"], "bronze", reason],
            )
            quarantined_rows.append({**obs, "reason": reason})

    for row in passed_rows:
        conn.execute(
            f"""INSERT INTO {BRONZE_TABLE}
               (id, patient_id, status, loinc_code, effective_datetime, value_numeric, unit, raw_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [row["id"], row["patient_id"], row["status"], row["loinc_code"],
             row["effective_datetime"], row["value_numeric"], row["unit"], row["raw_json"]],
        )

    return {
        "total": len(observations),
        "passed": len(passed_rows),
        "quarantined": len(quarantined_rows),
        "quarantined_rows": quarantined_rows,
        "passed_df": pd.DataFrame(passed_rows) if passed_rows else pd.DataFrame(),
        "gate_results": _aggregate_gate_results(all_gates),
    }


def run_silver(conn: duckdb.DuckDBPyConnection) -> dict:
    """Normalize bronze → silver. Returns stats."""
    bronze_df = conn.execute(f"SELECT * FROM {BRONZE_TABLE}").df()

    passed_rows, quarantined_rows, all_gates = [], [], []

    for _, row in bronze_df.iterrows():
        gates = run_silver_gates(row.to_dict(), SCHEMA)
        all_gates.append(gates)
        if all_passed(gates):
            passed_rows.append(row.to_dict())
        else:
            reason = first_failure_reason(gates)
            conn.execute(
                f"INSERT INTO {QUARANTINE_TABLE} (id, layer, reason) VALUES (?, ?, ?)",
                [row["id"], "silver", reason],
            )
            quarantined_rows.append({**row.to_dict(), "reason": reason})

    for row in passed_rows:
        # Annotate with LBNRIND - this is NOT a gate, just annotation
        loinc_code = row["loinc_code"]
        mapping = get_mapping(loinc_code)
        lbnrind = "UN"
        if mapping:
            lbnrind = check_reference_range(row["value_numeric"], mapping)

        conn.execute(
            f"""INSERT INTO {SILVER_TABLE}
               (id, patient_id, loinc_code, effective_dt, value_numeric, unit, lbnrind)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [row["id"], row["patient_id"], row["loinc_code"],
             row["effective_datetime"], row["value_numeric"], row["unit"], lbnrind],
        )

    silver_df = conn.execute(f"SELECT * FROM {SILVER_TABLE}").df()
    quarantine_df = conn.execute(f"SELECT * FROM {QUARANTINE_TABLE}").df()

    return {
        "total": len(bronze_df),
        "passed": len(passed_rows),
        "quarantined": len(quarantined_rows),
        "quarantined_rows": quarantined_rows,
        "silver_df": silver_df,
        "quarantine_df": quarantine_df,
        "gate_results": _aggregate_gate_results(all_gates),
    }


def run_gold(conn: duckdb.DuckDBPyConnection) -> dict:
    """Produce SDTM LB output from silver layer. Returns stats."""
    silver_df = conn.execute(f"SELECT * FROM {SILVER_TABLE}").df()

    gold_rows = []
    for _, row in silver_df.iterrows():
        mapping = get_mapping(row["loinc_code"])
        if not mapping:
            continue

        pid = row["patient_id"] or ""
        usubjid = pid if pid.startswith(STUDY_ID) else f"{STUDY_ID}-{pid}"
        lborres = str(row["value_numeric"]) if row["value_numeric"] is not None else ""
        protocol_uri = f"study://{STUDY_ID}/lab/loinc/{row['loinc_code']}/baseline"

        gold_rows.append({
            "STUDYID": STUDY_ID,
            "USUBJID": usubjid,
            "LBTESTCD": mapping["LBTESTCD"],
            "LBTEST": mapping["LBTEST"],
            "LBSPEC": mapping["LBSPEC"],
            "LBORRES": lborres,
            "LBDTC": row["effective_dt"],
            "LBLOINC": row["loinc_code"],
            "LBNRIND": row["lbnrind"],
            "VISITNUM": VISITNUM,
            "VISIT": VISIT,
            "LBSTRESN": row["value_numeric"],
            "LBSTRESU": row["unit"],
            "LBBLFL": LBBLFL,
            "EPOCH": EPOCH,
            "protocol_uri": protocol_uri,
        })

    inserted = 0
    for row in gold_rows:
        try:
            conn.execute(
                f"""INSERT INTO {GOLD_TABLE}
                   (STUDYID, USUBJID, LBTESTCD, LBTEST, LBSPEC, LBORRES, LBDTC, LBLOINC, LBNRIND,
                    VISITNUM, VISIT, LBSTRESN, LBSTRESU, LBBLFL, EPOCH, protocol_uri)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [row["STUDYID"], row["USUBJID"], row["LBTESTCD"], row["LBTEST"],
                 row["LBSPEC"], row["LBORRES"], row["LBDTC"], row["LBLOINC"], row["LBNRIND"],
                 row["VISITNUM"], row["VISIT"], row["LBSTRESN"], row["LBSTRESU"],
                 row["LBBLFL"], row["EPOCH"], row["protocol_uri"]],
            )
            inserted += 1
        except Exception as e:
            logger.warning(f"Failed to insert Gold row {row['USUBJID']}/{row['LBTESTCD']}: {e}")

    gold_df = conn.execute(f"SELECT * FROM {GOLD_TABLE}").df()

    return {
        "total": len(silver_df),
        "inserted": inserted,
        "gold_df": gold_df,
    }


def run_full_pipeline(bundle_json: str) -> dict:
    """Run end-to-end pipeline. Returns all layer results."""
    conn = get_conn()
    init_db(conn)
    reset_tables(conn)

    bronze_result = run_bronze(bundle_json, conn)
    silver_result = run_silver(conn)
    gold_result = run_gold(conn)

    conn.close()
    return {
        "bronze": bronze_result,
        "silver": silver_result,
        "gold": gold_result,
    }
