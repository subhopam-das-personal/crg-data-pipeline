import json
import logging
import os
import sys
import uuid
from pathlib import Path

import requests
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))

from .pipeline import run_full_pipeline
from .lineage import emit_pipeline_events
from .master_schema import SCHEMA

logger = logging.getLogger(__name__)

OPENLINEAGE_URL = os.getenv("OPENLINEAGE_URL", "http://localhost:5000")
SAMPLE_DIR = Path(__file__).parent.parent / "data" / "sample_fhir"

st.set_page_config(
    page_title="Clinical Data Quality, Governance & Lineage",
    page_icon="🧬",
    layout="wide",
)

# Session state init
for key in ["bundle_json", "bundle_name", "results", "run_id"]:
    if key not in st.session_state:
        st.session_state[key] = None

# ── Header ────────────────────────────────────────────────────────────────────
st.title("Clinical Data Quality, Governance & Lineage")
st.caption(
    "FHIR R4 → Bronze → Silver → Gold (CDISC SDTM LB) · OpenLineage → Marquez"
)
st.divider()

tab_upload, tab_bronze, tab_silver, tab_gold, tab_lineage, tab_registry = st.tabs(
    ["📥 Upload", "🥉 Bronze", "🥈 Silver", "🥇 Gold", "🔗 Lineage", "📖 Registry"]
)

# ── Tab 1: Upload ─────────────────────────────────────────────────────────────
with tab_upload:
    st.subheader("Load a FHIR R4 Bundle")
    st.write(
        "A real-world data vendor delivers FHIR laboratory data. "
        "Upload a Bundle JSON or pick one of the demo files below."
    )

    # Marquez connectivity check (cached for 30 seconds)
    @st.cache_data(ttl=30)
    def _check_marquez() -> tuple[bool, str]:
        """Returns (healthy: bool, error_msg: str). Never raises."""
        try:
            r = requests.get(f"{OPENLINEAGE_URL}/api/v1/namespaces", timeout=3)
            if r.status_code == 200:
                return True, ""
            return False, f"HTTP {r.status_code}"
        except Exception as e:
            return False, str(e)

    healthy, error_msg = _check_marquez()
    if healthy:
        st.success("✅ Marquez connected")
    else:
        st.warning(f"⚠️ Marquez unavailable: {error_msg}. Pipeline will run but lineage events will not be recorded.")

    col_upload, col_demo = st.columns([2, 1])

    with col_upload:
        uploaded = st.file_uploader("Upload FHIR Bundle JSON", type=["json"])
        if uploaded:
            st.session_state.bundle_json = uploaded.read().decode()
            st.session_state.bundle_name = uploaded.name
            st.session_state.results = None

    with col_demo:
        st.write("**Demo files:**")
        if st.button("✅ Clean bundle (18 obs)", use_container_width=True):
            path = SAMPLE_DIR / "clean_bundle.json"
            st.session_state.bundle_json = path.read_text()
            st.session_state.bundle_name = "clean_bundle.json"
            st.session_state.results = None
        if st.button("⚠️ Anomaly bundle (20 obs, 2 broken)", use_container_width=True):
            path = SAMPLE_DIR / "anomaly_bundle.json"
            st.session_state.bundle_json = path.read_text()
            st.session_state.bundle_name = "anomaly_bundle.json"
            st.session_state.results = None

    if st.session_state.bundle_json:
        st.success(f"Loaded: **{st.session_state.bundle_name}**")

        bundle = json.loads(st.session_state.bundle_json)
        obs_count = sum(
            1 for e in bundle.get("entry", [])
            if e.get("resource", {}).get("resourceType") == "Observation"
        )
        st.metric("Observations in bundle", obs_count)

        if st.button("▶️ Run Pipeline", type="primary", use_container_width=True):
            with st.spinner("Running Bronze → Silver → Gold..."):
                try:
                    results = run_full_pipeline(st.session_state.bundle_json)
                    run_id = str(uuid.uuid4())
                    st.session_state.results = results
                    st.session_state.run_id = run_id
                    st.success("Pipeline complete. Check the Bronze / Silver / Gold / Lineage tabs.")
                except Exception as e:
                    st.error(f"Pipeline error: {e}")
                    logger.exception("Pipeline run failed")

            # Emit lineage events AFTER spinner closes (don't block UI)
            if st.session_state.results:
                try:
                    emit_pipeline_events(run_id, results)
                except Exception as e:
                    logger.warning(f"Failed to emit lineage events: {e}")
    else:
        st.info("Select a bundle above to get started.")

# ── Tab 2: Bronze ─────────────────────────────────────────────────────────────
with tab_bronze:
    st.subheader("🥉 Bronze Layer — Raw Ingestion")
    st.caption("Data Quality — structural gate results and quarantine log")
    st.write(
        "FHIR Observation resources are loaded verbatim. "
        "Quality gates check structural integrity: non-null identifiers, "
        "valid ISO 8601 dates, and presence of a LOINC code."
    )

    results = st.session_state.results
    if not results:
        st.info("Run the pipeline in the Upload tab first.")
    else:
        b = results["bronze"]
        col1, col2, col3 = st.columns(3)
        col1.metric("Total observations", b["total"])
        col2.metric("Passed", b["passed"], delta=None)
        col3.metric("Quarantined at Bronze", b["quarantined"],
                    delta=f"-{b['quarantined']}" if b["quarantined"] else None,
                    delta_color="inverse" if b["quarantined"] else "off")

        if not b["passed_df"].empty:
            st.write("**Ingested records (Bronze layer):**")
            display_cols = ["id", "patient_id", "loinc_code", "effective_datetime", "value_numeric", "unit"]
            available = [c for c in display_cols if c in b["passed_df"].columns]
            st.dataframe(b["passed_df"][available], use_container_width=True)

        if b["quarantined_rows"]:
            st.error(f"⚠️ {b['quarantined']} record(s) quarantined at Bronze:")
            st.dataframe(b["quarantined_rows"], use_container_width=True)

        # Gate results summary
        if b.get("gate_results"):
            st.divider()
            st.write("**Quality Gate Summary:**")
            for gate_name, counts in b["gate_results"].items():
                st.write(f"- **{gate_name}**: {counts['passed']} passed, {counts['failed']} failed")

# ── Tab 3: Silver ─────────────────────────────────────────────────────────────
with tab_silver:
    st.subheader("🥈 Silver Layer — Conformed Normalization")
    st.caption("Data Quality — LOINC normalization, reference range annotation (LBNRIND)")
    st.write(
        "Nested FHIR JSON is flattened to columnar format. "
        "Quality gates apply semantic validation: every LOINC code must resolve "
        "in the master data schema. Reference ranges are annotated via LBNRIND (H/L/N/UN)."
    )

    results = st.session_state.results
    if not results:
        st.info("Run the pipeline in the Upload tab first.")
    else:
        s = results["silver"]
        col1, col2, col3 = st.columns(3)
        col1.metric("Bronze records processed", s["total"])
        col2.metric("Normalized to Silver", s["passed"])
        col3.metric("Quarantined at Silver", s["quarantined"],
                    delta=f"-{s['quarantined']}" if s["quarantined"] else None,
                    delta_color="inverse" if s["quarantined"] else "off")

        if not s["silver_df"].empty:
            st.write("**Normalized records (Silver layer):**")
            st.dataframe(s["silver_df"], use_container_width=True)

        if not s["quarantine_df"].empty:
            st.error("⚠️ Quarantine log (all layers):")
            st.dataframe(s["quarantine_df"], use_container_width=True)
            st.caption(
                "These records failed quality gates and were NOT promoted to Gold. "
                "The reason codes are also embedded in the OpenLineage DataQualityAssertionsFacet "
                "— visible in the Marquez DAG."
            )

        # Gate results summary
        if s.get("gate_results"):
            st.divider()
            st.write("**Quality Gate Summary:**")
            for gate_name, counts in s["gate_results"].items():
                st.write(f"- **{gate_name}**: {counts['passed']} passed, {counts['failed']} failed")

# ── Tab 4: Gold ───────────────────────────────────────────────────────────────
with tab_gold:
    st.subheader("🥇 Gold Layer — CDISC SDTM LB Output")
    st.caption("Governance — CDISC SDTM LB output, regulation-ready variable set")
    st.write(
        "Silver observations are joined against the LOINC master data schema "
        "to produce SDTM LB prototype output. "
        "Each row is stamped with a `protocol_uri` linking it back to the "
        "exact protocol assessment that mandated its collection."
    )

    results = st.session_state.results
    if not results:
        st.info("Run the pipeline in the Upload tab first.")
    else:
        g = results["gold"]
        col1, col2 = st.columns(2)
        col1.metric("Silver records processed", g["total"])
        col2.metric("SDTM LB rows produced", g["inserted"])

        if not g["gold_df"].empty:
            st.write("**SDTM LB dataset (Gold layer):**")
            st.dataframe(g["gold_df"], use_container_width=True)

            # CSV export
            csv_data = g["gold_df"].to_csv(index=False)
            st.download_button(
                "⬇️ Download SDTM LB (CSV)",
                data=csv_data,
                file_name="sdtm_lb.csv",
                mime="text/csv",
                use_container_width=True
            )

            with st.expander("ℹ️ SDTM variable definitions"):
                st.markdown("""
| Variable | Description |
|----------|-------------|
| `STUDYID` | Study identifier |
| `USUBJID` | Unique subject identifier (`{STUDYID}-{patient_id}`) |
| `LBTESTCD` | Short lab test code (e.g. `GLUC`) — from LOINC master schema |
| `LBTEST` | Long lab test name (e.g. `Glucose`) |
| `LBSPEC` | Specimen type (e.g. `SERUM`) — from LOINC System axis |
| `LBORRES` | Original result value as reported |
| `LBDTC` | Date/time of collection (ISO 8601) |
| `LBLOINC` | Source LOINC code — satisfies FDA LBLOINC mandate |
| `LBNRIND` | Normal range indicator: H=high, L=low, N=normal, UN=unknown |
| `VISITNUM` | Visit number (demo constant: 1) |
| `VISIT` | Visit name (demo constant: SCREENING) |
| `LBSTRESN` | Numeric result in standard units |
| `LBSTRESU` | Standard units (e.g. mg/dL) |
| `LBBLFL` | Baseline flag (demo constant: Y) |
| `EPOCH` | Epoch (demo constant: SCREENING) |
| `protocol_uri` | Machine-readable link to protocol assessment (USDM-inspired) |
                """)

# ── Tab 5: Lineage ────────────────────────────────────────────────────────────
with tab_lineage:
    st.subheader("🔗 Data Lineage — Marquez DAG")
    st.caption("Data Lineage — OpenLineage run graph with quality assertion facets")
    st.write(
        "Every pipeline run emits OpenLineage events to Marquez. "
        "The DAG shows how data flows from the FHIR bundle through Bronze, Silver, "
        "and Gold — with quality assertion results embedded at each step."
    )

    results = st.session_state.results
    run_id = st.session_state.run_id

    if results and run_id:
        col_info, col_link = st.columns([3, 1])
        with col_info:
            st.info(f"Run ID: `{run_id}`")
            st.markdown("""
**What to look for in Marquez:**
1. Open the Marquez UI (link →)
2. Navigate to **Jobs** → `silver_to_gold`
3. Click the job → **Facets** tab → `dataQualityAssertions`
4. See pass/fail counts per assertion
5. Click **Lineage** tab to see the full DAG: `fhir_bundle_upload → bronze → silver → gold`
            """)
        with col_link:
            marquez_ui_url = OPENLINEAGE_URL.replace(":5000", ":3000")
            st.link_button("Open Marquez UI →", marquez_ui_url, use_container_width=True)

        b = results["bronze"]
        s = results["silver"]
        g = results["gold"]
        total_quarantined = b["quarantined"] + s["quarantined"]

        st.divider()
        st.subheader("Run Summary")
        cols = st.columns(4)
        cols[0].metric("FHIR observations", b["total"])
        cols[1].metric("Bronze passed", b["passed"])
        cols[2].metric("Silver normalized", s["passed"])
        cols[3].metric("Gold SDTM rows", g["inserted"])

        if total_quarantined > 0:
            st.error(
                f"⚠️ {total_quarantined} record(s) quarantined across pipeline layers. "
                "See the Silver tab for details. "
                "In Marquez: job `bronze_to_silver` → DataQualityAssertionsFacet shows which assertions failed."
            )
        else:
            st.success("All records passed all quality gates.")
    else:
        st.info("Run the pipeline in the Upload tab first. Lineage events will be emitted automatically.")
        marquez_ui_url = OPENLINEAGE_URL.replace(":5000", ":3000")
        st.link_button("Open Marquez UI →", marquez_ui_url)

# ── Tab 6: Registry ───────────────────────────────────────────────────────────
with tab_registry:
    st.subheader("📖 Master Data Registry — LOINC Ontology")
    st.caption("Data Governance — LOINC master schema driving all quality gates")
    st.write(
        "This registry contains the LOINC codes that drive all quality gates. "
        "Each code includes its SDTM mapping, reference ranges, and LOINC 6-axis definition."
    )

    # Display master schema as a table
    registry_data = []
    for loinc_code, mapping in SCHEMA.items():
        registry_data.append({
            "loinc_code": loinc_code,
            "LBTESTCD": mapping["LBTESTCD"],
            "LBTEST": mapping["LBTEST"],
            "LBSPEC": mapping["LBSPEC"],
            "ref_range_low": mapping.get("ref_range_low"),
            "ref_range_high": mapping.get("ref_range_high"),
            "unit": mapping["unit"],
        })

    st.dataframe(registry_data, use_container_width=True)
    st.caption(
        "Demo LOINC master schema (8 codes). This registry drives all quality gates. "
        "Production would use the full Regenstrief LOINC catalog."
    )
