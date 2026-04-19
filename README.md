# Clinical Data Quality, Governance & Lineage

This pipeline demonstrates three clinical data management disciplines in one system:
data quality gates (structural + ontological validation at each medallion layer),
data governance (LOINC master schema as the single schema of truth, CDISC SDTM LB
as the regulatory output contract), and data lineage (OpenLineage events with
per-gate assertion facets stored in Marquez, traceable to the git commit that built
the image).

## Problem Statement

Clinical trial data arrives in fragmented, vendor-specific formats (FHIR, HL7, flat files).
To submit to regulatory agencies like the FDA, this data must be transformed into
standardized CDISC SDTM domains. This pipeline demonstrates end-to-end transformation
from FHIR R4 laboratory observations to SDTM LB (Lab Results) with full quality
gates and data lineage.

## Architecture

```
┌─────────────┐
│ FHIR Bundle │  (raw EHR data)
└──────┬──────┘
       │
       ▼
┌─────────────────────┐  Bronze Layer
│ fhir_observations   │  - Structural gates: not-null, ISO 8601, LOINC presence
│ (raw JSON stored)   │  - Quarantine failed records
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐  Silver Layer
│   observations      │  - Semantic gates: LOINC in master schema
│ (flattened,         │  - Reference range annotation (LBNRIND: H/L/N/UN)
│  normalized)        │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐  Gold Layer
│   sdtm_lb           │  - CDISC SDTM LB output format
│ (regulation-ready)  │  - VISITNUM, LBSTRESN, LBSTRESU, LBBLFL, EPOCH
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐  Data Lineage
│    Marquez          │  - OpenLineage events per layer
│   (DAG + facets)    │  - DataQualityAssertionsFacet with per-gate counts
└─────────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- Docker & Docker Compose (optional, for Marquez lineage backend)

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Start the Application
```bash
streamlit run app/main.py
```

The app will be available at **http://localhost:8501**

### Step 3: (Optional) Start Marquez for Full Lineage Tracking
```bash
docker-compose up -d
```

This starts Marquez (metadata service) and PostgreSQL for persistent lineage storage. Without it, the pipeline still works but lineage events are skipped.

### Step 4: Run Tests
```bash
pytest
```

---

## 📸 Demo Screenshots

### Home Tab - Architecture Overview
![Home Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/homepage.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776603993&Signature=RxYzr1vqdMioM5ESDdaoVMmrGDw=)

### Upload Tab - Load FHIR Data
![Upload Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/upload-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604025&Signature=GwpeC8uuVzID5N4+Qrmss8FONe4=)

### Bronze Tab - Quality Gates
![Bronze Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/bronze-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604090&Signature=ySpEPR7BGRzBS2sMG5VgDlANkf4=)

### Silver Tab - Normalized Data
![Silver Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/silver-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604108&Signature=rpOAKs5EUZaa7houa+l3tbeM3Y0=)

### Gold Tab - CDISC SDTM Output
![Gold Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/gold-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604108&Signature=ccOdDEFImGYsCiWI+62HODbUO5A=)

### Lineage Tab - Data Provenance
![Lineage Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/lineage-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604126&Signature=WWCsKGTvYEuhvX8K8owNhIaswmc=)

### Registry Tab - LOINC Master Schema
![Registry Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/registry-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604126&Signature=6uVgW3+Zdtj0m7hnWvLeI2h+Fdg=)

---

## 🔍 What to Look For in the Lineage Data

When evaluating this demo, pay attention to these key aspects of data lineage and quality governance:

### 1. **Quality Gate Assertions**
Each transformation layer (Bronze, Silver, Gold) records quality assertion facets showing:
- **Total records processed** vs. **records that passed** vs. **records quarantined**
- **Specific gate failures** (e.g., "missing LOINC code", "invalid date format")
- **Quarantine reasons** for each failed record

**Why this matters:** Regulators need to see that bad data was caught and handled appropriately, not silently dropped or corrupted.

### 2. **End-to-End Traceability**
The lineage DAG shows the complete journey from raw FHIR Bundle to SDTM LB output:
- **Source:** Which FHIR bundle was the input (with git commit SHA if running in Docker)
- **Transformations:** All jobs that touched the data (`fhir_to_bronze` → `bronze_to_silver` → `silver_to_gold`)
- **Output:** Final SDTM LB records ready for submission

**Why this matters:** When a regulator asks "Where did this value come from?" you can trace it back through every transformation step.

### 3. **Data Quality Metrics Embedded in Lineage**
OpenLineage events include `dataQualityAssertions` facets with per-gate statistics:
```json
{
  "dataQualityAssertions": {
    "totalRows": 18,
    "passedRows": 18,
    "failedRows": 0,
    "assertions": [
      {"name": "gate_1_non_null_ids", "passed": 18, "failed": 0},
      {"name": "gate_2_valid_dates", "passed": 18, "failed": 0},
      {"name": "gate_3_loinc_present", "passed": 18, "failed": 0}
    ]
  }
}
```

**Why this matters:** Quality metrics are part of the lineage record, not separate reports. This ensures quality data can't be separated from the data it describes.

### 4. **Regulatory Audit Trail**
When Marquez is running, each pipeline run includes:
- **Run ID:** Unique identifier for this execution
- **Job names:** Clear, descriptive names for each transformation
- **Input/output datasets:** Exact data sets at each stage
- **Facets:** Additional metadata (quality assertions, source code location, schema)

**Why this matters:** 21 CFR Part 11 requires complete audit trails. This lineage provides machine-readable evidence of every data transformation.

### 5. **Schema Evolution Tracking**
The LOINC Registry tab shows the master schema that drives all quality gates:
- **LOINC codes** that are allowed (currently 8 demo codes)
- **Units** and **reference ranges** for each code
- **Protocol URI** linking to the regulatory requirement

**Why this matters:** When the master schema changes, lineage shows which pipeline runs used which schema version—critical for reproducibility.

---

## 🎯 Demo Walkthrough

### Step 1: Upload FHIR Data
1. Go to the **Upload** tab
2. Click "✅ Clean bundle (18 obs)" to load demo data (all records pass quality gates)
3. OR click "⚠️ Anomaly bundle (20 obs, 2 broken)" to see quality gates in action

### Step 2: Run the Pipeline
1. Click "▶️ Run Pipeline"
2. Watch the Bronze → Silver → Gold transformation execute
3. Observe quality gate results at each layer

### Step 3: Explore Quality Gates
1. **Bronze tab:** See which records passed structural validation (IDs, dates, LOINC presence)
2. **Silver tab:** See semantic validation results (LOINC resolution, reference range annotation)
3. **Gold tab:** View the final CDISC SDTM LB output

### Step 4: Inspect Data Lineage
1. Go to the **Lineage** tab
2. Review the run summary (records processed, quarantine counts)
3. Click "Open Marquez UI →" to explore the full lineage DAG
4. In Marquez, click the **Facets** tab on any job to see quality assertion details

### Step 5: Check the Registry
1. Go to the **Registry** tab
2. See the LOINC master schema that drives all quality gates
3. Understand how reference ranges are defined and used

---

## 🐳 Docker Deployment

For production-like deployment with baked-in git commit SHA:

```bash
# Build the image
docker build --build-arg GIT_SHA=$(git rev-parse HEAD) -t clinical-lineage-demo .

# Run the container
docker run -p 8501:8501 \
  -e OPENLINEAGE_URL=http://host.docker.internal:5000 \
  clinical-lineage-demo
```

**Why GIT_SHA matters:** When baked into the image, the git commit SHA appears in lineage events, allowing you to trace data back to the exact code version that produced it.

## Demo Script (Interview Flow)

1. **Open the app** at `http://localhost:8501`

2. **Upload a FHIR bundle** - Click "✅ Clean bundle (18 obs)" to load sample data

3. **Run the pipeline** - Click "▶️ Run Pipeline" and watch the Bronze → Silver → Gold transformation

4. **Review quality gates** - Check the Bronze and Silver tabs to see:
   - Records that passed/failed structural gates (Bronze)
   - Records that passed/failed semantic validation (Silver)
   - LBNRIND annotation (H=high, L=low, N=normal, UN=unknown)

5. **Download SDTM LB output** - On the Gold tab, click "⬇️ Download SDTM LB (CSV)"

6. **Explore data lineage** - On the Lineage tab:
   - Click "Open Marquez UI →"
   - Navigate to Jobs → `silver_to_gold`
   - Click Facets tab → `dataQualityAssertions` to see per-gate pass/fail counts
   - Click Lineage tab to see the full DAG

## Marquez Fallback

If Marquez is unavailable, the pipeline still runs; lineage events are logged as warnings.
Check the Marquez connectivity pill on the Upload tab.

## Notes

- **SDTM LB output is a demo prototype.** Production requires VISITNUM from protocol,
  LBSTAT/LBREASND for missing values, and a real LOINC catalog (Regenstrief/VSAC).

- **GIT_SHA in container = commit hash baked at image build time.** Without the
  `--build-arg GIT_SHA=$(git rev-parse HEAD)` flag, SHA shows as empty in lineage events.

- **Single DuckDB connection assumed.** Run Streamlit with one user at a time.

## Scope (Current)

- Single-patient FHIR bundles
- 8 LOINC codes (demo master schema)
- Medallion architecture (Bronze → Silver → Gold)
- Quality gates at each layer
- OpenLineage + Marquez data lineage
- Streamlit UI for demo

## Deferred to Production

- Multi-patient FHIR bundles
- Full LOINC catalog (Regenstrief API or VSAC)
- Marquez PostgreSQL backing (H2 resets on restart)
- 21 CFR Part 11 audit trail
- VISITNUM from actual protocol design
- LBSTAT/LBREASND for missing values
- Multi-study support
