# 🧬 Clinical Data Quality, Governance & Lineage

[![Python](https://img.shields.io/badge/Python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.29%2B-red.svg)](https://streamlit.io/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Supported-blue.svg)](https://www.docker.com/)

> End-to-end clinical data engineering pipeline with quality gates, governance, and OpenLineage tracking for CDISC SDTM regulatory submissions.

---

## 📋 Overview

This pipeline demonstrates three critical clinical data management disciplines in a unified system:

- **🔍 Data Quality Gates** — Structural and ontological validation at each medallion layer (Bronze → Silver → Gold)
- **📋 Data Governance** — LOINC master schema as the single source of truth, CDISC SDTM LB as regulatory output
- **🔗 Data Lineage** — OpenLineage events with per-gate assertion facets stored in Marquez, traceable to git commit

### 🎯 Problem Statement

Clinical trial data arrives in fragmented, vendor-specific formats (FHIR, HL7, flat files). To submit to regulatory agencies like the FDA, this data must be transformed into standardized CDISC SDTM domains. This pipeline demonstrates end-to-end transformation from **FHIR R4 laboratory observations → SDTM LB (Lab Results)** with full quality gates and data lineage.

---

## 🏗️ Architecture

```
┌─────────────┐
│ FHIR Bundle │  (raw EHR data)
└──────┬──────┘
       │
       ▼
┌─────────────────────┐  🥉 Bronze Layer
│ fhir_observations   │  ✅ Structural gates: not-null, ISO 8601, LOINC presence
│ (raw JSON stored)   │  ⚠️  Quarantine failed records
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐  🥈 Silver Layer
│   observations      │  ✅ Semantic gates: LOINC in master schema
│ (flattened,         │  📊 Reference range annotation (LBNRIND: H/L/N/UN)
│  normalized)        │  ⚠️  Quarantine failed records
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐  🥇 Gold Layer
│   sdtm_lb           │  ✅ CDISC SDTM LB output format
│ (regulation-ready)  │  📋 VISITNUM, LBSTRESN, LBSTRESU, LBBLFL, EPOCH
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐  🔗 Data Lineage
│    Marquez          │  ✅ OpenLineage events per layer
│   (DAG + facets)    │  📊 DataQualityAssertionsFacet with per-gate counts
└─────────────────────┘
```

---

## 🚀 Quick Start

### 📦 Prerequisites

- **Python 3.12+** — [Download](https://www.python.org/downloads/)
- **Docker & Docker Compose** (optional, for Marquez) — [Install Docker](https://docs.docker.com/get-docker/)

### ⚡ Installation

```bash
# Clone the repository
git clone https://github.com/subhopam-das-personal/crg-data-pipeline.git
cd crg-data-pipeline

# Install dependencies
pip install -r requirements.txt
```

### 🎬 Running the Application

```bash
# Start the Streamlit app
streamlit run app/main.py
```

The application will be available at **http://localhost:8501**

### 🔗 Optional: Enable Full Lineage Tracking

```bash
# Start Marquez (metadata service) and PostgreSQL
docker-compose up -d
```

> **💡 Note:** Marquez is optional. Without it, the pipeline works normally but lineage events are skipped.

### 🧪 Running Tests

```bash
pytest
```

---

## 📸 Demo Screenshots

### 🏠 Home Tab - Architecture Overview

![Home Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/homepage.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776603993&Signature=RxYzr1vqdMioM5ESDdaoVMmrGDw=)

Comprehensive explanation of the pipeline architecture, data flow, and use case with clear storytelling approach.

---

### 📥 Upload Tab - Load FHIR Data

![Upload Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/upload-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604025&Signature=GwpeC8uuVzID5N4+Qrmss8FONe4=)

Upload FHIR R4 Bundle JSON files or use demo bundles. Includes Marquez connectivity status.

---

### 🥉 Bronze Tab - Quality Gates

![Bronze Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/bronze-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604090&Signature=ySpEPR7BGRzBS2sMG5VgDlANkf4=)

View records that passed structural validation and those quarantined with failure reasons.

---

### 🥈 Silver Tab - Normalized Data

![Silver Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/silver-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604108&Signature=rpOAKs5EUZaa7houa+l3tbeM3Y0=)

See normalized data with reference range indicators (H=high, L=low, N=normal, UN=unknown).

---

### 🥇 Gold Tab - CDISC SDTM Output

![Gold Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/gold-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604108&Signature=ccOdDEFImGYsCiWI+62HODbUO5A=)

View FDA-ready SDTM LB output with protocol URI links and download as CSV.

---

### 🔗 Lineage Tab - Data Provenance

![Lineage Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/lineage-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604126&Signature=WWCsKGTvYEuhvX8K8owNhIaswmc=)

Explore the complete data lineage DAG with quality assertion summaries and deep links to Marquez.

---

### 📖 Registry Tab - LOINC Master Schema

![Registry Tab](https://maas-log-prod.cn-wlcb.ufileos.com/anthropic/0cfa1dd7-915b-4846-836d-6178181c5572/registry-tab.png?UCloudPublicKey=TOKEN_e15ba47a-d098-4fbd-9afc-a0dcf0e4e621&Expires=1776604126&Signature=6uVgW3+Zdtj0m7hnWvLeI2h+Fdg=)

Inspect the LOINC master schema driving all quality gates (8 demo codes).

---

## 🔍 What to Look For in the Lineage Data

When evaluating this demo for clinical data governance, pay attention to these key aspects:

### 1️⃣ Quality Gate Assertions

Each transformation layer records **data quality assertions** showing:
- ✅ **Total records processed** vs. **records passed** vs. **records quarantined**
- ❌ **Specific gate failures** (e.g., "missing LOINC code", "invalid date format")
- 📋 **Quarantine reasons** for each failed record

> **🎯 Why this matters:** Regulators need to see that bad data was caught and handled appropriately, not silently dropped or corrupted.

### 2️⃣ End-to-End Traceability

The lineage DAG shows the complete journey from raw FHIR Bundle to SDTM LB output:
- 📥 **Source:** Which FHIR bundle was the input (with git commit SHA if running in Docker)
- 🔧 **Transformations:** All jobs that touched the data (`fhir_to_bronze` → `bronze_to_silver` → `silver_to_gold`)
- 📤 **Output:** Final SDTM LB records ready for submission

> **🎯 Why this matters:** When a regulator asks "Where did this value come from?" you can trace it back through every transformation step.

### 3️⃣ Data Quality Metrics Embedded in Lineage

OpenLineage events include **`dataQualityAssertions`** facets with per-gate statistics:

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

> **🎯 Why this matters:** Quality metrics are part of the lineage record, not separate reports. This ensures quality data can't be separated from the data it describes.

### 4️⃣ Regulatory Audit Trail

When Marquez is running, each pipeline run includes:
- 🆔 **Run ID:** Unique identifier for this execution
- 🔨 **Job names:** Clear, descriptive names for each transformation
- 📊 **Input/output datasets:** Exact data sets at each stage
- 📋 **Facets:** Additional metadata (quality assertions, source code location, schema)

> **🎯 Why this matters:** 21 CFR Part 11 requires complete audit trails. This lineage provides machine-readable evidence of every data transformation.

### 5️⃣ Schema Evolution Tracking

The LOINC Registry tab shows the master schema that drives all quality gates:
- 🧬 **LOINC codes** that are allowed (currently 8 demo codes)
- 📏 **Units** and **reference ranges** for each code
- 🔗 **Protocol URI** linking to the regulatory requirement

> **🎯 Why this matters:** When the master schema changes, lineage shows which pipeline runs used which schema version—critical for reproducibility.

---

## 🎯 Demo Walkthrough

### Step 1️⃣: Upload FHIR Data

1. Go to the **📥 Upload** tab
2. Click **"✅ Clean bundle (18 obs)"** to load demo data (all records pass quality gates)
3. OR click **"⚠️ Anomaly bundle (20 obs, 2 broken)"** to see quality gates in action

### Step 2️⃣: Run the Pipeline

1. Click **"▶️ Run Pipeline"**
2. Watch the **Bronze → Silver → Gold** transformation execute
3. Observe quality gate results at each layer

### Step 3️⃣: Explore Quality Gates

1. **🥉 Bronze tab:** See which records passed structural validation (IDs, dates, LOINC presence)
2. **🥈 Silver tab:** See semantic validation results (LOINC resolution, reference range annotation)
3. **🥇 Gold tab:** View the final CDISC SDTM LB output

### Step 4️⃣: Inspect Data Lineage

1. Go to the **🔗 Lineage** tab
2. Review the run summary (records processed, quarantine counts)
3. Click **"Open Marquez UI →"** to explore the full lineage DAG
4. In Marquez, click the **Facets** tab on any job to see quality assertion details

### Step 5️⃣: Check the Registry

1. Go to the **📖 Registry** tab
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

> **💡 Why GIT_SHA matters:** When baked into the image, the git commit SHA appears in lineage events, allowing you to trace data back to the exact code version that produced it.

---

## 📊 Medallion Architecture Explained

### 🥉 Bronze Layer — Raw Ingestion

**Purpose:** Store FHIR data exactly as received

**Quality Gates:**
1. ✅ Gate 1: Non-null patient & observation IDs
2. ✅ Gate 2: Valid ISO 8601 effective date
3. ✅ Gate 3: LOINC code present

**Output:** Raw FHIR JSON + quarantine log

### 🥈 Silver Layer — Semantic Normalization

**Purpose:** Flatten and semantically validate data

**Transformations:**
- Flatten nested FHIR JSON → columnar format
- Resolve LOINC codes against master schema
- Annotate reference range indicator (LBNRIND)

**Quality Gates:**
4. ✅ Gate 4: LOINC code resolves in master schema

**Output:** Normalized observations + quarantine log

### 🥇 Gold Layer — Regulatory Output

**Purpose:** Produce FDA-ready CDISC SDTM LB

**Transformations:**
- Join Silver against LOINC master schema
- Map to SDTM LB variable set
- Stamp with `protocol_uri`

**Output:** CDISC SDTM LB CSV

---

## 📚 Technical Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **UI** | Streamlit 1.29+ | Web application interface |
| **Data Processing** | Python 3.12+ | Pipeline logic |
| **Data Storage** | DuckDB | In-memory OLAP database |
| **Lineage** | OpenLineage | Data lineage specification |
| **Metadata** | Marquez | Open-source metadata service |
| **Database** | PostgreSQL | Persistent storage for Marquez |
| **Container** | Docker | Deployment and isolation |

---

## 🔄 Pipeline Workflow

```
FHIR Bundle (JSON)
    ↓
[Structural Validation]
    ├─ ✅ Pass → fhir_observations table
    └─ ❌ Fail → quarantine log
    ↓
[Semantic Normalization]
    ├─ ✅ Pass → observations table
    └─ ❌ Fail → quarantine log
    ↓
[CDISC SDTM Transformation]
    └─ ✅ → sdtm_lb table (CSV export)
```

**Each step emits OpenLineage events** with quality assertion facets.

---

## ⚠️ Notes & Limitations

- **SDTM LB output is a demo prototype.** Production requires:
  - VISITNUM from protocol design
  - LBSTAT/LBREASND for missing values
  - Real LOINC catalog (Regenstrief/VSAC)
- **GIT_SHA in container = commit hash baked at image build time.** Without the `--build-arg GIT_SHA=$(git rev-parse HEAD)` flag, SHA shows as empty in lineage events.
- **Single DuckDB connection assumed.** Run Streamlit with one user at a time.

---

## 🎯 Current Scope

- ✅ Single-patient FHIR bundles
- ✅ 8 LOINC codes (demo master schema)
- ✅ Medallion architecture (Bronze → Silver → Gold)
- ✅ Quality gates at each layer
- ✅ OpenLineage + Marquez data lineage
- ✅ Streamlit UI for demo

---

## 🚧 Deferred to Production

- ⏳ Multi-patient FHIR bundles
- ⏳ Full LOINC catalog (Regenstrief API or VSAC)
- ⏳ Marquez PostgreSQL backing (H2 resets on restart)
- ⏳ 21 CFR Part 11 audit trail
- ⏳ VISITNUM from actual protocol design
- ⏳ LBSTAT/LBREASND for missing values
- ⏳ Multi-study support

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📧 Contact

- **Repository:** https://github.com/subhopam-das-personal/crg-data-pipeline
- **Issues:** https://github.com/subhopam-das-personal/crg-data-pipeline/issues

---

## 🙏 Acknowledgments

- [OpenLineage](https://openlineage.io/) — Data lineage specification
- [Marquez](https://github.com/MarquezProject/marquez) — Open-source metadata service
- [Streamlit](https://streamlit.io/) — Python web app framework
- [CDISC](https://www.cdisc.org/) — Clinical data interchange standards
- [LOINC](https://loinc.org/) - Logical Observation Identifiers Names and Codes

---

**🎉 Built with ❤️ for clinical data quality and regulatory compliance**
