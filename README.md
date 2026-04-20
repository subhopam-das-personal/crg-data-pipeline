# 🧬 Clinical Data Lineage for Regulatory Submissions

[![Python](https://img.shields.io/badge/Python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.29%2B-red.svg)](https://streamlit.io/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Supported-blue.svg)](https://www.docker.com/)

> End-to-end clinical data lineage system that solves the regulatory submission puzzle.

---

## 📖 The Clinical Trial Story

### Scene: FDA Review, 3 Days Before Submission Deadline

**FDA Reviewer:** *"We see patient ID 1001's glucose value is marked 'HIGH' in your SDTM LB domain. Can you tell us:*

1. *Where did this value originally come from?*
2. *What was the LOINC code?*
3. *How did you determine it was HIGH?*
4. *Was any data quarantined during processing?*
5. *Which code version produced this output?"*

**Your Team Without This System:** *"We'll need to check with the data engineer, then the lab vendor, then cross-reference with our transformation scripts... this might take a few days."*

**Your Team With This System:** *Click → One-click lineage graph shows:*
```
LabCorp FHIR Bundle → Bronze → Silver → Gold
    "2111-8" → LBTESTCD → LBSTRESN → "H" (from LOINC reference range)
```

### The Problem This Solves

Clinical trials collect lab data from **multiple vendors** (LabCorp, Quest, Mayo Clinic, hospital EHRs) in **FHIR format**. But regulatory submissions require **CDISC SDTM** — a completely different standard.

The transformation journey is complex:
- **Source:** FHIR Observation resources with nested JSON
- **Destination:** SDTM LB domain with 16+ standardized fields
- **Risk:** Multiple transformation steps = multiple failure points

**Without lineage**, answering regulatory questions requires:
- Manual traceback through scripts
- Phone calls to lab vendors
- Hoping someone remembers which code version ran when

**With lineage**, the complete story is captured automatically:
- ✅ **Source-level traceability** — Which vendor provided the data
- ✅ **Column-level transformations** — Exactly how each field was changed
- ✅ **Quality gate evidence** — What passed vs. what was quarantined
- ✅ **Code version tracking** — Which git commit produced the output
- ✅ **Reference range provenance** — How derived values were calculated

### The Regulatory Puzzle Solved

This system implements **ALCOA+** compliance (Attributable, Legible, Contemporaneous, Original, Accurate + Complete, Consistent, Enduring, Available):

| ALCOA+ Principle | How This System Implements It |
|------------------|-------------------------------|
| **Attributable** | Every data point traces back to source (LabCorp, Quest, etc.) |
| **Legible** | Machine-readable lineage events with human-readable descriptions |
| **Contemporaneous** | Lineage captured at runtime, not retroactively documented |
| **Original** | Source dataset names reflect actual data providers |
| **Accurate** | Transformation descriptions match actual code |
| **Complete** | Quality gates record all failures, not just successes |
| **Consistent** | Standardized OpenLineage format across all transformations |
| **Enduring** | Marquez stores lineage persistently for audit trail |
| **Available** | One-click lineage graph from any pipeline run |

---

## 🏗️ Architecture: From FHIR to SDTM

```
┌─────────────────────┐
│  LabCorp FHIR Bundle│  (raw EHR data from vendor)
│  "2111-8" glucose   │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐  🥉 Bronze Layer
│ labcorp.fhir_bundle │  ✅ Structural gates: not-null, ISO 8601, LOINC
│ (raw JSON stored)   │  ⚠️  Quarantine: malformed records
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐  🥈 Silver Layer
│  silver.observations │  ✅ Semantic gates: LOINC in master schema
│ (flattened,         │  📊 Calculate LBNRIND (H/L/N/UN) from ref ranges
│  normalized)        │  ⚠️  Quarantine: unknown LOINC codes
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐  🥇 Gold Layer
│  gold.sdtm_lb       │  ✅ CDISC SDTM LB output format
│ (regulation-ready)  │  📋 VISITNUM, LBSTRESN, LBSTRESU, LBBLFL, EPOCH
│                     │  🔗 Protocol URI traceability
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐  🔗 Data Lineage (Marquez)
│  marquez database   │  ✅ OpenLineage events per layer
│                     │  📊 Column-level transformation descriptions
│                     │  🔍 Source → Destination traceability
└─────────────────────┘
```

---

## 🎯 Key Features for Regulatory Compliance

### 1️⃣ Per-Source Dataset Naming

Marquez shows distinct upstream nodes for each data provider:

```
labcorp.fhir_bundle ───┐
                       ├──→ bronze.fhir_observations ───→ silver.observations ───→ gold.sdtm_lb
quest_diagnostics.fhir_bundle ───┘
hospital_ehr.fhir_bundle ────┘
```

**Regulatory Impact:** *"Patient 1001's glucose came from LabCorp, not Quest. Here's the lineage path."*

### 2️⃣ Column-Level Lineage

Each transformation is documented with **transformation type** and **description**:

```python
# Example transformations captured in lineage:
"loinc_code" → "LBTESTCD" 
  transformationType: "LOOKUP"
  description: "Mapped from LOINC code to CDISC LB test code"

"effective_datetime" → "LBDTC"
  transformationType: "IDENTITY"
  description: "Renamed to CDISC SDTM LB date/time format"

"lbnrind" (derived field)
  transformationType: "DERIVATION"
  description: "Calculated reference range indicator (H/L/N/UN) based on LOINC reference ranges"
  inputFields: ["value_numeric", "loinc_code"]
```

**Regulatory Impact:** *"The HIGH flag came from LOINC reference ranges (70-105 mg/dL), not manual entry."*

### 3️⃣ Quality Gate Evidence

Each pipeline run records **data quality assertions**:

```json
{
  "dataQualityAssertions": {
    "assertions": [
      {
        "assertion": "schema_check",
        "success": true,
        "column": null
      },
      {
        "assertion": "range_check",
        "success": false,
        "column": null
      }
    ]
  }
}
```

**Regulatory Impact:** *"We received 10 LabCorp records. 8 passed quality gates, 2 were quarantined due to missing LOINC codes. Here are the quarantine logs."*

### 4️⃣ Protocol URI Traceability

The Gold layer includes a **protocol URI** linking LOINC codes to study protocol:

```python
"protocol_uri": "https://protocol.example.com/study/12345/loinc/2111-8"
```

**Regulatory Impact:** *"This LOINC code is part of the protocol's glucose monitoring requirement. Here's the protocol specification."*

### 5️⃣ Git Commit Traceability

When deployed via Docker, the **git commit SHA** is baked into lineage events:

```python
"sourceCode": {
  "language": "Python",
  "source": "https://github.com/subhopam-das-personal/crg-data-pipeline"
}
# GIT_SHA available in container metadata
```

**Regulatory Impact:** *"This output was produced by commit 8d84c75. Here's the exact code that ran."*

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

## 🌐 Live Demo

**Try the live demo:** https://ravishing-learning-production-042f.up.railway.app/

The live demo showcases all features:
- 🏠 **Home Tab** — Architecture overview and clinical trial story
- 📥 **Upload Tab** — Load FHIR bundles or use demo data
- 🥉 **Bronze Tab** — Quality gates and quarantine tracking
- 🥈 **Silver Tab** — Normalized data with reference range indicators
- 🥇 **Gold Tab** — CDISC SDTM LB output with download
- 🔗 **Lineage Tab** — Data provenance and Marquez integration
- 📖 **Registry Tab** — LOINC master schema inspection

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
- ✅ Per-source dataset naming (labcorp.fhir_bundle, quest_diagnostics.fhir_bundle, etc.)
- ✅ 8 LOINC codes (demo master schema)
- ✅ Medallion architecture (Bronze → Silver → Gold)
- ✅ Quality gates at each layer
- ✅ OpenLineage + Marquez data lineage
- ✅ Column-level transformation tracking
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
