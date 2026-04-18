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

## Setup

### Prerequisites
- Python 3.12+
- Docker & Docker Compose (for Marquez)

### Local Development

1. Clone the repository:
```bash
git clone https://github.com/subhopam-das-personal/crg-data-pipeline.git
cd crg-data-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Start Marquez (data lineage backend):
```bash
docker-compose up -d
```

4. Run the Streamlit app:
```bash
streamlit run app/main.py
```

5. Run tests:
```bash
pytest
```

### Docker Deployment

Build the image with GIT_SHA baked in:
```bash
docker build --build-arg GIT_SHA=$(git rev-parse HEAD) -t clinical-lineage-demo .
```

Run the container:
```bash
docker run -p 8501:8501 \
  -e OPENLINEAGE_URL=http://host.docker.internal:5000 \
  clinical-lineage-demo
```

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
