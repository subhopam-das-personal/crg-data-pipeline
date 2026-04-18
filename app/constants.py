"""Constants for the clinical data lineage pipeline."""

STUDY_ID = "NCT12345"
NAMESPACE = "clinical-lineage-demo"
PRODUCER = "https://github.com/subhopam/data_lineage"

# Demo constants (would come from protocol/study design in production)
VISITNUM = 1
VISIT = "SCREENING"
EPOCH = "SCREENING"
LBBLFL = "Y"  # Baseline flag - all records are baseline in this single-visit demo

# Schema names
BRONZE_TABLE = "bronze.fhir_observations"
SILVER_TABLE = "silver.observations"
GOLD_TABLE = "gold.sdtm_lb"
QUARANTINE_TABLE = "silver.quarantine"

# Job names for lineage
JOB_FHIR_TO_BRONZE = "fhir_to_bronze"
JOB_BRONZE_TO_SILVER = "bronze_to_silver"
JOB_SILVER_TO_GOLD = "silver_to_gold"

# Input dataset names
INPUT_FHIR_BUNDLE = "fhir_bundle_upload"
