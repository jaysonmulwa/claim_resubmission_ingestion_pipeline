import pandas as pd
import json
import logging
from prefect import flow, task

CSV_INPUT_FILE = "./uploads/emr_alpha.csv"
JSON_INPUT_FILE= "./uploads/emr_beta.json"
RESUBMISSION_CANDIDATES_JSON_FILE = "./outputs/resubmission_candidates.json"
FAILED_RECORDS_OUTPUT_FILE = "./outputs/failed_records.json"
CLAIM_METRICS_OUTPUT_FILE = "./outputs/claims_metrics.json"
METRICS = {
    "total_claims": 0,
    "total_claims_alpha_csv": 0,
    "total_claims_beta_json": 0,
    "total_resubmission_eligible": 0,
    "total_approved": 0,
    "total_failed": 0
}

@task(log_prints=True)
def read_json():
    return pd.read_json(JSON_INPUT_FILE)

@task(log_prints=True)
def read_csv():
    return pd.read_csv(CSV_INPUT_FILE)


def unified_schema_map():
    return {
        "id": "claim_id",
        "member": "patient_id",
        "code": "procedure_code",
        "error_msg": "denial_reason",
        "status": "status",
        "date": "submitted_at",
        "source_system": "alpha or beta"
    }

@task(log_prints=True)
def transform_data(df, source):
    schema_map = unified_schema_map()

    # unify schema
    df_transformed = df.rename(columns=schema_map)

    # Handling null/None values for "denial_reason" and "patient_id"
    df_transformed.fillna({ "denial_reason": "null"}, inplace=True)
    df_transformed.fillna({ "patient_id": "null"}, inplace=True)
    
    # Ensure date is in ISO format (already in ISO format, but we can standardize it)
    df_transformed["submitted_at"] = pd.to_datetime(df_transformed["submitted_at"]).dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Status should be either "approved" or "denied" – no changes needed, but can clean up
    df_transformed["status"] = df_transformed["status"].apply(lambda x: x if x in ["approved", "denied"] else "denied")

    # Adding the new field 'source_system' with a constant value
    df_transformed["source_system"] = source

    return df_transformed

@task(log_prints=True)
def check_resubmission(row):
    """
    Check resubmission eligibility per Column
    """
    # Check if submitted more than 7 days ago
    submitted_date = pd.to_datetime(row["submitted_at"])
    today = pd.to_datetime('2025-07-30')
    days_since_submission = (today - submitted_date).days

    if row["status"] == "denied" and row["patient_id"] != "null" and days_since_submission > 7: 

            # Fetch denial reason       
            denial_reason = row["denial_reason"].lower() if row["denial_reason"] != "null" else ""
            
            # Check if denial reason is a known retryable reason
            known_retryable_reasons = [
                "Missing modifier", "Incorrect NPI", "Prior auth required"
            ]

            # Check for known retryable reasons
            for reason in known_retryable_reasons:
                if reason.lower() == denial_reason:
                    return True
            
            # Heuristic classifier for ambiguous cases
            ambiguous_examples = ["incorrect procedure", "form incomplete", "not billable", "null"]
            for example in ambiguous_examples:
                if example.lower() == denial_reason:
                    return True
    
    return False 

@task(log_prints=True)
def check_failed(row):

    # Fetch denial reason       
    denial_reason = row["denial_reason"].lower() if row["denial_reason"] != "null" else ""

    known_non_retryable_reasons = [
        "Authorization expired", "Incorrect provider type"

    ]

    # Check for known retryable reasons
    for reason in known_non_retryable_reasons:
        if reason.lower() == denial_reason:
            return True
        
    
    return False

@task(log_prints=True)
def resubmission_logic(df):
    """
    A claim should be flagged for resubmission if all the following are true:
    1. Status is denied
    2. Patient ID is not null
    3. The claim was submitted more than 7 days ago
    (assume today is 2025-07-30)
    4. The denial reason is either:
    o A known retryable reason (see below),
    o OR inferred as retryable via LLM/heuristic
    classifier if ambiguous
    """
    df_flaggable = df.copy()
    df_flaggable['resubmission_eligible'] = df_flaggable.apply(check_resubmission, axis=1)
    return df_flaggable

@task(log_prints=True)
def automated_resubmission_output(df):
    """
    Produce a list of claims eligible for automated resubmission, including:
    """
    # Filter out resubmission_eligible
    df_resubmission = df[df['resubmission_eligible'] == True]
    print(df_resubmission)

    # denial_reason to resubmission_reason
    df_resubmission = df_resubmission.rename(columns={'denial_reason': 'resubmission_reason'})

    # introduce recommended_changes
    df_resubmission["recommended_changes"] = ""

    # only required columns
    df_resubmission = df_resubmission[['claim_id', 'resubmission_reason', 'source_system', 'recommended_changes']]

    # Save the filtered DataFrame as JSON
    df_resubmission.to_json(RESUBMISSION_CANDIDATES_JSON_FILE, orient='records', lines=False)

    METRICS["total_resubmission_eligible"] = len(df_resubmission)

@task(log_prints=True)
def output_failed(df):
    """
    Produce a list of claims that failed:
    """
    df_failed = df.copy()
    df_failed['failed'] = df_failed.apply(check_failed, axis=1)
    
    # Filter out failed
    df_failed = df_failed[df_failed['failed'] == True]

    # only required columns
    df_failed = df_failed[['claim_id', 'denial_reason', 'source_system']]

    # Save the filtered DataFrame as JSON
    df_failed.to_json(FAILED_RECORDS_OUTPUT_FILE, orient='records', lines=False)

    METRICS["total_failed"] = len(df_failed)

@task(log_prints=True)
def aggregate_metrics():
    with open(CLAIM_METRICS_OUTPUT_FILE, 'w') as f:
        json.dump(METRICS, f)
    logging.info(f"Metrics saved to ${CLAIM_METRICS_OUTPUT_FILE}")

@flow(name="claim_resubmission_ingestion")
def claim_resubmission_ingestion():

    # Read both files
    logging.info("Loading json and csv files")
    df_json = read_json()
    df_csv = read_csv()

    # 1. Schema Normalization: transform both frames
    logging.info("Starting Schema Normalization")
    df_transformed_json = transform_data(df=df_json, source="beta")
    METRICS["total_claims_beta_json"] = len(df_transformed_json)
    df_transformed_csv  = transform_data(df=df_csv, source="alpha")
    METRICS["total_claims_alpha_csv"] = len(df_transformed_csv)
    
    # 1.1: combine both dataframes
    logging.info("Combining json and csv dataframes")
    df_combined = pd.concat([df_transformed_json, df_transformed_csv], ignore_index=True)
    METRICS["total_claims"] = len(df_combined)

    # 2. Flag for resubmission
    logging.info("Flagging records for resubmission")
    df_flagged = resubmission_logic(df_combined)

    # 3. Filter and Save output
    logging.info("Saving records due for resubmission")
    automated_resubmission_output(df_flagged)

    # 4. Output_failed
    logging.info("Saving failed records output")
    output_failed(df_flagged)

    # 5. Output_metrcis
    logging.info("Finalizing metrcis aggregation")
    aggregate_metrics()
        

if __name__ == "__main__":
    try:
        logging.info("Pipeline run started")
        claim_resubmission_ingestion()
        logging.info("Completed pipleine run")
    except Exception as e:
        logging.error(f"Something went wrong! {e}")