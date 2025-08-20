# To Run the application:
Run the following python code, this should ingest your files and produce the required output.
```bash
python main.py
```

The FastApi app to upload files and query uploaded file directories can be started with:
```bash
python api.py
```

# API Spec:
Upload a file:
```bash
POST /upload
```

Get List of files uploaded
```bash
GET /uploads
```

Get List of output files
```bash
GET /outputs
```

# Inputs
As recommended the required input files have been saved as:
1. CSV = emr_alpha.csv
2. JSON = emr_beta.json


# Acceptance Criteria
The required ouputs are as below and are saved in their respective files:
1. Resubmission candidates -> resubmission_candidates.json
2. Failed records -> failed_records.json
3. Claims metrics -> claims_metrics.json



# Checklists
## Final Deliverables
- [x] Working script (or Jupyter notebook or pipeline script) that performs all steps.
- [x] Print or save output to resubmission_candidates.json
- [x] Basic logging or metrics, e.g.: Total claims processed; How many from each source; How many flagged for resubmission; How many excluded (and why)
- [x] Handle malformed or missing data gracefully.

## Bonus Stretch Goals (Optional but impressive):
- [x] Modularize the pipeline using functions or classes.
- [x] Add a FastAPI endpoint to upload new datasets and return resubmission candidates.
- [x] Simulate a Dagster or Prefect pipeline for orchestration.
- [ ] Mock an LLM "classifier" function for ambiguous denial