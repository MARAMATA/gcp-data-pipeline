# README: GCP Data Processing and BigQuery Loading Pipeline

## Project Overview
This project implements a data processing and loading pipeline using Google Cloud Platform (GCP). The workflow:
- Validates and cleans data uploaded to a Cloud Storage bucket.
- Loads the cleaned data into a BigQuery table.
- Moves processed files to a designated folder for record-keeping.

The pipeline is built using Python scripts, leveraging the GCP client libraries.

---

## Workflow Overview

### 1. Input
- **Folder:** `input/`
  - Contains raw data files uploaded for processing.
  - Example file: `transactions.csv`

### 2. Processing
- **Script:** `data_processing.py`
  - Validates and cleans the raw data.
  - Uploads the cleaned file to the `clean/` folder in Cloud Storage.
  - Example output: `clean/transactions_cleaned.csv`

### 3. Loading
- **Script:** `data_loader.py`
  - Loads cleaned data from the `clean/` folder into a BigQuery table.
  - Moves successfully processed files to the `done/` folder in Cloud Storage.

### 4. Output
- **Folder:** `done/`
  - Contains files that have been successfully processed and loaded into BigQuery.

---

## Project Structure
```
project-root/
├── input/                        # Folder for raw data files
├── clean/                        # Folder for cleaned files
├── done/                         # Folder for processed files
├── error/                        # Folder for files with errors (if any)
├── data_processing.py            # Script for data validation and cleaning
├── data_loader.py                # Script for loading data into BigQuery
├── main.py                       # Main script to orchestrate the pipeline
├── schema.json                   # BigQuery schema definition
├── requirements.txt              # Python dependencies
└── README.md                     # Documentation (this file)
```

---

## Setup Instructions

### 1. Prerequisites
- **Google Cloud Platform**:
  - A GCP project with BigQuery and Cloud Storage APIs enabled.
  - A service account with necessary permissions.
- **Local environment**:
  - Python 3.8+ installed.
  - Virtual environment (recommended).

### 2. Configuration
1. **Authenticate GCP**:
   - Run the following command to authenticate:
     ```bash
     gcloud auth application-default login
     ```
2. **Set GCP project**:
   - Update the project in `gcloud` configuration:
     ```bash
     gcloud config set project YOUR_PROJECT_ID
     ```
3. **Update script variables**:
   - Edit the following variables in the scripts as needed:
     - `bucket_name`: Your Cloud Storage bucket name.
     - `dataset_id`: Your BigQuery dataset name.
     - `table_id`: Your BigQuery table name.

### 3. Install Dependencies
1. Create and activate a virtual environment:
   ```bash
   python -m venv env
   source env/bin/activate  # On Windows: env\Scripts\activate
   ```
2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

---

## Running the Pipeline
1. Place raw data files into the `input/` folder in the Cloud Storage bucket.

2. Run the main script:
   ```bash
   python main.py
   ```

3. Monitor the logs for the following steps:
   - Validation and cleaning of raw data.
   - Upload of cleaned data to `clean/`.
   - Loading of cleaned data into BigQuery.
   - Movement of processed files to `done/`.

---

## Logs and Monitoring
- **Logs:**
  - All logs are printed to the console for real-time monitoring.
- **Error Handling:**
  - Files with validation errors will be uploaded to the `error/` folder.

---

## Key Notes
1. **Schema Enforcement**:
   - The schema in `schema.json` ensures data consistency.
   - Ensure all input files conform to the schema.

2. **Folder Structure**:
   - The `clean/` folder always contains a placeholder file to ensure the folder remains visible in Cloud Storage.

3. **BigQuery Table**:
   - Data is appended to the `transactions` table without overwriting.
   - Use `WRITE_APPEND` mode for incremental loading.

---

## Troubleshooting
1. **Quota/Permission Errors**:
   - Ensure the authenticated user/service account has required permissions for BigQuery and Cloud Storage.

2. **Schema Mismatch**:
   - Verify the input file structure matches the schema in `schema.json`.

3. **Folder Visibility**:
   - Use a `.placeholder` file to keep folders visible in Cloud Storage when empty.

---

## Future Enhancements
1. Automate error handling and email notifications for failed processes.
2. Implement data validation reports.
3. Add support for other file formats (e.g., JSON, Parquet).

---

## Contributors
- **Maramata Diop**


