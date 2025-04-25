# Real-time Climate Data Analytics Platform with ELT

## Overview

This project implements a modern data lake architecture using Apache Iceberg on Google Cloud Platform (GCP). The focus is on Extract-Load-Transform (ELT) rather than traditional ETL, leveraging BigQuery's powerful transformation capabilities while maintaining data lake flexibility with Iceberg.

The platform ingests and processes climate data from NOAA (National Oceanic and Atmospheric Administration) through both batch and streaming pipelines, storing it in a structured Iceberg-formatted data lake on GCS, and making it available for analytics through BigQuery.

## Why This Project is Useful

- **Modern Data Architecture**: Demonstrates ELT pattern with a cloud-native data lake implementation
- **Real-time Analytics**: Combines batch and streaming data for comprehensive climate analysis
- **Open Format**: Uses Apache Iceberg for table format, providing ACID transactions and schema evolution
- **Scalability**: Cloud-based architecture that scales with data volume and processing needs
- **Analytics-Ready**: Transforms data for immediate use in dashboards and data science workflows

## Project Structure

```
.
├── PRD.md                        # Product Requirements Document
├── README.md                     # This file
├── .env.example                  # Example environment variables
├── pyproject.toml                # Project dependencies (for uv)
├── src                           # Source code
│   ├── batch_loader.py           # Extract and load historical climate data
│   ├── stream_processor.py       # Process real-time climate data
│   ├── iceberg_setup.py          # Set up Iceberg catalog and tables
│   ├── main.py                   # Cloud Function entry points
│   └── climate_data_dag.py       # Airflow DAG for orchestration
└── test                          # Test code
    ├── integration_test          # Integration tests
    │   ├── test_batch_loader.py  # Tests for batch loader
    │   └── test_stream_processor.py # Tests for stream processor
    └── unit_test                 # Unit tests
        ├── test_iceberg_setup.py # Tests for Iceberg setup
        └── test_main.py          # Tests for Cloud Functions
```

## Requirements and Dependencies

### GCP Services
- Google Cloud Storage
- Google BigQuery
- Google Cloud Functions
- Google Cloud Pub/Sub
- Google Cloud Composer (Airflow)
- Google Cloud Scheduler

### Python Dependencies
- Python 3.8+
- pyiceberg
- pandas
- pyarrow
- google-cloud-storage
- google-cloud-bigquery
- google-cloud-pubsub
- google-cloud-functions
- requests
- python-dotenv
- apache-airflow

## Local Development Setup

### Setting Up uv for Dependency Management

[uv](https://github.com/astral-sh/uv) is a fast Python package installer and resolver. To install uv:

```bash
# Install uv
pip install uv

# Initialize the project using pyproject.toml
uv pip install -e .
```

### Setting Up Environment Variables

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Edit the `.env` file with your GCP project details and API credentials:
   ```
   GCP_PROJECT_ID=your-project-id
   GCP_REGION=us-central1
   GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-key.json
   NOAA_API_TOKEN=your-api-token
   ```

## Running the Application Locally

### Setting Up Iceberg Catalog and Tables

```bash
# Initialize Iceberg catalog and tables
python -m src.iceberg_setup
```

### Running the Batch Data Loader

```bash
# Load historical climate data
python -m src.batch_loader --year 2022
```

### Testing the Stream Processor

```bash
# Process a test message (simulates Pub/Sub event)
python -m src.stream_processor --test
```

## Testing

### Running Unit Tests

```bash
# Run all unit tests
pytest test/unit_test

# Run specific unit test
pytest test/unit_test/test_iceberg_setup.py
```

### Running Integration Tests

```bash
# Run all integration tests
pytest test/integration_test

# Run specific integration test
pytest test/integration_test/test_batch_loader.py
```

## Deployment to GCP

### 1. Set Up GCP Environment

```bash
# Initialize gcloud and create a new project (if not already created)
gcloud init
gcloud projects create climate-data-lake-iceberg --name="Climate Data Lake Iceberg"
gcloud config set project climate-data-lake-iceberg

# Enable necessary GCP services
gcloud services enable storage-api.googleapis.com \
                   bigquery.googleapis.com \
                   pubsub.googleapis.com \
                   cloudfunctions.googleapis.com \
                   cloudscheduler.googleapis.com \
                   composer.googleapis.com
```

### 2. Create GCS Buckets

```bash
# Create GCS buckets for the data lake zones
gcloud storage buckets create gs://climate-lake-raw-data \
    --location=us-central1 \
    --uniform-bucket-level-access

gcloud storage buckets create gs://climate-lake-iceberg-tables \
    --location=us-central1 \
    --uniform-bucket-level-access

# Create a bucket for the Iceberg catalog
gcloud storage buckets create gs://climate-lake-iceberg-catalog \
    --location=us-central1 \
    --uniform-bucket-level-access
```

### 3. Deploy Cloud Functions

```bash
# Deploy batch loader Cloud Function
gcloud functions deploy climate-data-loader \
  --gen2 \
  --runtime=python310 \
  --region=us-central1 \
  --source=. \
  --entry-point=climate_data_loader \
  --trigger-http \
  --service-account=data-lake-sa@climate-data-lake-iceberg.iam.gserviceaccount.com

# Deploy stream processor Cloud Function
gcloud functions deploy climate-data-stream-processor \
  --gen2 \
  --runtime=python310 \
  --region=us-central1 \
  --source=. \
  --entry-point=process_pubsub_message \
  --trigger-topic=climate-data-stream \
  --service-account=data-lake-sa@climate-data-lake-iceberg.iam.gserviceaccount.com
```

### 4. Set Up Cloud Composer and Deploy DAG

```bash
# Create a Cloud Composer environment
gcloud composer environments create climate-data-orchestrator \
  --location us-central1 \
  --image-version composer-2.6.0-airflow-2.6.3 \
  --python-version 3.8

# Get the DAGs folder for the Cloud Composer environment
DAGS_FOLDER=$(gcloud composer environments describe climate-data-orchestrator \
  --location us-central1 \
  --format="get(config.dagGcsPrefix)")

# Upload the DAG
gcloud storage cp src/climate_data_dag.py ${DAGS_FOLDER}/
```

## Input/Output Formats

### Batch Loader Input
- NOAA GHCN data in text and CSV formats
- Station data in fixed-width format
- Observation data in CSV format

### Streaming Input (Pub/Sub Message)
JSON format with the following schema:
```json
{
  "station_id": "USW00094728",
  "date": "2023-07-01",
  "element": "TMAX",
  "value": 32.5,
  "measurement_flag": "",
  "quality_flag": "",
  "source_flag": "S",
  "observation_time": "15:00"
}
```

### Output Data Structure
- Apache Iceberg tables with Parquet file format
- BigQuery views and materialized tables for analytics
- Transformation results accessible through SQL queries

## License

This project is licensed under the MIT License - see the LICENSE file for details.
