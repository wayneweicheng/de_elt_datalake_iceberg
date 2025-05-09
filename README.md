# Real-time Climate Data Analytics Platform with ELT on AWS

## Overview

This project implements a modern data lake architecture using Apache Iceberg on Amazon Web Services (AWS). The focus is on Extract-Load-Transform (ELT) rather than ETL, leveraging Amazon Athena's powerful transformation capabilities while maintaining data lake flexibility with Iceberg.

We'll use public climate data from NOAA to build a comprehensive data platform that showcases AWS services integration with Iceberg table format.

## Getting Started

### Prerequisites

- AWS Account with appropriate permissions
- Python 3.9+ installed
- AWS CLI configured

### Installation

1. Clone the repository:
```bash
git clone https://github.com/wayneweicheng/de_elt_datalake_iceberg.git
cd de_elt_datalake_iceberg
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv

# Activate virtual environment (macOS/Linux)
source .venv/bin/activate
# OR on Windows
# .venv\Scripts\activate

# Initialize the project using pyproject.toml
uv pip install -e .

```

### Setting Up Environment Variables

1. Create a `.env` file in the project root:
   ```bash
   touch .env
   ```

2. Add the following content to the `.env` file:
   ```
   # AWS Configuration
   AWS_REGION=ap-southeast-2
   AWS_PROFILE=default
   
   # AWS Access Credentials (if not using AWS profiles)
   AWS_ACCESS_KEY_ID=your-access-key
   AWS_SECRET_ACCESS_KEY=your-secret-key
   
   # Iceberg Configuration
   ICEBERG_TABLES_BUCKET=climate-lake-iceberg-tables
   CATALOG_NAME=climate_catalog
   NAMESPACE=climate_data
   
   # NOAA API Configuration
   NOAA_API_TOKEN=your-api-token
   
   # Logging
   LOG_LEVEL=INFO
   ```

3. Update the values with your specific configuration.

### Setting Up AWS Environment (Required Before Running Any Commands)

You must set up your AWS environment before running any Python commands:

```bash
# Configure AWS CLI with your credentials
aws configure

# Create an S3 bucket for raw data
aws s3 mb s3://climate-lake-raw-data --region ap-southeast-2

# Create an S3 bucket for Iceberg tables
aws s3 mb s3://climate-lake-iceberg-tables --region ap-southeast-2

# Create an S3 bucket for Iceberg catalog
aws s3 mb s3://climate-lake-iceberg-catalog --region ap-southeast-2

# Create an AWS Glue Database for Iceberg tables
aws glue create-database --database-input '{"Name":"climate_catalog"}' --region ap-southeast-2
```

## Running the Application Locally

### Setting Up Iceberg Catalog and Tables

AWS Glue Data Catalog is used to manage Iceberg table metadata. To set up the Iceberg catalog and tables:

```bash
# Set up Iceberg tables with AWS Glue
python -m src.iceberg_setup
```

This command will create the necessary tables in AWS Glue Data Catalog and configure them for use with Iceberg.

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

### AWS Services
- Amazon S3
- Amazon Athena
- Amazon Glue
- Amazon Cloud Functions
- Amazon Cloud Pub/Sub
- Amazon Cloud Composer (Airflow)
- Amazon Cloud Scheduler

### Python Dependencies
- Python 3.8+
- pyiceberg (<=0.9.1)
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

# Create virtual environment
uv venv

# Activate virtual environment (macOS/Linux)
source .venv/bin/activate
# OR on Windows
# .venv\Scripts\activate

# Initialize the project using pyproject.toml
uv pip install -e .

# Install PyIceberg with S3 and Glue support
uv pip install 'pyiceberg[s3,glue]<=0.9.1'
```

### Setting Up Environment Variables

1. Create a `.env` file in the project root:
   ```bash
   touch .env
   ```

2. Add the following content to the `.env` file:
   ```
   # AWS Configuration
   AWS_REGION=ap-southeast-2
   AWS_PROFILE=default
   
   # AWS Access Credentials (if not using AWS profiles)
   AWS_ACCESS_KEY_ID=your-access-key
   AWS_SECRET_ACCESS_KEY=your-secret-key
   
   # Iceberg Configuration
   ICEBERG_TABLES_BUCKET=climate-lake-iceberg-tables
   CATALOG_NAME=climate_catalog
   NAMESPACE=climate_data
   
   # NOAA API Configuration
   NOAA_API_TOKEN=your-api-token
   
   # Logging
   LOG_LEVEL=INFO
   ```

3. Update the paths and values with your specific configuration.

### Setting Up AWS Environment (Required Before Running Any Commands)

You must set up your AWS environment before running any Python commands:

```bash
# Configure your AWS CLI
aws configure

# Create S3 buckets for the data lake zones
aws s3 mb s3://climate-lake-raw-data --region ap-southeast-2
aws s3 mb s3://climate-lake-iceberg-tables --region ap-southeast-2
aws s3 mb s3://climate-lake-iceberg-catalog --region ap-southeast-2

# Create an AWS Glue Database for Iceberg tables
aws glue create-database --database-input '{"Name":"climate_catalog"}' --region ap-southeast-2
```

## Running the Application Locally

### Setting Up Iceberg Catalog and Tables

AWS Glue Data Catalog is used to manage Iceberg table metadata. To set up the Iceberg catalog and tables:

```bash
# Set up Iceberg tables with AWS Glue
python -m src.iceberg_setup
```

This command will create the necessary tables in AWS Glue Data Catalog and configure them for use with Iceberg.

### Development Roadmap

The Iceberg table setup is being implemented in phases:

1. **Mock Mode**: For early development and testing without creating actual resources
2. **AWS Athena Integration**: Creating proper Iceberg tables using Athena (current implementation)
3. **PyIceberg with Glue**: Future integration using PyIceberg's native AWS Glue catalog support

To run in mock mode (for testing without AWS access):

```bash
# Run with mock mode (no actual tables created)
python -m src.iceberg_setup --mock
```

To run in live mode (create proper Iceberg tables with AWS Athena, requires AWS access):

```bash
python -m src.iceberg_setup
```

### Using AWS Athena with the Iceberg Tables

Once the tables are created, you can query them using Amazon Athena:

```sql
-- Sample query to select from the stations table
SELECT * FROM climate_data.stations LIMIT 10;

-- Sample query to select from the partitioned observations table
SELECT station_id, date, element, value 
FROM climate_data.observations 
WHERE year = 2022 AND month = 1
LIMIT 10;
```

### How the Iceberg Tables are Created

This project creates native Iceberg tables using AWS Athena as the execution engine. The tables are created with:

1. **Proper Iceberg metadata** - Athena properly initializes the Iceberg table metadata
2. **Partitioning** - The observations table is partitioned by year and month
3. **Parquet format** - Data is stored in Parquet format for efficient querying

The script handles:
- Creating the database in AWS Glue if it doesn't exist
- Dropping existing tables if they need to be recreated as proper Iceberg tables
- Creating tables with the appropriate schema and configuration
- Setting up partitioning for the observations table

### Running the Batch Data Loader

The project includes a batch data loader for downloading historical climate data from NOAA's Global Historical Climatology Network (GHCN) and loading it into the Iceberg tables in AWS.

This process:
1. Downloads station metadata and observation data
2. Transforms the data to match the table schema
3. Loads it into AWS S3 
4. Uses Amazon Athena to insert the data into the Iceberg tables

To run the batch loader:

```bash
# Load just the stations data
python -m src.batch_loader --stations-only

# Load data for a specific year
python -m src.batch_loader --year 2022

# Load data for a specific year but filter observations to limit data volume
python -m src.batch_loader --year 2022 --filter-stations
```

Note: By default, the batch loader will use the previous year if no year is specified.

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

## Troubleshooting

### PyIceberg Version Issues

The project currently requires PyIceberg <=0.9.1 (the latest available version). 
This may cause some compatibility issues with the code which was originally written for a newer version.

#### SQLAlchemy Support for In-Memory Catalog

If you see this error:
```
Error setting up Iceberg catalog: SQLAlchemy support not installed: pip install 'pyiceberg[sql-sqlite]'
```

There are several conflicting dependencies. The recommended workaround is to modify `src/iceberg_setup.py` to use a simple file-based approach for local development.

For a quick workaround in local development, you can create a mock catalog that stores data locally:

```python
def create_catalog_config() -> Dict[str, str]:
    """Create simple file-based catalog configuration for local development"""
    try:
        # For local development, we'll use a local path
        warehouse_path = os.path.join(parent_dir, "local_warehouse")
        os.makedirs(warehouse_path, exist_ok=True)
        
        catalog_config = {
            "type": "in-memory",
            "warehouse": warehouse_path,
        }
        return catalog_config
    except Exception as e:
        logger.error(f"Error creating catalog config: {e}")
        raise
```

**Note**: This is only for local development and testing. For production GCP deployment, you would need to configure a proper catalog.

### Missing Hive Support Error

If you see this error:
```
Error setting up Iceberg catalog: Apache Hive support not installed: pip install 'pyiceberg[hive]'
```

Install the Hive extras for PyIceberg:
```bash
source .venv/bin/activate
uv pip install 'pyiceberg[hive]<=0.9.1'
```

### Required Dependencies

For local development, you may need:
```bash
# Install everything needed for local development
source .venv/bin/activate
uv pip install 'pyiceberg[hive,sql-sqlite]<=0.9.1' 'sqlalchemy<2.0.0'
```

### URI Required Error

If you see this error:
```
Error setting up Iceberg catalog: 'uri'
```

There are two options to address this:

1. Use an in-memory catalog for local development:
```python
catalog_config = {
    "type": "in-memory",
    "warehouse": f"gs://{os.getenv('ICEBERG_TABLES_BUCKET', 'climate-lake-iceberg-tables')}",
    "io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
    "credential": "gcp",
    "region": os.getenv("GCP_REGION", "australia-southeast1"),
}
```

2. If you need a persistent Hive catalog, you'll need to set up a Hive metastore service and update the URI:
```python
catalog_config = {
    "type": "hive",
    "uri": "thrift://your-hive-metastore:9083",
    "warehouse": f"gs://{os.getenv('ICEBERG_TABLES_BUCKET', 'climate-lake-iceberg-tables')}",
    "io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
    "credential": "gcp",
    "region": os.getenv("GCP_REGION", "australia-southeast1"),
}
```

### Common Errors

1. **Import errors**: Ensure you're using the correct import paths for your PyIceberg version.

2. **Credential issues**: Make sure your GCP credentials are set correctly:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credentials.json
   ```

3. **Missing buckets**: Ensure you've created all required GCS buckets before running the setup script.
