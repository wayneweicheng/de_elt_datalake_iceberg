# Real-time Climate Data Analytics Platform with ELT and Apache Iceberg on AWS

## Overview

This project implements a modern data lake architecture using Apache Iceberg on Amazon Web Services (AWS). The focus is on Extract-Load-Transform (ELT) rather than ETL, leveraging Amazon Athena's powerful transformation capabilities while maintaining data lake flexibility with Iceberg.

We use public climate data from NOAA to build a comprehensive data platform that showcases AWS services integration with Iceberg table format.

## Getting Started

### Prerequisites

- AWS Account with appropriate permissions
- Python 3.9+ installed
- AWS CLI configured

### Installation

1. Clone the repository:
```bash
git clone https://github.com/username/de_elt_datalake_iceberg.git
cd de_elt_datalake_iceberg
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv

# Activate virtual environment (macOS/Linux)
source .venv/bin/activate
# OR on Windows
# .venv\Scripts\activate
```

3. Install dependencies:
```bash
# Using pip
pip install -r requirements.txt

# Or using uv
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

### Setting Up AWS Environment

You must set up your AWS environment before running any Python commands:

```bash
# Configure AWS CLI with your credentials
aws configure

# Create an S3 bucket for raw data
aws s3 mb s3://climate-lake-raw-data --region ap-southeast-2

# Create an S3 bucket for Iceberg tables
aws s3 mb s3://climate-lake-iceberg-tables --region ap-southeast-2

# Create an S3 bucket for Athena query results
aws s3 mb s3://climate-lake-athena-results --region ap-southeast-2

# Create an AWS Glue Database for Iceberg tables
aws glue create-database --database-input '{"Name":"climate_catalog"}' --region ap-southeast-2
```

## Project Structure

```
.
├── README.md                     # Main documentation file
├── docs/                         # Documentation files
│   ├── PRD.md                    # Product Requirements Document
│   ├── future_enhancements.md    # Future development ideas
│   └── time_travel.md            # Time travel usage guide
├── pyproject.toml                # Project dependencies
├── requirements.txt              # Dependencies for pip
├── src                           # Source code
│   ├── batch_loader.py           # Extract and load historical climate data
│   ├── stream_processor.py       # Process real-time climate data
│   ├── iceberg_setup.py          # Set up Iceberg catalog and tables
│   ├── test_iceberg_tables.py    # Tests for Iceberg tables functionality
│   ├── data/                     # Sample test data for local development
│   ├── query_engine/             # Query engine implementations
│   │   ├── athena/              # Athena query tools
│   │   │   └── time_travel_example.py # Athena time travel
│   │   └── duckdb/              # DuckDB query tools
│   │       ├── time_travel_example.py # DuckDB time travel
│   │       └── query_with_duckdb.py  # DuckDB query utilities
│   └── utils/                    # Utility functions
└── test                          # Test code
    ├── integration_test/         # Integration tests
    └── unit_test/                # Unit tests
```

## Running the Application

### Setting Up Iceberg Catalog and Tables

AWS Glue Data Catalog is used to manage Iceberg table metadata. To set up the Iceberg catalog and tables:

```bash
# Set up Iceberg tables with AWS Glue
python -m src.iceberg_setup
```

This command will create the necessary tables in AWS Glue Data Catalog and configure them for use with Iceberg.

To run in mock mode (for testing without AWS access):

```bash
# Run with mock mode (no actual tables created)
python -m src.iceberg_setup --mock
```

### Running the Batch Data Loader

The batch loader downloads historical climate data from NOAA's Global Historical Climatology Network (GHCN) and loads it into the Iceberg tables:

```bash
# Load just the stations data
python -m src.batch_loader --stations-only

# Load data for a specific year
python -m src.batch_loader --year 2022

# Load data for a specific year but filter observations to limit data volume
python -m src.batch_loader --year 2022 --filter-stations
```

### Running the Stream Processor

The stream processor handles real-time climate data events. For local testing:

```bash
# Process a test message (simulates SQS event)
python -m src.stream_processor --test

# Process a message from a sample JSON file
python -m src.stream_processor --input-file src/data/temperature_reading.json
```

## Using Time Travel with Iceberg Tables

Apache Iceberg's time travel capabilities allow you to query tables as they existed at previous points in time.

### Time Travel with AWS Athena

```bash
# List snapshots for a table
python -m src.query_engine.athena.time_travel_example --table observations --list-snapshots

# Query a table at a specific snapshot ID
python -m src.query_engine.athena.time_travel_example --table observations --snapshot-id 3821550127947880959

# Query a table as it existed at a specific timestamp
python -m src.query_engine.athena.time_travel_example --table observations --timestamp "2023-07-01 12:00:00"
```

### Time Travel with DuckDB

```bash
# List snapshots for a table
python -m src.query_engine.duckdb.time_travel_example --table observations --list-snapshots

# Query a table as it existed at a specific timestamp
python -m src.query_engine.duckdb.time_travel_example --table observations --timestamp "2023-07-01 12:00:00"
```

For detailed information about time travel capabilities, see the [time travel documentation](docs/time_travel.md).

## Testing

```bash
# Run all unit tests
pytest test/unit_test

# Run all integration tests
pytest test/integration_test
```

## Input/Output Formats

### Streaming Input (JSON Format)
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
- Apache Iceberg tables with Parquet file format in S3
- Data accessible through AWS Athena and DuckDB
- Partitioned by year and month for efficient querying

## Troubleshooting

### AWS Credentials Issues

If you encounter errors related to AWS credentials:

1. **Ensure your AWS CLI is properly configured**:
   ```bash
   aws configure
   # Enter your access key, secret key, and preferred region
   ```

2. **Check environment variables**:
   Make sure your `.env` file contains the correct AWS configuration.

3. **IAM permissions**:
   Ensure your IAM user/role has the necessary permissions for S3, Athena, and Glue.

### Athena Query Errors

If Athena queries fail:

1. **Check your Athena query results bucket**:
   Verify that the `climate-lake-athena-results` bucket exists and that you have write permissions.

2. **Examine query logs**:
   Check the Athena query execution details in the AWS console for specific error messages.

### Iceberg Setup Issues

If the Iceberg setup fails:

1. **Check if the database exists**:
   Verify that the AWS Glue database specified in your environment variables exists.

2. **Verify S3 bucket existence**:
   Ensure that the S3 bucket for Iceberg tables exists and you have the appropriate permissions.

## Future Enhancements

For a list of potential future enhancements to this project, see [future_enhancements.md](docs/future_enhancements.md).

