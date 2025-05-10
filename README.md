# Real-time Climate Data Analytics Platform with ELT on AWS

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

# Create an S3 bucket for Athena query results
aws s3 mb s3://climate-lake-athena-results --region ap-southeast-2

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
│   ├── stream_processor.py       # Process real-time climate data from SQS
│   ├── iceberg_setup.py          # Set up Iceberg catalog and tables
│   ├── test_iceberg_tables.py    # Tests for Iceberg tables functionality
│   └── data/                     # Sample test data for local development
├── nessie_warehouse/             # Configuration for Project Nessie (empty)
├── local_warehouse/              # Local warehouse configuration (empty)
└── test                          # Test code
    ├── integration_test          # Integration tests
    │   ├── test_batch_loader.py  # Tests for batch loader
    │   └── test_stream_processor.py # Tests for stream processor
    └── unit_test                 # Unit tests
        ├── test_iceberg_setup.py # Tests for Iceberg setup
        └── test_main.py          # Tests for Lambda Functions
```

## Requirements and Dependencies

### AWS Services
- Amazon S3
- Amazon Athena
- Amazon Glue
- AWS Lambda
- Amazon SQS
- Amazon EventBridge
- Amazon Managed Workflows for Apache Airflow (MWAA)

### Python Dependencies
- Python 3.8+
- boto3
- pandas
- pyarrow
- requests
- python-dotenv
- apache-airflow
- pyiceberg[s3,glue]

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

# Install boto3 for AWS access
uv pip install boto3
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
aws s3 mb s3://climate-lake-athena-results --region ap-southeast-2

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

### Accessing Iceberg Tables From Other Query Engines

One of the key benefits of Apache Iceberg is its ability to work with multiple query engines beyond just AWS Athena. Here are some options:

#### Apache Spark

```python
# Configure Spark to access Iceberg tables in AWS Glue
spark = (SparkSession.builder
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://climate-lake-iceberg-tables")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate())

# Read from Iceberg table
df = spark.read.format("iceberg").load("glue_catalog.climate_data.stations")
df.show()

# Write to Iceberg table
df.write.format("iceberg").save("glue_catalog.climate_data.new_table")
```

#### Trino (formerly PrestoSQL)

```properties
# Add to your Trino catalog properties
connector.name=iceberg
iceberg.catalog.type=glue
hive.metastore.glue.region=ap-southeast-2
```

Then query with SQL:
```sql
SELECT * FROM glue_catalog.climate_data.stations LIMIT 10;
```

#### Flink

```java
// Configure Flink catalog
Map<String, String> properties = new HashMap<>();
properties.put("warehouse", "s3://climate-lake-iceberg-tables");
properties.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
tEnv.executeSql("CREATE CATALOG glue_catalog WITH (" +
    "'type'='iceberg', " +
    "'warehouse'='s3://climate-lake-iceberg-tables', " +
    "'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog', " +
    "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO'" +
    ")");

// Query Iceberg tables
tEnv.executeSql("SELECT * FROM glue_catalog.climate_data.stations LIMIT 10").print();
```

#### Project Nessie Integration

[Project Nessie](https://projectnessie.org/) adds Git-like version control to Iceberg tables:

```bash
# Set up Nessie server (locally using Docker)
docker run -p 19120:19120 projectnessie/nessie

# Configure Spark with Nessie
spark = (SparkSession.builder
    .config("spark.sql.catalog.nessie_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie_catalog.warehouse", "s3://climate-lake-iceberg-tables")
    .config("spark.sql.catalog.nessie_catalog.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie_catalog.uri", "http://localhost:19120/api/v1")
    .getOrCreate())

# Use Nessie branches for version control
spark.sql("CREATE TABLE nessie_catalog.climate_data.test (id INT)")
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

The stream processor is designed to handle real-time climate data events received through AWS SQS. For local testing:

```bash
# Process a test message (simulates SQS event)
python -m src.stream_processor --test

# Process a message from a sample JSON file
# Several example files are provided in the src/data directory:
python -m src.stream_processor --input-file src/data/temperature_reading.json
python -m src.stream_processor --input-file src/data/precipitation_reading.json
python -m src.stream_processor --input-file src/data/snow_reading.json
python -m src.stream_processor --input-file src/data/wind_reading.json
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

### Streaming Input (SQS Message)
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
- Apache Iceberg tables with Parquet file format in S3
- Data accessible through AWS Athena for SQL queries
- Partitioned by year and month for efficient querying

## License

This project is licensed under the MIT License - see the LICENSE file for details.

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

3. **Verify database and table names**:
   Ensure that your `.env` file has the correct `NAMESPACE` and `CATALOG_NAME` values.

### SQS Stream Processing Issues

If the stream processor is not working:

1. **Make sure Iceberg tables are set up first**:
   ```bash
   # Set up Iceberg tables before running the stream processor
   python -m src.iceberg_setup
   ```
   The stream processor requires the `observations` Iceberg table to exist.

2. **Verify boto3 installation**:
   ```bash
   pip install boto3
   ```

3. **Check SQS message format**:
   Ensure your messages match the expected JSON format.

4. **Run with increased logging**:
   ```bash
   LOG_LEVEL=DEBUG python -m src.stream_processor --test
   ```

5. **Check Athena query logs**:
   If your stream processing fails at the Athena query stage, check the query logs in the AWS console to see the specific error message.

### Batch Loader Issues

If the batch loader fails:

1. **Check network connectivity**:
   Ensure you can reach the NOAA data sources.

2. **Verify S3 buckets exist**:
   Confirm all required S3 buckets are created.

3. **Examine temporary directories**:
   Check if temporary files are being created and processed correctly.

### Iceberg Integration with Other Query Engines

If you have trouble connecting other query engines to your Iceberg tables:

1. **Check catalog configuration**:
   Ensure you're using the right catalog implementation (`org.apache.iceberg.aws.glue.GlueCatalog` for AWS Glue).

2. **Verify AWS region**:
   Make sure you're using the correct AWS region in your configuration.

3. **Check S3 access**:
   Ensure your query engine has proper access to the S3 bucket containing the Iceberg data.

4. **Review table name format**:
   Different engines may require different formats for referencing tables (catalog.database.table).
