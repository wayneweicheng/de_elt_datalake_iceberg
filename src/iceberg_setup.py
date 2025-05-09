#!/usr/bin/env python
"""
iceberg_setup.py - Sets up Apache Iceberg catalog and tables for climate data

This script configures the Iceberg catalog on Amazon S3 and AWS Athena and creates
the necessary tables for storing climate data (stations and observations).
"""

import os
import logging
import argparse
import time
from typing import Dict, Any
import sys
from pathlib import Path

# Add the parent directory to sys.path to allow importing from src
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Try to import boto3 for AWS - will fail gracefully if not available
try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    logger.warning("boto3 is not installed or could not be imported. "
                  "Run 'pip install boto3' to install.")
    HAS_BOTO3 = False

# Try to import PyIceberg - will fail gracefully if not available
try:
    from pyiceberg.catalog import load_catalog
    HAS_PYICEBERG = True
except ImportError:
    logger.warning("PyIceberg is not installed or could not be imported. "
                  "Run 'pip install \"pyiceberg[s3,glue]<=0.9.1\"' to install.")
    HAS_PYICEBERG = False


def load_env_vars() -> None:
    """Load environment variables from .env file"""
    env_path = Path(parent_dir) / '.env'
    if env_path.exists():
        logger.info(f"Loading environment variables from {env_path}")
        load_dotenv(env_path)
    else:
        logger.warning(f"No .env file found at {env_path}, using existing environment variables")
        
    # Set log level from environment variable
    log_level = os.getenv("LOG_LEVEL", "INFO")
    numeric_level = getattr(logging, log_level.upper(), None)
    if isinstance(numeric_level, int):
        logging.getLogger().setLevel(numeric_level)


def create_catalog_config() -> Dict[str, str]:
    """Create Iceberg catalog configuration for AWS Glue or local catalog"""
    try:
        # Default to using AWS Glue catalog
        logger.info("Setting up AWS Glue catalog")
        iceberg_tables_bucket = os.getenv('ICEBERG_TABLES_BUCKET', 'climate-lake-iceberg-tables')
        aws_region = os.getenv("AWS_REGION", "ap-southeast-2")
        
        return {
            "type": "glue",
            "warehouse": f"s3://{iceberg_tables_bucket}",
            "s3.region": aws_region
        }
    except Exception as e:
        logger.error(f"Error creating catalog config: {e}")
        raise


def setup_aws_glue_client():
    """Set up AWS Glue client for database operations"""
    if not HAS_BOTO3:
        logger.error("boto3 is not available, cannot set up AWS Glue client")
        return None
    
    try:
        aws_region = os.getenv("AWS_REGION", "ap-southeast-2")
        glue_client = boto3.client('glue', region_name=aws_region)
        return glue_client
    except Exception as e:
        logger.error(f"Error setting up AWS Glue client: {e}")
        return None


def setup_aws_athena_client():
    """Set up AWS Athena client for SQL operations"""
    if not HAS_BOTO3:
        logger.error("boto3 is not available, cannot set up AWS Athena client")
        return None
    
    try:
        aws_region = os.getenv("AWS_REGION", "ap-southeast-2")
        athena_client = boto3.client('athena', region_name=aws_region)
        return athena_client
    except Exception as e:
        logger.error(f"Error setting up AWS Athena client: {e}")
        return None


def start_query_execution(athena_client, query, database, workgroup="primary"):
    """Execute a query in Athena"""
    try:
        # Get S3 bucket for query results
        results_bucket = os.getenv('ATHENA_RESULTS_BUCKET', 'temp-analysis')
        
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': f's3://{results_bucket}/athena-results/'
            },
            WorkGroup=workgroup
        )
        return response['QueryExecutionId']
    except Exception as e:
        logger.error(f"Error starting Athena query: {e}")
        raise


def wait_for_query_to_complete(athena_client, query_execution_id):
    """Wait for an Athena query to complete"""
    while True:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        state = response['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if state == 'FAILED':
                error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                logger.error(f"Query failed: {error_message}")
                raise Exception(f"Athena query failed: {error_message}")
            return state
        
        logger.info(f"Query is {state}, waiting...")
        time.sleep(5)


def main() -> None:
    """Main function to set up Iceberg catalog and tables with AWS Athena"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Set up Iceberg catalog and tables for climate data")
    parser.add_argument("--catalog", default=None, help="Name of the Iceberg catalog")
    parser.add_argument("--namespace", default=None, help="Namespace for the Iceberg tables")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode without requiring dependencies")
    args = parser.parse_args()
    
    # Load environment variables
    load_env_vars()
    
    # Get catalog and namespace names from arguments or environment variables
    catalog_name = args.catalog or os.getenv("CATALOG_NAME", "climate_catalog")
    namespace = args.namespace or os.getenv("NAMESPACE", "climate_data")
    
    # Check if we're in mock mode
    if args.mock:
        logger.info("Running in mock mode - skipping actual setup")
        logger.info(f"Would create catalog: {catalog_name}")
        logger.info(f"Would create namespace: {namespace}")
        logger.info("Would create tables: stations, observations")
        logger.info("Would use AWS Athena to create Iceberg tables")
        return
    
    try:
        # Set up AWS Glue client
        glue_client = setup_aws_glue_client()
        if glue_client is None:
            logger.error("Failed to set up AWS Glue client. Make sure boto3 is installed and AWS credentials are configured.")
            sys.exit(1)
        logger.info("Successfully connected to AWS Glue Data Catalog")
        
        # Set up AWS Athena client
        athena_client = setup_aws_athena_client()
        if athena_client is None:
            logger.error("Failed to set up AWS Athena client. Make sure boto3 is installed and AWS credentials are configured.")
            sys.exit(1)
        logger.info("Successfully connected to AWS Athena")
        
        # Create or get database in AWS Glue
        try:
            glue_client.get_database(Name=namespace)
            logger.info(f"Database {namespace} already exists in AWS Glue")
        except glue_client.exceptions.EntityNotFoundException:
            logger.info(f"Creating database {namespace} in AWS Glue")
            glue_client.create_database(
                DatabaseInput={
                    'Name': namespace,
                    'Description': 'Climate data database for Iceberg tables',
                }
            )
            logger.info(f"Successfully created database {namespace} in AWS Glue")
        
        # Get the S3 bucket for Iceberg tables
        iceberg_tables_bucket = os.getenv('ICEBERG_TABLES_BUCKET', 'climate-lake-iceberg-tables')
        
        # Check and drop existing tables if they exist (to recreate them properly)
        try:
            logger.info("Checking if stations table exists...")
            glue_client.get_table(DatabaseName=namespace, Name='stations')
            logger.info("Stations table exists. Dropping to recreate as proper Iceberg table.")
            # Drop using Athena
            drop_query = f"DROP TABLE IF EXISTS {namespace}.stations"
            query_id = start_query_execution(athena_client, drop_query, namespace)
            wait_for_query_to_complete(athena_client, query_id)
            logger.info("Successfully dropped stations table")
        except glue_client.exceptions.EntityNotFoundException:
            logger.info("Stations table doesn't exist yet")
        
        try:
            logger.info("Checking if observations table exists...")
            glue_client.get_table(DatabaseName=namespace, Name='observations')
            logger.info("Observations table exists. Dropping to recreate as proper Iceberg table.")
            # Drop using Athena
            drop_query = f"DROP TABLE IF EXISTS {namespace}.observations"
            query_id = start_query_execution(athena_client, drop_query, namespace)
            wait_for_query_to_complete(athena_client, query_id)
            logger.info("Successfully dropped observations table")
        except glue_client.exceptions.EntityNotFoundException:
            logger.info("Observations table doesn't exist yet")
        
        # Create stations table using Athena with Iceberg format
        stations_query = """
        CREATE TABLE IF NOT EXISTS {}.stations (
           station_id STRING,
           name STRING,
           latitude DOUBLE,
           longitude DOUBLE,
           elevation DOUBLE,
           country STRING,
           state STRING,
           first_year INT,
           last_year INT
        )
        LOCATION '{}/stations/'
        TBLPROPERTIES (
           'table_type'='ICEBERG',
           'format'='PARQUET'
        )
        """.format(namespace, f"s3://{iceberg_tables_bucket}/{namespace}")
        
        logger.info("Creating stations table as Iceberg table in Athena")
        stations_query_id = start_query_execution(athena_client, stations_query, namespace)
        wait_for_query_to_complete(athena_client, stations_query_id)
        logger.info("Successfully created stations table as Iceberg table")
        
        # Create observations table using Athena with Iceberg format
        observations_query = """
        CREATE TABLE IF NOT EXISTS {}.observations (
           station_id STRING,
           date DATE,
           element STRING,
           value DOUBLE,
           measurement_flag STRING,
           quality_flag STRING,
           source_flag STRING,
           observation_time TIMESTAMP,
           year INT,
           month INT
        )
        PARTITIONED BY (year, month)
        LOCATION '{}/observations/'
        TBLPROPERTIES (
           'table_type'='ICEBERG',
           'format'='PARQUET'
        )
        """.format(namespace, f"s3://{iceberg_tables_bucket}/{namespace}")
        
        logger.info("Creating observations table as Iceberg table in Athena")
        observations_query_id = start_query_execution(athena_client, observations_query, namespace)
        wait_for_query_to_complete(athena_client, observations_query_id)
        logger.info("Successfully created observations table as Iceberg table")
        
        logger.info("Iceberg tables created successfully using AWS Athena!")
        logger.info("You can now query these tables in Athena using SQL")
        
    except Exception as e:
        logger.error(f"Error setting up Iceberg: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 