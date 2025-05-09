#!/usr/bin/env python
"""
test_iceberg_tables.py - Run test queries against Iceberg tables in Athena
"""

import os
import logging
import sys
import time
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

try:
    import boto3
except ImportError:
    logger.error("boto3 is not installed or could not be imported. "
                "Run 'pip install boto3' to install.")
    sys.exit(1)


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


def setup_aws_athena_client():
    """Set up AWS Athena client for SQL operations"""
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
        results_bucket = os.getenv('ATHENA_RESULTS_BUCKET', os.getenv('RAW_BUCKET', 'climate-lake-raw-data'))
        
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


def get_query_results(athena_client, query_execution_id):
    """Get the results of a query from Athena"""
    try:
        response = athena_client.get_query_results(
            QueryExecutionId=query_execution_id
        )
        return response
    except Exception as e:
        logger.error(f"Error getting query results: {e}")
        raise


def print_results(results):
    """Print the results of a query in a readable format"""
    # Get column names
    columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    print("\n" + "-" * 80)
    print("| " + " | ".join(columns) + " |")
    print("-" * 80)
    
    # Print rows
    for row in results['ResultSet']['Rows'][1:]:  # Skip header row
        values = [data.get('VarCharValue', '') for data in row['Data']]
        print("| " + " | ".join(values) + " |")
    print("-" * 80)


def main():
    """Main function to run test queries against Iceberg tables"""
    print("Starting test of Iceberg tables...")
    load_env_vars()
    
    namespace = os.getenv("NAMESPACE", "climate_data")
    print(f"Using namespace: {namespace}")
    
    # Set up AWS Athena client
    athena_client = setup_aws_athena_client()
    if athena_client is None:
        logger.error("Failed to set up AWS Athena client. Make sure boto3 is installed and AWS credentials are configured.")
        sys.exit(1)
    print("Successfully connected to AWS Athena")
    
    # Test query for stations table
    print("Running test query against stations table...")
    stations_query = f"SELECT * FROM {namespace}.stations LIMIT 10"
    print(f"Query: {stations_query}")
    
    try:
        # Execute the query
        query_id = start_query_execution(athena_client, stations_query, namespace)
        print(f"Query ID: {query_id}")
        state = wait_for_query_to_complete(athena_client, query_id)
        print(f"Query state: {state}")
        
        if state == 'SUCCEEDED':
            print(f"Test query on stations table completed successfully!")
            
            # Get and print results
            results = get_query_results(athena_client, query_id)
            
            # Check if there are any rows
            if len(results['ResultSet']['Rows']) <= 1:  # Only header row
                print("No data in stations table yet (this is expected for new tables)")
            else:
                print("Data from stations table:")
                print_results(results)
        
    except Exception as e:
        print(f"Error running stations query: {e}")
    
    # Test query for observations table
    print("\nRunning test query against observations table...")
    observations_query = f"SELECT * FROM {namespace}.observations LIMIT 10"
    print(f"Query: {observations_query}")
    
    try:
        # Execute the query
        query_id = start_query_execution(athena_client, observations_query, namespace)
        print(f"Query ID: {query_id}")
        state = wait_for_query_to_complete(athena_client, query_id)
        print(f"Query state: {state}")
        
        if state == 'SUCCEEDED':
            print(f"Test query on observations table completed successfully!")
            
            # Get and print results
            results = get_query_results(athena_client, query_id)
            
            # Check if there are any rows
            if len(results['ResultSet']['Rows']) <= 1:  # Only header row
                print("No data in observations table yet (this is expected for new tables)")
            else:
                print("Data from observations table:")
                print_results(results)
        
    except Exception as e:
        print(f"Error running observations query: {e}")
    
    print("\nIceberg tables test complete!")


if __name__ == "__main__":
    main() 