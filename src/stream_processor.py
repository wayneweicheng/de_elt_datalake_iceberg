#!/usr/bin/env python
"""
stream_processor.py - Process real-time climate data from AWS SQS

This script processes real-time climate data events received through AWS SQS messages
and loads them into Iceberg tables in Amazon S3 data lake using AWS Glue/Athena.
"""

import os
import sys
import json
import base64
import argparse
import logging
import time
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union, Tuple

# Add the parent directory to sys.path to allow importing from src
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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


def init_s3_client():
    """Initialize Amazon S3 client"""
    if not HAS_BOTO3:
        logger.error("boto3 is not available, cannot initialize S3 client")
        return None
    
    try:
        aws_region = os.getenv("AWS_REGION", "ap-southeast-2")
        s3_client = boto3.client('s3', region_name=aws_region)
        return s3_client
    except Exception as e:
        logger.error(f"Error initializing S3 client: {e}")
        raise


def init_athena_client():
    """Initialize AWS Athena client"""
    if not HAS_BOTO3:
        logger.error("boto3 is not available, cannot initialize Athena client")
        return None
    
    try:
        aws_region = os.getenv("AWS_REGION", "ap-southeast-2")
        athena_client = boto3.client('athena', region_name=aws_region)
        return athena_client
    except Exception as e:
        logger.error(f"Error initializing Athena client: {e}")
        raise


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
    """
    Wait for an Athena query to complete execution.
    
    Args:
        athena_client: Boto3 Athena client
        query_execution_id: ID of the query execution to wait for
        
    Returns:
        Query execution state
    """
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            logger.info(f"Query {query_execution_id} finished with state {state}")
            return state
        
        logger.info(f"Query is {state}, waiting...")
        time.sleep(5)  # Wait for 5 seconds before checking again


def execute_athena_query(athena_client, query):
    """
    Execute an Athena query and wait for it to complete.
    
    Args:
        athena_client: Boto3 Athena client
        query: SQL query to execute
        
    Returns:
        Query execution ID if successful
    """
    # Get database from environment
    namespace = os.getenv("NAMESPACE", "climate_data")
    
    # Start the query execution
    query_execution_id = start_query_execution(athena_client, query, namespace)
    
    # Wait for query to complete
    state = wait_for_query_to_complete(athena_client, query_execution_id)
    
    if state == 'SUCCEEDED':
        return query_execution_id
    else:
        exception = Exception(f"Query failed with state {state}")
        # Store the query execution ID in the exception for better error handling
        exception.query_execution_id = query_execution_id
        raise exception


def validate_message(message_data: Dict[str, Any]) -> bool:
    """
    Validate the SQS message data
    
    Args:
        message_data: The message data to validate
        
    Returns:
        True if valid, False otherwise
    """
    # Check required fields
    required_fields = ['station_id', 'date', 'element', 'value']
    for field in required_fields:
        if field not in message_data:
            logger.error(f"Required field '{field}' missing from message")
            return False
    
    # Validate data types
    try:
        # Validate station_id is string
        if not isinstance(message_data['station_id'], str):
            logger.error("station_id must be a string")
            return False
        
        # Validate element is string
        if not isinstance(message_data['element'], str):
            logger.error("element must be a string")
            return False
        
        # Validate value is numeric
        value = float(message_data['value'])
        
        # Validate date format
        if isinstance(message_data['date'], str):
            datetime.strptime(message_data['date'], '%Y-%m-%d')
        
        return True
    except ValueError as e:
        logger.error(f"Data validation error: {e}")
        return False


def check_table_exists(athena_client, database, table_name):
    """
    Check if a table exists in the database
    
    Args:
        athena_client: Boto3 Athena client
        database: Database name
        table_name: Table name to check
        
    Returns:
        True if table exists, False otherwise
    """
    try:
        query = f"SHOW TABLES IN {database} LIKE '{table_name}'"
        query_execution_id = start_query_execution(athena_client, query, database)
        state = wait_for_query_to_complete(athena_client, query_execution_id)
        
        if state == 'SUCCEEDED':
            # Get query results to see if there are any rows
            results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            # The table exists if the result has more than just the header row
            # return len(results['ResultSet']['Rows']) > 1
            return results['ResultSet']['Rows'][0]['Data'][0]['VarCharValue'] == table_name
        return False
    except Exception as e:
        logger.warning(f"Error checking if table exists: {e}")
        return False


def process_message(message_data: Dict[str, Any]) -> bool:
    """
    Process a climate data message and write to Iceberg using Athena
    
    Args:
        message_data: The message data to process
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Processing message: {message_data}")
        
        # Get environment variables
        raw_bucket = os.getenv("RAW_BUCKET", "climate-lake-raw-data")
        namespace = os.getenv("NAMESPACE", "climate_data")
        
        # Initialize clients
        s3_client = init_s3_client()
        athena_client = init_athena_client()
        
        if s3_client is None or athena_client is None:
            logger.error("Failed to initialize required AWS clients")
            return False
        
        # Check if the observations table exists
        if not check_table_exists(athena_client, namespace, "observations"):
            logger.error(f"The table {namespace}.observations does not exist. Please run iceberg_setup.py first.")
            return False
        
        # Convert message to DataFrame
        df = pd.DataFrame([message_data])
        
        # Convert date string to date object if needed
        if isinstance(df['date'].iloc[0], str):
            df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Extract year and month for partitioning
        if isinstance(df['date'].iloc[0], str):
            date_obj = datetime.strptime(df['date'].iloc[0], '%Y-%m-%d')
        else:
            date_obj = df['date'].iloc[0]
            
        year = date_obj.year
        month = date_obj.month
        
        # Add partition columns
        df['year'] = year
        df['month'] = month
        
        # Write to temporary Parquet file
        with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
            # Convert DataFrame to PyArrow Table and write to Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(table, tmp.name)
            
            # Create S3 key with partitioning
            s3_key = f"streaming/observations/year={year}/month={month}/stream_{int(time.time())}.parquet"
            
            # Upload to S3
            s3_client.upload_file(tmp.name, raw_bucket, s3_key)
            logger.info(f"Uploaded streaming data to s3://{raw_bucket}/{s3_key}")
            
            # Create a temporary table in Athena
            temp_table_name = f"{namespace}.temp_stream_{int(time.time())}"
            s3_location = f"s3://{raw_bucket}/streaming/observations/year={year}/month={month}/"
            
            # First, drop the temp table if it exists
            drop_table_query = f"DROP TABLE IF EXISTS {temp_table_name};"
            try:
                execute_athena_query(athena_client, drop_table_query)
            except Exception as e:
                logger.warning(f"Error dropping temp table: {e}. Will proceed with table creation.")
            
            create_table_query = f"""
            CREATE EXTERNAL TABLE {temp_table_name} (
                station_id STRING,
                date DATE,
                element STRING,
                value DOUBLE,
                measurement_flag STRING,
                quality_flag STRING,
                source_flag STRING,
                observation_time STRING,
                year INT,
                month INT
            )
            STORED AS PARQUET
            LOCATION '{s3_location}';
            """
            
            # Execute the create table query
            execute_athena_query(athena_client, create_table_query)
            logger.info(f"Created temporary table {temp_table_name}")
            
            # Insert data into Iceberg table
            insert_query = f"""
            INSERT INTO {namespace}.observations
            SELECT
                station_id,
                date,
                element,
                value,
                measurement_flag,
                quality_flag,
                source_flag,
                -- If observation_time is a time format (HH:MM), combine with date to form a full timestamp
                CASE
                    WHEN observation_time = '' THEN NULL
                    WHEN observation_time IS NULL THEN NULL 
                    ELSE CAST(CONCAT(CAST(date AS VARCHAR), ' ', observation_time) AS TIMESTAMP)
                END as observation_time,
                year,
                month
            FROM {temp_table_name};
            """
            
            # Execute the insert query
            try:
                execute_athena_query(athena_client, insert_query)
                logger.info(f"Inserted streaming record into Iceberg table")
            except Exception as e:
                # Get detailed error information from Athena
                try:
                    if hasattr(e, 'query_execution_id'):
                        query_id = e.query_execution_id
                        response = athena_client.get_query_execution(QueryExecutionId=query_id)
                        if 'QueryExecution' in response and 'Status' in response['QueryExecution']:
                            reason = response['QueryExecution']['Status'].get('StateChangeReason', 'No detailed reason available')
                            logger.error(f"Athena query failed: {reason}")
                    raise e
                except Exception as inner_error:
                    logger.error(f"Additional error while retrieving query details: {inner_error}")
                    raise e
            
            # Drop the temporary table
            drop_table_query = f"DROP TABLE {temp_table_name};"
            execute_athena_query(athena_client, drop_table_query)
            logger.info(f"Dropped temporary table {temp_table_name}")
            
            # Delete the parquet file from S3 after successful insertion
            try:
                logger.info(f"Deleting temporary parquet file: s3://{raw_bucket}/{s3_key}")
                s3_client.delete_object(Bucket=raw_bucket, Key=s3_key)
                logger.info(f"Successfully deleted temporary parquet file")
            except Exception as e:
                logger.warning(f"Error deleting temporary parquet file: {e}")
            
            return True
            
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return False


def process_sqs_message(event: Dict[str, Any]) -> Tuple[str, int]:
    """
    Lambda Function entry point for processing SQS messages
    
    Args:
        event: The SQS event
        
    Returns:
        A tuple of (message, status_code)
    """
    try:
        # Configure logging for Lambda Function environment
        logging.getLogger().setLevel(logging.INFO)
        
        # Load environment variables
        load_env_vars()
        
        # Process each record in the event
        processed = 0
        failed = 0
        
        # Check if we're processing a direct SQS event from Lambda
        if 'Records' in event:
            for record in event['Records']:
                try:
                    # Parse the SQS message body
                    message_body = record['body']
                    logger.info(f"Received SQS message: {message_body}")
                    
                    # Parse the message as JSON
                    try:
                        message_data = json.loads(message_body)
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON: {e}")
                        failed += 1
                        continue
                    
                    # Validate the message
                    if not validate_message(message_data):
                        logger.error("Invalid message format or data")
                        failed += 1
                        continue
                    
                    # Process the message
                    success = process_message(message_data)
                    if success:
                        processed += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"Error processing SQS record: {e}")
                    failed += 1
                
            # Return summary
            if failed > 0:
                return f"Processed {processed} messages, failed {failed}", 500 if processed == 0 else 200
            else:
                return f"Successfully processed {processed} messages", 200
        
        # For local testing or direct invocation, assume a simulated event structure
        elif 'message' in event and 'data' in event['message']:
            # Decode the message if base64 encoded
            try:
                message_str = base64.b64decode(event['message']['data']).decode("utf-8")
            except:
                # If decoding fails, assume it's already a string
                message_str = event['message']['data']
            
            logger.info(f"Received direct message: {message_str}")
            
            # Parse the message as JSON
            try:
                message_data = json.loads(message_str)
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")
                return f"Error: Invalid JSON in message", 400
            
            # Validate the message
            if not validate_message(message_data):
                return "Error: Invalid message format or data", 400
                
            # Process the message
            success = process_message(message_data)
            if success:
                return "Success", 200
            else:
                return "Error processing message", 500
                
        else:
            return "Invalid event format", 400
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return f"Error: {str(e)}", 500


def simulate_sqs_message(message_data: Dict[str, Any]) -> None:
    """
    Simulate an SQS message for local testing
    
    Args:
        message_data: The message data to simulate
    """
    # Create a simulated SQS event
    message_str = json.dumps(message_data)
    
    # For local testing, we'll use a simpler format
    event = {
        'message': {
            'data': message_str
        }
    }
    
    # Process the simulated event
    message, status_code = process_sqs_message(event)
    logger.info(f"Response: {message}, Status: {status_code}")


def create_test_message() -> Dict[str, Any]:
    """
    Create a test message for local testing
    
    Returns:
        A test message
    """
    return {
        "station_id": "USW00094728",
        "date": datetime.now().strftime('%Y-%m-%d'),
        "element": "TMAX",
        "value": 32.5,
        "measurement_flag": "",
        "quality_flag": "",
        "source_flag": "S",
        "observation_time": datetime.now().strftime('%H:%M')
    }


def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Process real-time climate data")
    parser.add_argument("--test", action="store_true", help="Process a test message")
    parser.add_argument("--input-file", type=str, help="JSON file containing a message to process")
    return parser.parse_args()


def main() -> None:
    """Main function for local execution"""
    try:
        # Load environment variables
        load_env_vars()
        
        # Parse command line arguments
        args = parse_args()
        
        if args.input_file:
            # Load message from file
            with open(args.input_file, 'r') as f:
                message_data = json.load(f)
                logger.info(f"Loaded message from {args.input_file}")
        elif args.test:
            # Create a test message
            message_data = create_test_message()
            logger.info(f"Created test message: {message_data}")
        else:
            logger.error("No input specified. Use --test or --input-file")
            sys.exit(1)
            
        # Validate the message
        if not validate_message(message_data):
            logger.error("Invalid message format or data")
            sys.exit(1)
            
        # Process the message
        simulate_sqs_message(message_data)
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 