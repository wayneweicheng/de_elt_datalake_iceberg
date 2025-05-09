#!/usr/bin/env python
"""
stream_processor.py - Process real-time climate data from Pub/Sub

This script processes real-time climate data events received through Google Cloud Pub/Sub
and loads them into Iceberg tables in a Google Cloud Storage data lake.
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
from typing import Dict, Any, Optional, Union

# Add the parent directory to sys.path to allow importing from src
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from pyiceberg.catalog import load_catalog
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


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


def init_gcs_client() -> storage.Client:
    """Initialize Google Cloud Storage client"""
    try:
        storage_client = storage.Client()
        return storage_client
    except Exception as e:
        logger.error(f"Error initializing GCS client: {e}")
        raise


def init_iceberg_catalog() -> Any:
    """Initialize Iceberg catalog"""
    try:
        # Get catalog name and configuration from environment variables
        catalog_name = os.getenv("CATALOG_NAME", "climate_catalog")
        catalog_config = {
            "type": "rest",
            "uri": f"gs://{os.getenv('ICEBERG_CATALOG_BUCKET', 'climate-lake-iceberg-catalog')}",
            "warehouse": f"gs://{os.getenv('ICEBERG_TABLES_BUCKET', 'climate-lake-iceberg-tables')}",
            "credential": "gcp",
            "region": os.getenv("GCP_REGION", "australia-southeast1")
        }
        
        # Load the catalog
        iceberg_catalog = load_catalog(catalog_name, **catalog_config)
        return iceberg_catalog
    except Exception as e:
        logger.error(f"Error initializing Iceberg catalog: {e}")
        raise


def validate_message(message_data: Dict[str, Any]) -> bool:
    """
    Validate the Pub/Sub message data
    
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


def process_message(message_data: Dict[str, Any]) -> bool:
    """
    Process a climate data message and write to Iceberg
    
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
        storage_client = init_gcs_client()
        iceberg_catalog = init_iceberg_catalog()
        
        # Convert message to DataFrame
        df = pd.DataFrame([message_data])
        
        # Convert date string to date object if needed
        if isinstance(df['date'].iloc[0], str):
            df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Get the Iceberg table
        observations_table = iceberg_catalog.load_table(f"{namespace}.observations")
        
        # Write to temporary Parquet file
        with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
            # Convert DataFrame to PyArrow Table and write to Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(table, tmp.name)
            
            # Extract year and month for partitioning
            if isinstance(df['date'].iloc[0], str):
                date_obj = datetime.strptime(df['date'].iloc[0], '%Y-%m-%d')
            else:
                date_obj = df['date'].iloc[0]
                
            year = date_obj.year
            month = date_obj.month
            
            # Create blob name with partitioning
            parquet_blob_name = f"iceberg/observations/data/year={year}/month={month}/stream_{int(time.time())}.parquet"
            
            # Upload to GCS
            bucket = storage_client.bucket(raw_bucket)
            blob = bucket.blob(parquet_blob_name)
            blob.upload_from_filename(tmp.name)
            
            # Register with Iceberg
            observations_table.append(f"gs://{raw_bucket}/{parquet_blob_name}")
            
            logger.info(f"Successfully processed streaming record and wrote to Iceberg table")
            return True
            
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return False


def process_pubsub_message(event: Dict[str, Any]) -> Tuple[str, int]:
    """
    Cloud Function entry point for processing Pub/Sub messages
    
    Args:
        event: The Pub/Sub event
        
    Returns:
        A tuple of (message, status_code)
    """
    try:
        # Configure logging for Cloud Function environment
        logging.getLogger().setLevel(logging.INFO)
        
        # Load environment variables
        load_env_vars()
        
        # Get the Pub/Sub message
        if 'data' not in event['message']:
            return "No data in message", 400
            
        # Decode the message
        pubsub_message = base64.b64decode(event['message']['data']).decode("utf-8")
        logger.info(f"Received message: {pubsub_message}")
        
        # Parse the message as JSON
        try:
            message_data = json.loads(pubsub_message)
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
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return f"Error: {str(e)}", 500


def simulate_pubsub_message(message_data: Dict[str, Any]) -> None:
    """
    Simulate a Pub/Sub message for local testing
    
    Args:
        message_data: The message data to simulate
    """
    # Create a simulated Pub/Sub event
    pubsub_message_bytes = json.dumps(message_data).encode('utf-8')
    b64_message = base64.b64encode(pubsub_message_bytes).decode('utf-8')
    
    event = {
        'message': {
            'data': b64_message
        }
    }
    
    # Process the simulated event
    message, status_code = process_pubsub_message(event)
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
        simulate_pubsub_message(message_data)
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 