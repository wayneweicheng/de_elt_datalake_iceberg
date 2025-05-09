#!/usr/bin/env python
"""
batch_loader.py - Batch extraction and loading of historical climate data

This script downloads and processes historical climate data from NOAA sources,
and loads it into Iceberg tables in Amazon S3 with AWS Glue/Athena.
"""

import os
import sys
import argparse
import logging
import time
import tempfile
import gzip
import io
import csv
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

# Add the parent directory to sys.path to allow importing from src
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import requests
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


def download_and_upload_to_s3(url: str, bucket_name: str, key_name: str, 
                             s3_client) -> bytes:
    """
    Download data from URL and upload to S3 bucket
    
    Args:
        url: URL to download data from
        bucket_name: S3 bucket name
        key_name: S3 key name
        s3_client: Initialized S3 client
        
    Returns:
        Content of the downloaded file as bytes
    """
    try:
        logger.info(f"Downloading {url}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        logger.info(f"Upload to s3://{bucket_name}/{key_name}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key_name,
            Body=response.content
        )
        logger.info(f"Uploaded to s3://{bucket_name}/{key_name}")
        
        return response.content
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading from {url}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        raise


def insert_into_stations_table(stations_df: pd.DataFrame, namespace: str, athena_client) -> None:
    """
    Insert data into stations Iceberg table using Athena
    
    Args:
        stations_df: DataFrame containing stations data
        namespace: Database namespace
        athena_client: AWS Athena client
    """
    try:
        # Create a temporary directory for processing
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write to temporary Parquet file
            parquet_file = os.path.join(tmpdir, "stations.parquet")
            table = pa.Table.from_pandas(stations_df)
            pq.write_table(table, parquet_file)
            
            # Upload to S3
            raw_bucket = os.getenv("RAW_BUCKET", "climate-lake-raw-data")
            s3_client = init_s3_client()
            s3_key = f"raw/stations/stations_{int(time.time())}.parquet"
            
            with open(parquet_file, 'rb') as f:
                s3_client.put_object(
                    Bucket=raw_bucket,
                    Key=s3_key,
                    Body=f
                )
            
            # Create a temporary table to hold the data
            temp_table_name = f"temp_stations_{int(time.time())}"
            create_temp_table_query = f"""
            CREATE EXTERNAL TABLE {namespace}.{temp_table_name} (
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
            ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            LOCATION 's3://{raw_bucket}/raw/stations/'
            """
            
            query_id = start_query_execution(athena_client, create_temp_table_query, namespace)
            wait_for_query_to_complete(athena_client, query_id)
            
            # Insert the data into the Iceberg table
            insert_query = f"""
            INSERT INTO {namespace}.stations
            SELECT * FROM {namespace}.{temp_table_name}
            """
            
            query_id = start_query_execution(athena_client, insert_query, namespace)
            wait_for_query_to_complete(athena_client, query_id)
            
            # Drop the temporary table
            drop_query = f"DROP TABLE IF EXISTS {namespace}.{temp_table_name}"
            query_id = start_query_execution(athena_client, drop_query, namespace)
            wait_for_query_to_complete(athena_client, query_id)
            
            logger.info(f"Inserted {len(stations_df)} stations into Iceberg table")
            
    except Exception as e:
        logger.error(f"Error inserting into stations table: {e}")
        raise


def process_stations_data():
    """
    Process GHCN stations data and load to Iceberg
    
    Returns:
        DataFrame containing stations data
    """
    try:
        logging.info("Processing stations data...")
        
        # Get environment variables
        raw_bucket = os.getenv("RAW_BUCKET", "climate-lake-raw-data")
        ghcn_stations_url = os.getenv("GHCN_STATIONS_URL",
                                     "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt")
        
        # Initialize clients
        s3_client = init_s3_client()
        athena_client = init_athena_client()
        
        if s3_client is None or athena_client is None:
            logging.error("Failed to initialize required AWS clients")
            sys.exit(1)
        
        # Download stations data
        content = download_and_upload_to_s3(
            ghcn_stations_url, 
            raw_bucket, 
            "ghcn/stations/ghcnd-stations.txt",
            s3_client
        )
        
        # Parse fixed-width format
        # Format spec: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
        stations_data = []
        for line in content.decode('utf-8').splitlines():
            if len(line) < 85:  # Skip malformed lines
                continue
            
            try:
                # Extract fields with proper type handling
                station_id = line[0:11].strip()
                # Ensure name is processed as a string
                name = line[41:71].strip()
                
                # These are numeric fields
                try:
                    latitude = float(line[12:20].strip() or 0)
                    longitude = float(line[21:30].strip() or 0)
                    elevation = float(line[31:37].strip() or 0)
                except ValueError:
                    logging.warning(f"Skipping line with invalid numeric data: {line.strip()}")
                    continue
                
                # Extract country and state fields
                country = line[38:40].strip()
                
                # For US stations, get the state code from positions 71-73
                if country == 'US':
                    state = line[71:73].strip()
                else:
                    state = ""  # Empty string for non-US stations
                
                # Parse the first and last years if present
                try:
                    first_year = int(line[74:78].strip() or "0")
                    last_year = int(line[79:83].strip() or "0")
                except ValueError as e:
                    # Log the error and skip this record
                    logging.warning(f"Error parsing line: {line.strip()}. Error: {e}")
                    continue
                
                stations_data.append({
                    'station_id': station_id,
                    'name': name,
                    'latitude': latitude,
                    'longitude': longitude, 
                    'elevation': elevation,
                    'country': country,
                    'state': state,
                    'first_year': first_year,
                    'last_year': last_year
                })
                
            except Exception as e:
                logging.warning(f"Error parsing line: {line.strip()}. Error: {e}")
        
        # Create DataFrame with columns in the specific order that matches the table schema
        df = pd.DataFrame(stations_data)
        
        # Ensure the column order matches the target table schema exactly
        # Expected order: station_id, name, latitude, longitude, elevation, country, state, first_year, last_year
        column_order = ['station_id', 'name', 'latitude', 'longitude', 'elevation', 'country', 'state', 'first_year', 'last_year']
        df = df[column_order]
        
        logging.info(f"Processed {len(df)} stations")
        
        # Get database namespace from environment
        namespace = os.getenv("NAMESPACE", "climate_data")
        
        # Insert into Iceberg table using Athena
        insert_into_stations_table(df, namespace, athena_client)
        
        return df
        
    except Exception as e:
        logging.error(f"Error processing stations data: {e}")
        raise


def process_observations_chunk(chunk_data: List[Dict], year: int, month: int, chunk_counter: int, namespace: str, s3_client, athena_client) -> None:
    """
    Process and load a chunk of observations data
    
    Args:
        chunk_data: List of observation records
        year: Year of the data
        month: Month of the data
        chunk_counter: Counter for the chunk
        namespace: Database namespace
        s3_client: AWS S3 client
        athena_client: AWS Athena client
    """
    try:
        # Create a temporary directory for processing
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create dataframe from chunk
            chunk_df = pd.DataFrame(chunk_data)
            
            # Add partition columns if missing
            if 'year' not in chunk_df.columns:
                chunk_df['year'] = year
            if 'month' not in chunk_df.columns:
                chunk_df['month'] = month
            
            # Write to temporary parquet file
            parquet_file = os.path.join(tmpdir, f"chunk_{chunk_counter}.parquet")
            table = pa.Table.from_pandas(chunk_df)
            pq.write_table(table, parquet_file)
            
            # Upload to S3
            raw_bucket = os.getenv("RAW_BUCKET", "climate-lake-raw-data")
            s3_key = f"raw/observations/year={year}/month={month}/chunk_{chunk_counter}_{int(time.time())}.parquet"
            
            with open(parquet_file, 'rb') as f:
                s3_client.put_object(
                    Bucket=raw_bucket,
                    Key=s3_key,
                    Body=f
                )
            
            # Create a temporary table to hold the data
            temp_table_name = f"temp_observations_{year}_{month}_{chunk_counter}_{int(time.time())}"
            create_temp_table_query = f"""
            CREATE EXTERNAL TABLE {namespace}.{temp_table_name} (
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
            ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            LOCATION 's3://{raw_bucket}/raw/observations/year={year}/month={month}/'
            """
            
            query_id = start_query_execution(athena_client, create_temp_table_query, namespace)
            wait_for_query_to_complete(athena_client, query_id)
            
            # Insert the data into the Iceberg table
            insert_query = f"""
            INSERT INTO {namespace}.observations
            SELECT * FROM {namespace}.{temp_table_name}
            """
            
            query_id = start_query_execution(athena_client, insert_query, namespace)
            wait_for_query_to_complete(athena_client, query_id)
            
            # Drop the temporary table
            drop_query = f"DROP TABLE IF EXISTS {namespace}.{temp_table_name}"
            query_id = start_query_execution(athena_client, drop_query, namespace)
            wait_for_query_to_complete(athena_client, query_id)
            
            logger.info(f"Inserted chunk {chunk_counter} with {len(chunk_data)} records into Iceberg table")
            
    except Exception as e:
        logger.error(f"Error processing observations chunk: {e}")
        raise


def process_observations_data(year: int, stations_df: Optional[pd.DataFrame] = None) -> int:
    """
    Process GHCN observations data for a specific year and load to Iceberg
    
    Args:
        year: Year to process
        stations_df: Optional DataFrame containing station data for filtering
        
    Returns:
        Number of processed records
    """
    try:
        logger.info(f"Processing observations data for year {year}...")
        
        # Get environment variables
        raw_bucket = os.getenv("RAW_BUCKET", "climate-lake-raw-data")
        ghcn_data_url = os.getenv("GHCN_DATA_URL", "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/")
        max_records = int(os.getenv("MAX_RECORDS_PER_BATCH", "100000"))
        chunk_size = int(os.getenv("CHUNK_SIZE", "10000"))
        
        # Initialize clients
        s3_client = init_s3_client()
        athena_client = init_athena_client()
        
        if s3_client is None or athena_client is None:
            logger.error("Failed to initialize required AWS clients")
            sys.exit(1)
        
        # Download year data
        url = f"{ghcn_data_url}{year}.csv.gz"
        blob_name = f"ghcn/observations/by_year/{year}.csv.gz"
        
        try:
            content = download_and_upload_to_s3(url, raw_bucket, blob_name, s3_client)
            logger.info(f"Downloaded data for year {year}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error downloading data for year {year}: {e}")
            return 0
        
        # If we got stations data as parameter, filter only for certain stations
        # This is helpful to limit the data volume for demonstration purposes
        filter_stations = stations_df is not None
        
        # Get database namespace from environment
        namespace = os.getenv("NAMESPACE", "climate_data")
        
        # Process the CSV data
        # Format: station_id,date,element,value,m-flag,q-flag,s-flag,obs-time
        # Example: USC00045721,20150101,PRCP,0,,,P,
        
        # Process in chunks
        processed_records = 0
        chunk_counter = 0
        chunk_data = []
        
        with gzip.open(io.BytesIO(content), 'rt') as f:
            reader = csv.reader(f)
            
            for row in reader:
                if len(row) < 8:  # Skip malformed rows
                    continue
                    
                # Filter by stations if needed
                if filter_stations and row[0] not in stations_df['station_id'].values:
                    continue
                    
                # Parse and validate date format (YYYYMMDD)
                try:
                    date_str = row[1]
                    year_val = int(date_str[0:4])
                    month_val = int(date_str[4:6])
                    day_val = int(date_str[6:8])
                    date_obj = date(year_val, month_val, day_val)
                except (ValueError, IndexError):
                    continue
                
                # Add record to chunk
                chunk_data.append({
                    'station_id': row[0],
                    'date': date_obj,
                    'element': row[2],
                    'value': float(row[3]) / 10.0 if row[2] in ['TMIN', 'TMAX', 'TAVG', 'PRCP'] else float(row[3]),
                    'measurement_flag': row[4] if row[4] else None,
                    'quality_flag': row[5] if row[5] else None,
                    'source_flag': row[6] if row[6] else None,
                    'observation_time': row[7] if row[7] else None,
                    'year': year_val,
                    'month': month_val
                })
                
                processed_records += 1
                
                # When chunk is full or we've reached the max records, process the chunk
                if len(chunk_data) >= chunk_size or processed_records >= max_records:
                    # Process the chunk
                    process_observations_chunk(
                        chunk_data, 
                        year, 
                        chunk_data[0]['month'],  # Use month from first record in chunk
                        chunk_counter, 
                        namespace, 
                        s3_client, 
                        athena_client
                    )
                    
                    # Reset for next chunk
                    chunk_data = []
                    chunk_counter += 1
                    
                # If we've reached max records, stop
                if processed_records >= max_records:
                    logger.info(f"Reached maximum records limit of {max_records}")
                    break
        
        # Process any remaining data
        if chunk_data:
            process_observations_chunk(
                chunk_data, 
                year, 
                chunk_data[0]['month'],  # Use month from first record in chunk
                chunk_counter, 
                namespace, 
                s3_client, 
                athena_client
            )
            chunk_counter += 1
        
        logger.info(f"Processed {processed_records} observations for year {year}")
        return processed_records
            
    except Exception as e:
        logger.error(f"Error processing observations data for year {year}: {e}")
        raise


def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Batch loader for climate data")
    parser.add_argument("--year", type=int, default=None, 
                        help="Year to process (defaults to previous year if not specified)")
    parser.add_argument("--stations-only", action="store_true", 
                        help="Only process stations data, not observations")
    parser.add_argument("--filter-stations", action="store_true", 
                        help="Filter observations by stations (to limit data volume)")
    return parser.parse_args()


def main() -> None:
    """Main function to run the batch loader process"""
    try:
        # Load environment variables
        load_env_vars()
        
        # Parse command line arguments
        args = parse_args()
        
        # Determine which year to process
        if args.year is None:
            year = datetime.now().year - 1
            logger.info(f"No year specified, defaulting to previous year: {year}")
        else:
            year = args.year
            
        # Load stations data
        stations_df = process_stations_data()
        
        # If stations-only flag is set, skip observations processing
        if args.stations_only:
            logger.info("Only processing stations data as requested")
            return
            
        # Process observations with the specified year
        if args.filter_stations:
            logger.info("Filtering observations by stations")
            process_observations_data(year, stations_df)
        else:
            process_observations_data(year)
            
    except Exception as e:
        logger.error(f"Error in batch loader: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 