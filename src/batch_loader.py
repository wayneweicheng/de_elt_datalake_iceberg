#!/usr/bin/env python
"""
batch_loader.py - Batch extraction and loading of historical climate data

This script downloads and processes historical climate data from NOAA sources,
and loads it into Iceberg tables in a Google Cloud Storage data lake.
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
            "region": os.getenv("GCP_REGION", "us-central1")
        }
        
        # Load the catalog
        iceberg_catalog = load_catalog(catalog_name, **catalog_config)
        return iceberg_catalog
    except Exception as e:
        logger.error(f"Error initializing Iceberg catalog: {e}")
        raise


def download_and_upload_to_gcs(url: str, bucket_name: str, blob_name: str, 
                              storage_client: storage.Client) -> bytes:
    """
    Download data from URL and upload to GCS bucket
    
    Args:
        url: URL to download data from
        bucket_name: GCS bucket name
        blob_name: GCS blob name
        storage_client: Initialized GCS client
        
    Returns:
        Content of the downloaded file as bytes
    """
    try:
        logger.info(f"Downloading {url}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        logger.info(f"Upload to gs://{bucket_name}/{blob_name}")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(response.content)
        logger.info(f"Uploaded to gs://{bucket_name}/{blob_name}")
        
        return response.content
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading from {url}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")
        raise


def process_stations_data() -> pd.DataFrame:
    """
    Process GHCN stations data and load to Iceberg
    
    Returns:
        DataFrame containing stations data
    """
    try:
        logger.info("Processing stations data...")
        
        # Get environment variables
        raw_bucket = os.getenv("RAW_BUCKET", "climate-lake-raw-data")
        ghcn_stations_url = os.getenv("GHCN_STATIONS_URL",
                                     "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt")
        
        # Initialize clients
        storage_client = init_gcs_client()
        iceberg_catalog = init_iceberg_catalog()
        
        # Download stations data
        content = download_and_upload_to_gcs(
            ghcn_stations_url, 
            raw_bucket, 
            "ghcn/stations/ghcnd-stations.txt",
            storage_client
        )
        
        # Parse fixed-width format
        # Format spec: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
        stations_data = []
        for line in content.decode('utf-8').splitlines():
            if len(line) < 85:  # Skip malformed lines
                continue
            
            try:
                stations_data.append({
                    'station_id': line[0:11].strip(),
                    'latitude': float(line[12:20].strip() or 0),
                    'longitude': float(line[21:30].strip() or 0),
                    'elevation': float(line[31:37].strip() or 0),
                    'name': line[41:71].strip(),
                    'country': line[38:40].strip(),
                    'state': line[38:40].strip() if line[38:40].strip() == 'US' else '',
                    'first_year': int(line[74:79].strip() or 0),
                    'last_year': int(line[79:85].strip() or 0)
                })
            except (ValueError, IndexError) as e:
                logger.warning(f"Error parsing line: {line}. Error: {e}")
                continue
        
        # Create DataFrame
        df = pd.DataFrame(stations_data)
        logger.info(f"Processed {len(df)} stations")
        
        # Get Iceberg table
        namespace = os.getenv("NAMESPACE", "climate_data")
        stations_table = iceberg_catalog.load_table(f"{namespace}.stations")
        
        # Write to temporary Parquet file
        with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
            table = pa.Table.from_pandas(df)
            pq.write_table(table, tmp.name)
            
            # Upload to GCS
            stations_blob_name = f"iceberg/stations/data/stations_{int(time.time())}.parquet"
            bucket = storage_client.bucket(raw_bucket)
            blob = bucket.blob(stations_blob_name)
            blob.upload_from_filename(tmp.name)
        
        # Register data with Iceberg
        stations_table.append(f"gs://{raw_bucket}/{stations_blob_name}")
        
        logger.info(f"Loaded {len(df)} stations to Iceberg table")
        return df
        
    except Exception as e:
        logger.error(f"Error processing stations data: {e}")
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
        storage_client = init_gcs_client()
        iceberg_catalog = init_iceberg_catalog()
        
        # Download year data
        url = f"{ghcn_data_url}{year}.csv.gz"
        blob_name = f"ghcn/observations/by_year/{year}.csv.gz"
        
        try:
            content = download_and_upload_to_gcs(url, raw_bucket, blob_name, storage_client)
            logger.info(f"Downloaded data for year {year}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error downloading data for year {year}: {e}")
            return 0
        
        # If we got stations data as parameter, filter only for certain stations
        # This is helpful to limit the data volume for demonstration purposes
        filter_stations = stations_df is not None
        
        # Process the CSV data
        # Format: station_id,date,element,value,m-flag,q-flag,s-flag,obs-time
        # Example: USC00045721,20150101,PRCP,0,,,P,
        
        # Create a temporary directory for processing chunks
        with tempfile.TemporaryDirectory() as tmpdir:
            # Get Iceberg table
            namespace = os.getenv("NAMESPACE", "climate_data")
            observations_table = iceberg_catalog.load_table(f"{namespace}.observations")
            
            # Process in chunks and write each chunk as a separate file
            parquet_files = []
            
            # Process the gzipped CSV
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
                        date_obj = date(int(date_str[0:4]), int(date_str[4:6]), int(date_str[6:8]))
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
                        'observation_time': row[7] if row[7] else None
                    })
                    
                    processed_records += 1
                    
                    # When chunk is full or we've reached the max records, write to parquet
                    if len(chunk_data) >= chunk_size or processed_records >= max_records:
                        # Create dataframe from chunk
                        chunk_df = pd.DataFrame(chunk_data)
                        
                        # Write to parquet
                        parquet_file = os.path.join(tmpdir, f"chunk_{chunk_counter}.parquet")
                        table = pa.Table.from_pandas(chunk_df)
                        pq.write_table(table, parquet_file)
                        
                        # Upload to GCS
                        parquet_blob_name = f"iceberg/observations/data/year={year}/chunk_{chunk_counter}_{int(time.time())}.parquet"
                        bucket = storage_client.bucket(raw_bucket)
                        blob = bucket.blob(parquet_blob_name)
                        blob.upload_from_filename(parquet_file)
                        parquet_files.append(f"gs://{raw_bucket}/{parquet_blob_name}")
                        
                        # Log progress
                        logger.info(f"Processed chunk {chunk_counter} with {len(chunk_data)} records")
                        
                        # Reset for next chunk
                        chunk_data = []
                        chunk_counter += 1
                        
                    # If we've reached max records, stop
                    if processed_records >= max_records:
                        logger.info(f"Reached maximum records limit of {max_records}")
                        break
            
            # Process any remaining data
            if chunk_data:
                # Create dataframe from chunk
                chunk_df = pd.DataFrame(chunk_data)
                
                # Write to parquet
                parquet_file = os.path.join(tmpdir, f"chunk_{chunk_counter}.parquet")
                table = pa.Table.from_pandas(chunk_df)
                pq.write_table(table, parquet_file)
                
                # Upload to GCS
                parquet_blob_name = f"iceberg/observations/data/year={year}/chunk_{chunk_counter}_{int(time.time())}.parquet"
                bucket = storage_client.bucket(raw_bucket)
                blob = bucket.blob(parquet_blob_name)
                blob.upload_from_filename(parquet_file)
                parquet_files.append(f"gs://{raw_bucket}/{parquet_blob_name}")
                
                # Log progress
                logger.info(f"Processed final chunk {chunk_counter} with {len(chunk_data)} records")
                chunk_counter += 1
            
            # Register all parquet files with Iceberg
            if parquet_files:
                observations_table.append(parquet_files)
                logger.info(f"Loaded {processed_records} observations to Iceberg table for year {year}")
            else:
                logger.warning(f"No observations were processed for year {year}")
            
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