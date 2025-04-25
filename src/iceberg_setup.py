#!/usr/bin/env python
"""
iceberg_setup.py - Sets up Apache Iceberg catalog and tables for climate data

This script configures the Iceberg catalog on Google Cloud Storage and creates
the necessary tables for storing climate data (stations and observations).
"""

import os
import logging
import argparse
from typing import Dict, Any, Optional
import sys
from pathlib import Path

# Add the parent directory to sys.path to allow importing from src
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog

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


def create_catalog_config() -> Dict[str, str]:
    """Create Iceberg catalog configuration from environment variables"""
    try:
        catalog_config = {
            "type": "rest",
            "uri": f"gs://{os.getenv('ICEBERG_CATALOG_BUCKET', 'climate-lake-iceberg-catalog')}",
            "warehouse": f"gs://{os.getenv('ICEBERG_TABLES_BUCKET', 'climate-lake-iceberg-tables')}",
            "credential": "gcp",
            "region": os.getenv("GCP_REGION", "us-central1"),
        }
        return catalog_config
    except Exception as e:
        logger.error(f"Error creating catalog config: {e}")
        raise


def setup_iceberg_catalog(catalog_name: str) -> Any:
    """Create and configure Iceberg catalog"""
    try:
        # Get credential path from environment variable
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_path:
            logger.info(f"Using credentials from {credentials_path}")
            # Ensure the credentials path exists
            if not os.path.exists(credentials_path):
                logger.warning(f"Credentials file not found at {credentials_path}")
        else:
            logger.warning("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
            
        # Create catalog configuration
        catalog_config = create_catalog_config()
        logger.info(f"Setting up Iceberg catalog with config: {catalog_config}")
        
        # Load the catalog
        iceberg_catalog = load_catalog(catalog_name, **catalog_config)
        logger.info(f"Successfully loaded Iceberg catalog: {catalog_name}")
        return iceberg_catalog
        
    except Exception as e:
        logger.error(f"Error setting up Iceberg catalog: {e}")
        raise


def create_namespace(iceberg_catalog: Any, namespace: str) -> None:
    """Create a namespace in the Iceberg catalog if it doesn't exist"""
    try:
        # Check if namespace already exists
        namespaces = iceberg_catalog.list_namespaces()
        if (namespace,) in namespaces:
            logger.info(f"Namespace {namespace} already exists")
        else:
            # Create the namespace
            logger.info(f"Creating namespace: {namespace}")
            iceberg_catalog.create_namespace(namespace)
            logger.info(f"Successfully created namespace: {namespace}")
    except Exception as e:
        logger.error(f"Error creating namespace {namespace}: {e}")
        raise


def define_stations_schema() -> Dict[str, Any]:
    """Define schema for weather stations metadata"""
    return {
        "type": "struct",
        "fields": [
            {"id": 1, "name": "station_id", "type": "string", "required": True},
            {"id": 2, "name": "name", "type": "string"},
            {"id": 3, "name": "latitude", "type": "double"},
            {"id": 4, "name": "longitude", "type": "double"},
            {"id": 5, "name": "elevation", "type": "double"},
            {"id": 6, "name": "country", "type": "string"},
            {"id": 7, "name": "state", "type": "string"},
            {"id": 8, "name": "first_year", "type": "int"},
            {"id": 9, "name": "last_year", "type": "int"}
        ]
    }


def define_observations_schema() -> Dict[str, Any]:
    """Define schema for daily weather observations"""
    return {
        "type": "struct",
        "fields": [
            {"id": 1, "name": "station_id", "type": "string", "required": True},
            {"id": 2, "name": "date", "type": "date", "required": True},
            {"id": 3, "name": "element", "type": "string", "required": True},
            {"id": 4, "name": "value", "type": "double"},
            {"id": 5, "name": "measurement_flag", "type": "string"},
            {"id": 6, "name": "quality_flag", "type": "string"},
            {"id": 7, "name": "source_flag", "type": "string"},
            {"id": 8, "name": "observation_time", "type": "timestamp"}
        ]
    }


def create_table(iceberg_catalog: Any, namespace: str, table_name: str, 
                schema: Dict[str, Any], partition_spec: Optional[list] = None,
                properties: Optional[Dict[str, str]] = None) -> None:
    """Create a table in the Iceberg catalog if it doesn't exist"""
    try:
        # Check if the table already exists
        table_identifier = (namespace, table_name)
        try:
            iceberg_catalog.load_table(f"{namespace}.{table_name}")
            logger.info(f"Table {namespace}.{table_name} already exists")
            return
        except Exception:
            # Table doesn't exist, proceed with creation
            pass
        
        # Set default properties if not provided
        if properties is None:
            properties = {
                "format-version": "2",
                "write.parquet.compression-codec": "zstd",
                "write.metadata.compression-codec": "gzip"
            }
            
        # Create the table
        logger.info(f"Creating table: {namespace}.{table_name}")
        if partition_spec:
            logger.info(f"Using partition spec: {partition_spec}")
            iceberg_catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties
            )
        else:
            iceberg_catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                properties=properties
            )
        logger.info(f"Successfully created table: {namespace}.{table_name}")
    except Exception as e:
        logger.error(f"Error creating table {namespace}.{table_name}: {e}")
        raise


def main() -> None:
    """Main function to set up Iceberg catalog and tables"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Set up Iceberg catalog and tables for climate data")
    parser.add_argument("--catalog", default=None, help="Name of the Iceberg catalog")
    parser.add_argument("--namespace", default=None, help="Namespace for the Iceberg tables")
    args = parser.parse_args()
    
    # Load environment variables
    load_env_vars()
    
    # Get catalog and namespace names from arguments or environment variables
    catalog_name = args.catalog or os.getenv("CATALOG_NAME", "climate_catalog")
    namespace = args.namespace or os.getenv("NAMESPACE", "climate_data")
    
    try:
        # Set up Iceberg catalog
        iceberg_catalog = setup_iceberg_catalog(catalog_name)
        
        # Create namespace
        create_namespace(iceberg_catalog, namespace)
        
        # Define schemas
        stations_schema = define_stations_schema()
        observations_schema = define_observations_schema()
        
        # Create tables
        # Stations table
        stations_table_properties = {
            "format-version": "2",
            "write.parquet.compression-codec": "zstd",
            "write.metadata.compression-codec": "gzip"
        }
        create_table(
            iceberg_catalog=iceberg_catalog,
            namespace=namespace,
            table_name="stations",
            schema=stations_schema,
            properties=stations_table_properties
        )
        
        # Observations table with partitioning
        observations_partitioning = [
            {"name": "year", "transform": "year(date)"},
            {"name": "month", "transform": "month(date)"}
        ]
        observations_table_properties = {
            "format-version": "2",
            "write.parquet.compression-codec": "zstd",
            "write.metadata.compression-codec": "gzip"
        }
        create_table(
            iceberg_catalog=iceberg_catalog,
            namespace=namespace,
            table_name="observations",
            schema=observations_schema,
            partition_spec=observations_partitioning,
            properties=observations_table_properties
        )
        
        logger.info("Iceberg tables created successfully!")
        
    except Exception as e:
        logger.error(f"Error setting up Iceberg: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 