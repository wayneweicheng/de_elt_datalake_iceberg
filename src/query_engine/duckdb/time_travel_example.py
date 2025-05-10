#!/usr/bin/env python3
"""
Example script demonstrating Iceberg time travel queries with DuckDB.
"""

import argparse
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    import duckdb
except ImportError:
    logger.error("DuckDB is not installed. Please install it with: pip install duckdb")
    sys.exit(1)

def load_env():
    """Load environment variables from .env file if available."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        logger.warning("python-dotenv not installed, skipping .env file loading")

def setup_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """
    Set up DuckDB connection with Iceberg extension.
    
    Returns:
        DuckDB connection with Iceberg extension loaded
    """
    # Create connection
    con = duckdb.connect(database=':memory:')
    
    # Install Iceberg extension if needed
    try:
        con.execute("INSTALL iceberg")
        con.execute("LOAD iceberg")
    except duckdb.Error as e:
        if "Extension is already loaded" not in str(e):
            raise
        logger.info("Iceberg extension already loaded")
    
    # Configure S3 credentials if available
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_REGION', 'ap-southeast-2')
    
    if aws_access_key and aws_secret_key:
        con.execute(f"""
        SET s3_region='{aws_region}';
        SET s3_access_key_id='{aws_access_key}';
        SET s3_secret_access_key='{aws_secret_key}';
        """)
    
    return con

def get_table_s3_path(table_name: str) -> str:
    """
    Generate the S3 path to the Iceberg table.
    
    Args:
        table_name: Name of the Iceberg table
    
    Returns:
        S3 path to the Iceberg table
    """
    warehouse_bucket = os.environ.get('ICEBERG_TABLES_BUCKET', 'climate-lake-iceberg-tables')
    namespace = os.environ.get('NAMESPACE', 'climate_data')
    
    return f"s3://{warehouse_bucket}/{namespace}/{table_name}"

def list_table_snapshots(con: duckdb.DuckDBPyConnection, table_name: str) -> List[Dict[str, Any]]:
    """
    List all snapshots of an Iceberg table with DuckDB.
    
    Args:
        con: DuckDB connection
        table_name: Name of the Iceberg table
    
    Returns:
        List of snapshots with their details
    """
    table_path = get_table_s3_path(table_name)
    
    try:
        # Query the snapshots
        query = f"SELECT * FROM iceberg_scan_snapshots('{table_path}')"
        logger.info(f"Executing: {query}")
        
        # Execute and fetch the results
        result = con.execute(query).fetchall()
        
        # Get column names
        column_names = [desc[0] for desc in con.description]
        
        # Convert to list of dictionaries
        snapshots = []
        for row in result:
            snapshot = {}
            for i, column_name in enumerate(column_names):
                # Format timestamp columns
                if column_name in ['committed_at', 'timestamp_ms']:
                    if isinstance(row[i], (int, float)):
                        # Convert milliseconds or microseconds to seconds
                        timestamp = datetime.fromtimestamp(row[i] / 1000)
                        snapshot[column_name] = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    else:
                        snapshot[column_name] = row[i]
                else:
                    snapshot[column_name] = row[i]
            snapshots.append(snapshot)
        
        return snapshots
    
    except Exception as e:
        logger.error(f"Error listing snapshots: {e}")
        return []

def run_time_travel_query(con: duckdb.DuckDBPyConnection, 
                         table_name: str,
                         snapshot_id: Optional[int] = None,
                         timestamp: Optional[datetime] = None,
                         where_clause: Optional[str] = None,
                         limit: int = 10) -> List[Dict[str, Any]]:
    """
    Run a time travel query on an Iceberg table with DuckDB.
    
    Args:
        con: DuckDB connection
        table_name: Name of the Iceberg table
        snapshot_id: Optional snapshot ID for time travel
        timestamp: Optional timestamp for time travel
        where_clause: Optional WHERE clause for filtering
        limit: Maximum number of rows to return
    
    Returns:
        List of query results as dictionaries
    """
    table_path = get_table_s3_path(table_name)
    
    try:
        # Build the query based on the time travel options
        if snapshot_id is not None:
            scan_func = f"iceberg_scan_snapshot('{table_path}', {snapshot_id})"
        elif timestamp is not None:
            # Format timestamp as expected by DuckDB
            ts_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            scan_func = f"iceberg_scan_timestamp('{table_path}', TIMESTAMP '{ts_str}')"
        else:
            scan_func = f"iceberg_scan('{table_path}')"
        
        # Add WHERE clause and LIMIT if provided
        query = f"SELECT * FROM {scan_func}"
        if where_clause:
            query += f" WHERE {where_clause}"
        query += f" LIMIT {limit}"
        
        # Execute the query
        logger.info(f"Executing: {query}")
        result = con.execute(query).fetchall()
        
        # Get column names
        column_names = [desc[0] for desc in con.description]
        
        # Convert to list of dictionaries for consistent printing
        results = []
        for row in result:
            result_dict = {}
            for i, column_name in enumerate(column_names):
                result_dict[column_name] = row[i]
            results.append(result_dict)
        
        return results
    
    except Exception as e:
        logger.error(f"Error executing time travel query: {e}")
        return []

def print_results(results: List[Dict[str, Any]]):
    """
    Print query results in a tabular format.
    
    Args:
        results: Query results as a list of dictionaries
    """
    if not results:
        logger.info("No results found")
        return
    
    # Get column widths for pretty printing
    column_widths = {col: max(len(col), max(len(str(row[col] or '')) for row in results)) 
                     for col in results[0].keys()}
    
    # Print header
    header = " | ".join(f"{col:{column_widths[col]}}" for col in results[0].keys())
    logger.info("-" * len(header))
    logger.info(header)
    logger.info("-" * len(header))
    
    # Print rows
    for row in results:
        row_str = " | ".join(f"{str(val or ''):{column_widths[col]}}" for col, val in row.items())
        logger.info(row_str)
    
    logger.info("-" * len(header))

def main():
    """Main function to demonstrate Iceberg time travel with DuckDB."""
    # Load environment variables
    load_env()
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='Demonstrate Iceberg time travel with DuckDB')
    parser.add_argument('--table', type=str, default='observations', help='Iceberg table name')
    parser.add_argument('--snapshot-id', type=int, help='Snapshot ID for time travel')
    parser.add_argument('--timestamp', type=str, help='Timestamp for time travel (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--minutes-ago', type=int, help='Minutes ago for time travel')
    parser.add_argument('--where', type=str, help='WHERE clause for filtering')
    parser.add_argument('--limit', type=int, default=10, help='Maximum number of rows to return')
    parser.add_argument('--list-snapshots', action='store_true', help='List table snapshots')
    
    args = parser.parse_args()
    
    # Ensure we have the table name
    if not args.table:
        parser.error('Table name is required')
    
    # Initialize DuckDB connection
    con = setup_duckdb_connection()
    
    # If --list-snapshots is specified, list snapshots and exit
    if args.list_snapshots:
        logger.info(f"Listing snapshots for table {args.table}")
        snapshots = list_table_snapshots(con, args.table)
        print_results(snapshots)
        return
    
    # Determine time travel parameters
    snapshot_id = args.snapshot_id
    timestamp = None
    
    if args.timestamp:
        try:
            timestamp = datetime.strptime(args.timestamp, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            parser.error('Invalid timestamp format. Use YYYY-MM-DD HH:MM:SS')
    
    if args.minutes_ago and not (args.timestamp or args.snapshot_id):
        timestamp = datetime.now() - timedelta(minutes=args.minutes_ago)
    
    # Run the time travel query
    results = run_time_travel_query(con, args.table, snapshot_id, timestamp, args.where, args.limit)
    print_results(results)

if __name__ == '__main__':
    main() 