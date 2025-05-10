#!/usr/bin/env python3
"""
Example script demonstrating Iceberg time travel queries with AWS Athena.
"""

import argparse
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Optional

# Add parent directory to path so we can import utils
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from utils.iceberg_utils import (
    get_athena_client, 
    execute_athena_query, 
    wait_for_query_to_complete,
    get_query_results, 
    list_table_snapshots, 
    time_travel_sql
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_env():
    """Load environment variables from .env file if available."""
    from dotenv import load_dotenv
    load_dotenv()

def run_time_travel_query(table_name: str, 
                        snapshot_id: Optional[int] = None, 
                        timestamp: Optional[datetime] = None, 
                        where_clause: Optional[str] = None, 
                        limit: int = 10):
    """
    Run a time travel query on an Iceberg table.
    
    Args:
        table_name: Name of the Iceberg table
        snapshot_id: Optional snapshot ID for time travel
        timestamp: Optional timestamp for time travel  
        where_clause: Optional WHERE clause for filtering
        limit: Maximum number of rows to return
    """
    namespace = os.environ.get('NAMESPACE', 'climate_data')
    
    # Get Athena client
    athena_client = get_athena_client()
    
    # Generate the table reference with time travel
    table_ref = time_travel_sql(table_name, snapshot_id, timestamp, namespace)
    
    # Build the query
    query = f"SELECT * FROM {table_ref}"
    if where_clause:
        query += f" WHERE {where_clause}"
    query += f" LIMIT {limit}"
    
    logger.info(f"Executing query: {query}")
    
    # Execute the query
    query_id = execute_athena_query(athena_client, query, namespace)
    
    # Wait for the query to complete
    response = wait_for_query_to_complete(athena_client, query_id)
    
    # Get the results
    if response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        results = get_query_results(athena_client, query_id)
        
        # Print results
        logger.info("Query results:")
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
    else:
        error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        logger.error(f"Query failed: {error_message}")

def main():
    """Main function to demonstrate Iceberg time travel."""
    # Load environment variables
    load_env()
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='Demonstrate Iceberg time travel with AWS Athena')
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
    
    # If --list-snapshots is specified, list snapshots and exit
    if args.list_snapshots:
        logger.info(f"Listing snapshots for table {args.table}")
        snapshots = list_table_snapshots(args.table)
        
        if not snapshots:
            logger.info("No snapshots found")
            return
        
        # Get column widths for pretty printing
        column_widths = {col: max(len(col), max(len(str(row[col] or '')) for row in snapshots)) 
                         for col in snapshots[0].keys()}
        
        # Print header
        header = " | ".join(f"{col:{column_widths[col]}}" for col in snapshots[0].keys())
        logger.info("-" * len(header))
        logger.info(header)
        logger.info("-" * len(header))
        
        # Print rows
        for row in snapshots:
            row_str = " | ".join(f"{str(val or ''):{column_widths[col]}}" for col, val in row.items())
            logger.info(row_str)
        
        logger.info("-" * len(header))
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
    run_time_travel_query(args.table, snapshot_id, timestamp, args.where, args.limit)

if __name__ == '__main__':
    main() 