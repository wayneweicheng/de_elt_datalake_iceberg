import boto3
import os
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import json

logger = logging.getLogger(__name__)

def get_athena_client():
    """Create and return an Athena client."""
    return boto3.client('athena', region_name=os.environ.get('AWS_REGION', 'ap-southeast-2'))

def execute_athena_query(client, query: str, database: str = None) -> str:
    """
    Execute an Athena query and return the query execution ID.
    
    Args:
        client: Boto3 Athena client
        query: SQL query to execute
        database: Optional database name
    
    Returns:
        QueryExecutionId: String ID of the executed query
    """
    athena_results_bucket = os.environ.get('ATHENA_RESULTS_BUCKET', 'climate-lake-athena-results')
    athena_output_location = f"s3://{athena_results_bucket}/athena-results/"
    
    if database is None:
        database = os.environ.get('NAMESPACE', 'climate_data')
    
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': athena_output_location,
        }
    )
    
    return response['QueryExecutionId']

def wait_for_query_to_complete(client, query_execution_id: str) -> Dict[str, Any]:
    """
    Wait for an Athena query to complete and return its status.
    
    Args:
        client: Boto3 Athena client
        query_execution_id: ID of the query to wait for
    
    Returns:
        Dict: Query execution response
    """
    import time
    
    while True:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return response
        
        time.sleep(1)  # Wait 1 second before checking again

def get_query_results(client, query_execution_id: str) -> List[Dict[str, Any]]:
    """
    Get the results of a completed Athena query.
    
    Args:
        client: Boto3 Athena client
        query_execution_id: ID of the completed query
    
    Returns:
        List[Dict]: List of result rows as dictionaries
    """
    response = client.get_query_results(QueryExecutionId=query_execution_id)
    
    # Extract column names from the first row
    column_names = [col['Label'] for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    
    # Extract data rows
    data_rows = response['ResultSet']['Rows'][1:]  # Skip the header row
    
    # Convert rows to dictionaries
    result = []
    for row in data_rows:
        row_data = {}
        for i, value in enumerate(row['Data']):
            # Handle the case where the value might be None
            if 'VarCharValue' in value:
                row_data[column_names[i]] = value['VarCharValue']
            else:
                row_data[column_names[i]] = None
        result.append(row_data)
    
    return result

def list_table_snapshots(table_name: str, namespace: str = None) -> List[Dict[str, Any]]:
    """
    List all snapshots of an Iceberg table.
    
    Args:
        table_name: Name of the Iceberg table
        namespace: Optional namespace/database name
    
    Returns:
        List[Dict]: List of snapshots with their details
    """
    if namespace is None:
        namespace = os.environ.get('NAMESPACE', 'climate_data')
    
    athena_client = get_athena_client()
    
    query = f"""
    SELECT
        snapshot_id,
        TO_VARCHAR(FROM_UNIXTIME(snapshot_id / 1000000), 'yyyy-MM-dd HH:mm:ss.SSS') as committed_at,
        added_files_count,
        deleted_files_count,
        added_rows_count,
        deleted_rows_count,
        total_data_files_count
    FROM "{namespace}"."{table_name}$snapshots"
    ORDER BY snapshot_id DESC
    """
    
    query_id = execute_athena_query(athena_client, query, namespace)
    wait_for_query_to_complete(athena_client, query_id)
    return get_query_results(athena_client, query_id)

def time_travel_sql(table_name: str, 
                   snapshot_id: Optional[int] = None, 
                   timestamp: Optional[datetime] = None,
                   namespace: str = None) -> str:
    """
    Generate SQL for time travel queries.
    
    Args:
        table_name: Name of the Iceberg table
        snapshot_id: Optional snapshot ID for time travel
        timestamp: Optional timestamp for time travel
        namespace: Optional namespace/database name
    
    Returns:
        str: The table reference SQL with time travel syntax
    """
    if namespace is None:
        namespace = os.environ.get('NAMESPACE', 'climate_data')
    
    table_ref = f"{namespace}.{table_name}"
    
    if snapshot_id is not None:
        return f"{table_ref} FOR VERSION AS OF {snapshot_id}"
    elif timestamp is not None:
        # Format timestamp as 'YYYY-MM-DD HH:MM:SS'
        ts_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        return f"{table_ref} FOR TIMESTAMP AS OF TIMESTAMP '{ts_str}'"
    else:
        return table_ref 