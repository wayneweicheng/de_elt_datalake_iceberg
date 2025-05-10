# Python with DuckDB
import duckdb
import os
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Install and load the Iceberg extension
con = duckdb.connect()
con.execute("INSTALL iceberg")
con.execute("LOAD iceberg")

# Get AWS credentials from boto3 (which uses ~/.aws/credentials)
session = boto3.Session()
credentials = session.get_credentials()
aws_region = os.getenv("AWS_REGION", "ap-southeast-2")

# Configure AWS credentials for DuckDB
con.execute(f"""
    SET s3_region='{aws_region}';
    SET s3_access_key_id='{credentials.access_key}';
    SET s3_secret_access_key='{credentials.secret_key}';
""")

# Set the unsafe_enable_version_guessing to true as indicated in the error message
# This is needed when we don't have version hint information
con.execute("SET unsafe_enable_version_guessing = true;")

# Get bucket and namespace information
warehouse_bucket = os.getenv("ICEBERG_TABLES_BUCKET", "climate-lake-iceberg-tables")
namespace = os.getenv("NAMESPACE", "climate_data")

print(f"Connecting to Iceberg tables in s3://{warehouse_bucket}/{namespace}")

# Using the AWS Glue catalog through direct Iceberg table access
# This respects the existing AWS Glue catalog by using the same S3 paths
print("\nQuerying stations table:")
try:
    # Use the iceberg_scan function to directly read the Iceberg table at the S3 path
    # This accesses the same metadata that the Glue catalog points to
    query = f"SELECT * FROM iceberg_scan('s3://{warehouse_bucket}/{namespace}/stations') LIMIT 10"
    print(f"Executing: {query}")
    result = con.execute(query).fetchall()
    print("\nResults:")
    print(result)
    
    # Get table metadata to understand the structure
    print("\nTable structure:")
    con.execute(f"DESCRIBE iceberg_scan('s3://{warehouse_bucket}/{namespace}/stations')")
    table_structure = con.fetchall()
    print(table_structure)
    
    # Get iceberg table metadata
    print("\nIceberg metadata:")
    con.execute(f"SELECT * FROM iceberg_scan_metadata('s3://{warehouse_bucket}/{namespace}/stations')")
    metadata = con.fetchall()
    print(metadata)
    
except Exception as e:
    print(f"Error accessing Iceberg table: {e}")
    
print("\nQuerying observations table:")
try:
    # Try querying the observations table
    query = f"SELECT * FROM iceberg_scan('s3://{warehouse_bucket}/{namespace}/observations') LIMIT 10"
    print(f"Executing: {query}")
    result = con.execute(query).fetchall()
    print("\nResults:")
    print(result)
except Exception as e:
    print(f"Error accessing observations table: {e}")

# Example of how to perform a SQL query with filtering
print("\nQuerying with filter:")
try:
    # Example query with filtering (adjust column names as needed based on your schema)
    query = f"""
    SELECT 
        station_id, 
        date, 
        element, 
        value
    FROM iceberg_scan('s3://{warehouse_bucket}/{namespace}/observations')
    WHERE year = 2022 AND month = 1
    LIMIT 10
    """
    print(f"Executing filtered query")
    result = con.execute(query).fetchall()
    print("\nFiltered results:")
    print(result)
except Exception as e:
    print(f"Error executing filtered query: {e}")