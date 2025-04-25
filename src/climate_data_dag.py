"""
climate_data_dag.py - Airflow DAG for climate data ELT pipeline orchestration

This DAG orchestrates the climate data extraction, loading, and transformation pipeline,
including batch data extraction, data quality checks, and BigQuery transformations.
"""

from datetime import datetime, timedelta
import os
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeOperator
from airflow.models import Variable

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Get environment variables from Airflow Variables
# These can be set in the Airflow UI or using the CLI
GCP_PROJECT_ID = Variable.get('GCP_PROJECT_ID', 'climate-data-lake-iceberg')
GCP_REGION = Variable.get('GCP_REGION', 'us-central1')
RAW_BUCKET = Variable.get('RAW_BUCKET', 'climate-lake-raw-data')
ICEBERG_CATALOG_BUCKET = Variable.get('ICEBERG_CATALOG_BUCKET', 'climate-lake-iceberg-catalog')
ICEBERG_TABLES_BUCKET = Variable.get('ICEBERG_TABLES_BUCKET', 'climate-lake-iceberg-tables')

# Define the DAG
with DAG(
    'climate_data_pipeline',
    default_args=default_args,
    description='Climate Data Lake ELT Pipeline',
    schedule_interval='0 1 * * *',  # Run daily at 1:00 AM UTC
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['climate', 'data-lake', 'iceberg'],
) as dag:

    # Task 1: Load Stations Data
    load_stations = CloudFunctionInvokeOperator(
        task_id='load_stations_data',
        function_id='climate-data-loader',
        location=GCP_REGION,
        project_id=GCP_PROJECT_ID,
        data=json.dumps({'load_stations_only': True}),
        gcp_conn_id='google_cloud_default',
    )

    # Task 2: Load Observations Data (for yesterday)
    # Uses macros to dynamically set the date
    load_observations = CloudFunctionInvokeOperator(
        task_id='load_observations_data',
        function_id='climate-data-loader',
        location=GCP_REGION,
        project_id=GCP_PROJECT_ID,
        data=json.dumps({
            'year': '{{ execution_date.year }}',
            'month': '{{ execution_date.month }}',
            'day': '{{ execution_date.day }}',
            'filter_stations': True
        }),
        gcp_conn_id='google_cloud_default',
    )

    # Task 3: Run Data Quality Check on Iceberg Tables
    run_dq_check = BashOperator(
        task_id='run_data_quality_check',
        bash_command=f"""
            python -c '
            from pyiceberg.catalog import load_catalog
            import os
            
            # Set up credentials - in Airflow this is handled by the connection
            # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/credentials.json"
            
            # Create catalog config
            catalog_config = {{
                "type": "rest",
                "uri": "gs://{ICEBERG_CATALOG_BUCKET}",
                "warehouse": "gs://{ICEBERG_TABLES_BUCKET}",
                "credential": "gcp",
                "region": "{GCP_REGION}"
            }}
            
            # Load catalog and tables
            iceberg_catalog = load_catalog("climate_catalog", **catalog_config)
            stations_table = iceberg_catalog.load_table("climate_data.stations")
            observations_table = iceberg_catalog.load_table("climate_data.observations")
            
            # Get counts for data quality check
            stations_count = stations_table.scan().to_arrow().num_rows
            observations_count = observations_table.scan().to_arrow().num_rows
            
            print(f"Stations count: {{stations_count}}")
            print(f"Observations count: {{observations_count}}")
            
            # Check minimum thresholds
            if stations_count < 10:
                raise ValueError(f"Stations count too low: {{stations_count}}")
            
            if observations_count < 100:
                raise ValueError(f"Observations count too low: {{observations_count}}")
            '
        """,
    )

    # Task 4: Refresh BigQuery Materialized View
    refresh_bigquery_view = BigQueryExecuteQueryOperator(
        task_id='refresh_bigquery_view',
        sql='''
        SELECT * FROM climate_data_lake.annual_climate_summary_refresh
        ''',
        destination_dataset_table=f'{GCP_PROJECT_ID}:climate_data_lake.annual_climate_summary',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location=GCP_REGION,
        gcp_conn_id='google_cloud_default',
    )

    # Task 5: Perform Iceberg Maintenance (compaction)
    run_iceberg_maintenance = BashOperator(
        task_id='run_iceberg_maintenance',
        bash_command=f"""
            python -c '
            from pyiceberg.catalog import load_catalog
            import os
            
            # Set up catalog config
            catalog_config = {{
                "type": "rest",
                "uri": "gs://{ICEBERG_CATALOG_BUCKET}",
                "warehouse": "gs://{ICEBERG_TABLES_BUCKET}",
                "credential": "gcp",
                "region": "{GCP_REGION}"
            }}
            
            # Load catalog and tables
            iceberg_catalog = load_catalog("climate_catalog", **catalog_config)
            stations_table = iceberg_catalog.load_table("climate_data.stations")
            observations_table = iceberg_catalog.load_table("climate_data.observations")
            
            # Perform maintenance operations
            print("Running rewrite_data_files on stations table")
            stations_table.rewrite_data_files()
            
            print("Running rewrite_data_files on observations table")
            observations_table.rewrite_data_files()
            
            print("Iceberg maintenance completed successfully")
            '
        """,
    )
    
    # Task 6: Cleanup old metadata files (to prevent excessive GCS costs)
    cleanup_old_metadata = BashOperator(
        task_id='cleanup_old_metadata',
        bash_command=f"""
            # Calculate date 30 days ago in YYYY-MM-DD format
            CLEANUP_DATE=$(date -d "30 days ago" +%Y-%m-%d)
            
            # Run metadata cleanup using gcloud storage command
            echo "Cleaning up metadata files older than $CLEANUP_DATE"
            gcloud storage ls gs://{ICEBERG_CATALOG_BUCKET}/metadata/files/ | grep ".*_${{CLEANUP_DATE}}.*" | xargs -I{{}} gcloud storage rm {{}} || echo "No files to clean up"
            
            echo "Metadata cleanup completed"
        """,
    )
    
    # Task 7: Generate a daily report of data loaded
    generate_data_report = PythonOperator(
        task_id='generate_data_report',
        python_callable=lambda ds, **kwargs: print(f"""
        Climate Data Lake Report for {ds}
        -------------------------------
        Date: {ds}
        Execution Date: {kwargs['execution_date']}
        Loaded observations for year: {kwargs['execution_date'].year}
        
        Check BigQuery for the latest analytics views
        """),
        provide_context=True,
    )

    # Define task dependencies
    load_stations >> load_observations >> run_dq_check >> refresh_bigquery_view >> run_iceberg_maintenance >> cleanup_old_metadata >> generate_data_report 