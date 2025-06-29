from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.hooks.athena_sql import AthenaSQLHook
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

THIS_DIR = Path(__file__).parent
SQL_DIR = THIS_DIR / "sql"

# Configuration (could also use Airflow Variables or a config file)
GLUE_DATABASE = "nyc_taxi"
S3_DATA_LAKE_BUCKET = "nyc-taxi-datalake-glue-nyc-taxi"
STAGING_PREFIX = "staging/yellow"  # within the bucket
ATHENA_RESULT_BUCKET = "my-athena-query-results"  # S3 bucket for Athena query outputs
ATHENA_OUTPUT_LOCATION = f"s3://{S3_DATA_LAKE_BUCKET}/athena_results/"
AWS_REGION = "us-east-1"


@dag(
    dag_id="ingest_taxi_data",
    dag_display_name="Ingest NYC Taxi Data",
    description="A DAG to ingest NYC Taxi data from S3 into Glue and Athena",
    schedule=None,
    catchup=False,
)
def ingest_taxi_data():

    @task
    def download_and_stage_data() -> None:
        """Download last 3 months of NYC Taxi data and upload to S3 staging"""
        s3 = S3Hook(aws_conn_id="aws_default", region_name=AWS_REGION)
        
        # Clear out the staging folder in S3 to avoid reprocessing old files
        old_keys = s3.list_keys(bucket_name=S3_DATA_LAKE_BUCKET, prefix=STAGING_PREFIX)
        print(f"{old_keys=}")
        if old_keys:
            s3.delete_objects(bucket=S3_DATA_LAKE_BUCKET, keys=old_keys)

        # Determine the last 3 full month year-month strings (e.g., '2025-05', '2025-04', '2025-03')
        today = datetime.now(timezone.utc).date()
        last_3_months = [
            (today - timedelta(days=30 * i)).strftime("%Y-%m") for i in range(1, 4)
        ]

        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        for ym in last_3_months:
            file_name = f"yellow_tripdata_{ym}.parquet"
            url = f"{base_url}/{file_name}"
            print(f"Downloading {url} ...")
            response = requests.get(url)
            response.raise_for_status()
            # Save directly to S3
            s3_key = f"{STAGING_PREFIX}/{file_name}"
            s3.load_bytes(
                response.content,
                key=s3_key,
                bucket_name=S3_DATA_LAKE_BUCKET,
                replace=True,
            )
            print(f"Uploaded {file_name} to s3://{S3_DATA_LAKE_BUCKET}/{s3_key}")

    @task
    def run_athena_query(name: str, query_fpath: Path, trigger_rule: str = "all_success") -> None:
        athena = AthenaHook(aws_conn_id="aws_default", region_name=AWS_REGION)
        
        query_execution_id = athena.run_query(
            query=query_fpath.read_text(),
            query_context={"Database": GLUE_DATABASE},
            result_configuration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
        )
        
        # Poll for query completion
        athena.poll_query_status(query_execution_id, max_polling_attempts=30)
        
        # Get query execution details to check for errors
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/get_query_execution.html
        query_execution = athena.get_query_info(query_execution_id)
        print("\n", query_execution, "\n")
        status = query_execution['QueryExecution']['Status']['State']
        print(f"{name} query status: {status}")
        
        if status == 'FAILED':
            error_message = query_execution['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            raise Exception(f"{name} query failed: {error_message}")
        elif status == 'CANCELLED':
            raise Exception(f"{name} query was cancelled")
        
        print(f"{name} completed successfully with query ID: {query_execution_id}")
    
    

    # DAG Execution
    download = download_and_stage_data()
    staging = run_athena_query.override(task_id="create_staging_table")("Create Staging", SQL_DIR / "create_staging_yellow.sql")
    raw = run_athena_query.override(task_id="create_raw_table")("Create Raw", SQL_DIR / "create_raw_yellow.sql")
    merge = run_athena_query.override(task_id="merge_upsert_raw_yellow", trigger_rule=TriggerRule.ALL_SUCCESS)(
        "Merge Upsert", SQL_DIR / "merge_into_raw_yellow.sql"
    )
    
    # Task Dependencies
    download >> [staging, raw] >> merge

ingest_taxi_data()