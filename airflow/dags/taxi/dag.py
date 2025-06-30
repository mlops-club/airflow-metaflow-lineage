from airflow.sdk import dag, task, Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

THIS_DIR = Path(__file__).parent
SQL_DIR = THIS_DIR / "sql"

# Note: unfortunately, step debugging fails whenever we access variables :(
AWS_REGION = Variable.get("datalake-aws-region")
S3_DATA_LAKE_BUCKET = Variable.get("datalake-s3-bucket")
GLUE_DATABASE = Variable.get("datalake-glue-database")

STAGING_PREFIX = "staging/yellow"  # within the bucket
ATHENA_RESULT_BUCKET = "my-athena-query-results"  # S3 bucket for Athena query outputs
ATHENA_OUTPUT_LOCATION = f"s3://{S3_DATA_LAKE_BUCKET}/athena_results/"


@dag(
    dag_id="ingest_yellow_taxi_data",
    description="Ingest NYC Taxi Yellow Line data -> Iceberg in S3 + Glue Catalog",
)
def ingest_taxi_data():

    download = download_and_stage_data_idempotently()

    create_staging_table_if_not_exists = make_athena_query_operator(
        task_id="create_staging_table_if_not_exists",
        query_file=SQL_DIR / "create_staging_yellow.sql",
    )
    create_raw_yellow_table_if_not_exists = make_athena_query_operator(
        task_id="create_raw_yellow_table_if_not_exists",
        query_file=SQL_DIR / "create_raw_yellow.sql",
    )
    merge_upsert_staging_into_raw_yellow = make_athena_query_operator(
        task_id="merge_upsert_staging_into_raw_yellow",
        query_file=SQL_DIR / "merge_into_raw_yellow.sql",
    )

    # Task Dependencies
    _ = (
        download
        >> [
            create_staging_table_if_not_exists,
            create_raw_yellow_table_if_not_exists,
        ]
        >> merge_upsert_staging_into_raw_yellow
    )


def yyyy_mm_already_exists(s3: S3Hook, year: int, month: int) -> bool:
    """Check if taxi data for a given year-month already exists in S3 staging"""
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    s3_key = f"{STAGING_PREFIX}/{file_name}"
    
    try:
        return s3.check_for_key(key=s3_key, bucket_name=S3_DATA_LAKE_BUCKET)
    except Exception as e:
        print(f"Error checking if {s3_key} exists: {e}")
        return False


def download_and_stage_yyyy_mm_if_not_exists(s3: S3Hook, year: int, month: int) -> bool:
    """Download and stage taxi data for a given year-month if it doesn't exist. Returns True if downloaded."""
    if yyyy_mm_already_exists(s3, year, month):
        print(f"File yellow_tripdata_{year}-{month:02d}.parquet already exists in S3, skipping download")
        return False
    
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    s3_key = f"{STAGING_PREFIX}/{file_name}"
    
    print(f"Downloading {url} ...")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Save directly to S3
        s3.load_bytes(
            response.content,
            key=s3_key,
            bucket_name=S3_DATA_LAKE_BUCKET,
            replace=True,
        )
        print(f"Uploaded {file_name} to s3://{S3_DATA_LAKE_BUCKET}/{s3_key}")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {file_name}: {e}")
        return False


@task
def download_and_stage_data_idempotently() -> None:
    """Download last 3 months of NYC Taxi data and upload to S3 staging"""
    s3 = S3Hook(aws_conn_id="aws_default", region_name=AWS_REGION)

    # Determine the last 3 full months
    today = datetime.now(timezone.utc).date()
    target_months = []
    for i in range(1, 4):  # Last 3 months
        target_date = today - timedelta(days=30 * i)
        target_months.append((target_date.year, target_date.month))

    files_downloaded = 0
    files_skipped = 0
    
    for year, month in target_months:
        if download_and_stage_yyyy_mm_if_not_exists(s3, year, month):
            files_downloaded += 1
        else:
            files_skipped += 1
    
    print(f"Download summary: {files_downloaded} files downloaded, {files_skipped} files skipped")

def make_athena_query_operator(
    task_id: str,
    query_file: Path,
) -> AthenaOperator:
    """Helper function to create an AthenaOperator with common parameters"""
    return AthenaOperator(
        task_id=task_id,
        query=query_file.read_text(),
        database=GLUE_DATABASE,
        output_location=ATHENA_OUTPUT_LOCATION,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )

this_dag = ingest_taxi_data()

if __name__ == "__main__":
    # This line allows us to step debug / trip breakpoints
    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/debug.html
    this_dag.test()
