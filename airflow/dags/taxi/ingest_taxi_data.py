from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

from datetime import datetime, timezone, timedelta
from pathlib import Path

from dags.consts import GLUE_DATABASE, AWS_REGION, S3_DATA_LAKE_BUCKET

import requests

THIS_DIR = Path(__file__).parent
SQL_DIR = THIS_DIR / "sql"

# Configuration (could also use Airflow Variables or a config file)
STAGING_PREFIX = "staging/yellow"  # within the bucket
ATHENA_RESULT_BUCKET = "my-athena-query-results"  # S3 bucket for Athena query outputs
ATHENA_OUTPUT_LOCATION = f"s3://{S3_DATA_LAKE_BUCKET}/athena_results/"


@dag(
    dag_id="ingest_taxi_data",
    dag_display_name="Ingest NYC Taxi Data",
    description="A DAG to ingest NYC Taxi data from S3 into Glue and Athena",
    schedule=None,
    catchup=False,
)
def ingest_taxi_data():

    download = download_and_stage_data()

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
