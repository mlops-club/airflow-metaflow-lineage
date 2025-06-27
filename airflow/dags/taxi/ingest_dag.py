from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.utils.trigger_rule import TriggerRule

from pathlib import Path

THIS_DIR = Path(__file__).parent
SQL_DIR = THIS_DIR / "sql"

# Configuration (could also use Airflow Variables or a config file)
GLUE_DATABASE = "nyc_taxi"
S3_DATA_LAKE_BUCKET = "nyc-taxi-datalake-glue-nyc-taxi"
STAGING_PREFIX = "staging/yellow"  # within the bucket
ATHENA_RESULT_BUCKET = "my-athena-query-results"  # S3 bucket for Athena query outputs
ATHENA_OUTPUT_LOCATION = f"s3://{ATHENA_RESULT_BUCKET}/athena_results/"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_data_pipeline",
    start_date=datetime(2025, 6, 1),
    schedule_interval="@monthly",  # e.g., run monthly to grab latest data
    catchup=False,
    default_args=default_args,
) as dag:

    # Task 1: Download last 3 months of data and upload to S3 staging
    def download_and_stage_data():
        s3 = S3Hook(aws_conn_id='aws_default')  # uses configured AWS credentials

        # Clear out the staging folder in S3 to avoid reprocessing old files
        staging_path = f"s3://{S3_DATA_LAKE_BUCKET}/{STAGING_PREFIX}/"
        s3.delete_objects(bucket=S3_DATA_LAKE_BUCKET, keys=s3.list_keys(S3_DATA_LAKE_BUCKET, prefix=STAGING_PREFIX))

        # Determine the last 3 full month year-month strings (e.g., '2025-05', '2025-04', '2025-03')
        today = datetime.utcnow().date()
        year = today.year
        month = today.month
        # get first day of current month, then subtract one day to get last day of prev month
        first_of_month = today.replace(day=1)
        last_day_prev_month = first_of_month - timedelta(days=1)
        # Last 3 months list
        last3 = []
        curr = last_day_prev_month
        for _ in range(3):
            last3.append(curr.strftime("%Y-%m"))
            # move to previous month end
            curr = curr.replace(day=1) - timedelta(days=1)
        # e.g., if today is 2025-06-26, last3 = ['2025-05', '2025-04', '2025-03']

        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        for ym in last3:
            file_name = f"yellow_tripdata_{ym}.parquet"
            url = f"{base_url}/{file_name}"
            print(f"Downloading {url} ...")
            response = requests.get(url)
            response.raise_for_status()
            # Save directly to S3
            s3_key = f"{STAGING_PREFIX}/{file_name}"
            s3.load_bytes(response.content, key=s3_key, bucket_name=S3_DATA_LAKE_BUCKET, replace=True)
            print(f"Uploaded {file_name} to s3://{S3_DATA_LAKE_BUCKET}/{s3_key}")

    download_task = PythonOperator(
        task_id="download_and_stage_data",
        python_callable=download_and_stage_data,
    )

    create_staging_table = AthenaOperator(
        task_id="create_staging_table",
        query=(SQL_DIR / "create_staging_yellow.sql").read_text(),
        database=GLUE_DATABASE,
        output_location=ATHENA_OUTPUT_LOCATION,
        aws_conn_id="aws_default",
    )

    create_raw_table = AthenaOperator(
        task_id="create_raw_table",
        query=(SQL_DIR / "create_raw_yellow.sql").read_text(),
        database=GLUE_DATABASE,
        output_location=ATHENA_OUTPUT_LOCATION,
        aws_conn_id="aws_default",
    )

    merge_upsert = AthenaOperator(
        task_id="merge_upsert_raw_yellow",
        query=(SQL_DIR / "merge_into_raw_yellow.sql").read_text(),
        database=GLUE_DATABASE,
        output_location=ATHENA_OUTPUT_LOCATION,
        aws_conn_id="aws_default",
        # Ensure this runs even if some earlier file had no data (we don't want to fail the whole DAG if no new data)
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Define task dependencies
    download_task >> [create_staging_table, create_raw_table] >> merge_upsert
