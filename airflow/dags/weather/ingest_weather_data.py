from typing import Literal
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.utils.trigger_rule import TriggerRule

from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests


THIS_DIR = Path(__file__).parent
SQL_DIR = THIS_DIR / "sql"

# Configuration (could also use Airflow Variables or a config file)
GLUE_DATABASE = "nyc_taxi"
S3_DATA_LAKE_BUCKET = "nyc-taxi-datalake-glue-nyc-taxi"
STAGING_PREFIX = "staging/weather"  # within the bucket
ATHENA_OUTPUT_LOCATION = f"s3://{S3_DATA_LAKE_BUCKET}/athena_results/"
AWS_REGION = "us-east-1"

N_MONTHS = 3
TMeasurementTypes = Literal["prcp", "tavg", "tmin", "tmax"]
MEASUREMENT_TYPES: list[TMeasurementTypes] = ["prcp", "tavg", "tmin", "tmax"]
MEASUREMENT_NAMES = {
    "prcp": "precipitation",
    "tavg": "average_temperature", 
    "tmin": "min_temperature",
    "tmax": "max_temperature"
}


def make_download_weather_url(year: int, month: int, measurement: TMeasurementTypes) -> str:
    """
    Create download URL for weather data.
    
    Format: https://www.ncei.noaa.gov/pub/data/daily-grids/v1-0-0/averages/yyyy/{measurement}-yyyymm-cty-scaled.csv
    """
    return f"https://www.ncei.noaa.gov/pub/data/daily-grids/v1-0-0/averages/{year}/{measurement}-{year}{month:02d}-cty-scaled.csv"



@dag(
    dag_id="ingest_weather_data",
    dag_display_name="Ingest NOAA Weather Data",
    description="A DAG to ingest NOAA weather data into Glue and Athena",
    schedule=None,
    catchup=False,
)
def ingest_weather_data():
    @task
    def download_and_stage_data() -> None:
        """Download last N months of NOAA weather data and upload to S3 staging"""
        s3 = S3Hook(aws_conn_id="aws_default", region_name=AWS_REGION)

        print(f"Starting download of {N_MONTHS} months of weather data...")

        # Clear out the staging folder in S3 to avoid reprocessing old files
        old_keys = s3.list_keys(bucket_name=S3_DATA_LAKE_BUCKET, prefix=STAGING_PREFIX)
        if old_keys:
            s3.delete_objects(bucket=S3_DATA_LAKE_BUCKET, keys=old_keys)

        # Weather data is typically available with some delay, let's try 2 months back
        weather_upload_delay_months = 2
        today = datetime.now().date()
        most_recently_published_month = today - relativedelta(months=weather_upload_delay_months)

        for i in range(N_MONTHS):
            target_date = most_recently_published_month - relativedelta(months=i)
            year = target_date.year
            month = target_date.month

            for measurement in MEASUREMENT_TYPES:
                url = make_download_weather_url(year, month, measurement)
                file_name = f"{measurement}-{year}-{month:02d}.csv"

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
    def run_athena_query(
        name: str, query_fpath: Path, trigger_rule: str = "all_success"
    ) -> None:
        """Execute an Athena query from SQL file"""
        athena = AthenaHook(aws_conn_id="aws_default", region_name=AWS_REGION)

        query_execution_id = athena.run_query(
            query=query_fpath.read_text(),
            query_context={"Database": GLUE_DATABASE},
            result_configuration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
        )

        # Poll for query completion
        athena.poll_query_status(query_execution_id, max_polling_attempts=30)

        # Get query execution details to check for errors
        query_execution = athena.get_query_info(query_execution_id)
        print("\n", query_execution, "\n")
        status = query_execution["QueryExecution"]["Status"]["State"]
        print(f"{name} query status: {status}")

        if status == "FAILED":
            error_message = query_execution["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise Exception(f"{name} query failed: {error_message}")
        elif status == "CANCELLED":
            raise Exception(f"{name} query was cancelled")

        print(f"{name} completed successfully with query ID: {query_execution_id}")

    # DAG Execution: download_and_stage_data >> create_staging_table >> create_raw_table >> merge_upsert_raw_weather
    download = download_and_stage_data()
    staging = run_athena_query.override(task_id="create_staging_table")(
        "Create Staging Table", SQL_DIR / "create_staging_weather.sql"
    )
    raw = run_athena_query.override(task_id="create_raw_table")(
        "Create Raw Table", SQL_DIR / "create_raw_weather.sql"
    )
    merge = run_athena_query.override(
        task_id="merge_upsert_raw_weather", trigger_rule=TriggerRule.ALL_SUCCESS
    )("Merge Upsert Raw Weather Data From Staging To Raw Table", SQL_DIR / "merge_into_raw_weather.sql")

    # Task Dependencies
    download >> [staging, raw] >> merge


ingest_weather_data()
