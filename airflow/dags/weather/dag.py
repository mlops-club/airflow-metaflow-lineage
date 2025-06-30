from typing import Literal
from airflow.sdk import dag, task, Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests


THIS_DIR = Path(__file__).parent
SQL_DIR = THIS_DIR / "sql"

# Note: unfortunately, step debugging fails whenever we access variables :(
AWS_REGION = Variable.get("datalake-aws-region")
S3_DATA_LAKE_BUCKET = Variable.get("datalake-s3-bucket")
GLUE_DATABASE = Variable.get("datalake-glue-database")

STAGING_PREFIX = "staging/weather"  # within the bucket
ATHENA_OUTPUT_LOCATION = f"s3://{S3_DATA_LAKE_BUCKET}/athena_results/"

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
    description="Ingest NCEI weather data -> Iceberg in S3 + Glue Catalog",
)
def ingest_weather_data():

    download = download_and_stage_data()

    create_staging_table_if_not_exists = make_athena_query_operator(
        task_id="create_staging_table_if_not_exists",
        query_file=SQL_DIR / "create_staging_weather.sql",
    )
    create_raw_weather_table_if_not_exists = make_athena_query_operator(
        task_id="create_raw_weather_table_if_not_exists",
        query_file=SQL_DIR / "create_raw_weather.sql",
    )
    merge_upsert_staging_into_raw_weather = make_athena_query_operator(
        task_id="merge_upsert_staging_into_raw_weather",
        query_file=SQL_DIR / "merge_into_raw_weather.sql",
    )

    # Task Dependencies
    _ = (
        download
        >> [
            create_staging_table_if_not_exists,
            create_raw_weather_table_if_not_exists,
        ]
        >> merge_upsert_staging_into_raw_weather
    )


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

this_dag = ingest_weather_data()

if __name__ == "__main__":
    # This line allows us to step debug / trip breakpoints
    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/debug.html
    this_dag.test()
