from typing import Literal
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

try:
    # Airflow 3+
    from airflow.sdk import dag, task, Variable
except ImportError:
    # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models import Variable

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
ATHENA_OUTPUT_LOCATION = f"s3://{S3_DATA_LAKE_BUCKET}/athena-results"

N_MONTHS = 3
TMeasurementTypes = Literal["prcp", "tavg", "tmin", "tmax"]
MEASUREMENT_TYPES: list[TMeasurementTypes] = ["prcp", "tavg", "tmin", "tmax"]
MEASUREMENT_NAMES = {
    "prcp": "precipitation",
    "tavg": "average_temperature",
    "tmin": "min_temperature",
    "tmax": "max_temperature",
}


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


def make_download_weather_url(year: int, month: int, measurement: TMeasurementTypes) -> str:
    return f"https://www.ncei.noaa.gov/pub/data/daily-grids/v1-0-0/averages/{year}/{measurement}-{year}{month:02d}-cty-scaled.csv"


def weather_file_already_exists(s3: S3Hook, year: int, month: int, measurement: TMeasurementTypes) -> bool:
    """Check if weather data file for a given year-month-measurement already exists in S3 staging"""
    file_name = f"{measurement}-{year}-{month:02d}.csv"
    s3_key = f"{STAGING_PREFIX}/{file_name}"

    try:
        return s3.check_for_key(key=s3_key, bucket_name=S3_DATA_LAKE_BUCKET)
    except Exception as e:
        print(f"Error checking if {s3_key} exists: {e}")
        return False


def download_and_stage_weather_file_if_not_exists(
    s3: S3Hook, year: int, month: int, measurement: TMeasurementTypes
) -> bool:
    """Download and stage weather data file if it doesn't exist. Returns True if downloaded."""
    if weather_file_already_exists(s3, year, month, measurement):
        file_name = f"{measurement}-{year}-{month:02d}.csv"
        print(f"File {file_name} already exists in S3, skipping download")
        return False

    url = make_download_weather_url(year, month, measurement)
    file_name = f"{measurement}-{year}-{month:02d}.csv"
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
def download_and_stage_data() -> None:
    """Download last N months of NOAA weather data and upload to S3 staging"""
    s3 = S3Hook(aws_conn_id="aws_default", region_name=AWS_REGION)

    print(f"Starting idempotent download of {N_MONTHS} months of weather data...")

    # Weather data is typically available with some delay, let's try 3 months back
    weather_upload_delay_months = 3
    today = datetime.now().date()
    most_recently_published_month = today - relativedelta(months=weather_upload_delay_months)

    files_downloaded = 0
    files_skipped = 0

    for i in range(N_MONTHS):
        target_date = most_recently_published_month - relativedelta(months=i)
        year = target_date.year
        month = target_date.month

        for measurement in MEASUREMENT_TYPES:
            if download_and_stage_weather_file_if_not_exists(s3, year, month, measurement):
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
        output_location=f"{ATHENA_OUTPUT_LOCATION}/{query_file.stem}",
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )


this_dag = ingest_weather_data()

if __name__ == "__main__":
    # This line allows us to step debug / trip breakpoints
    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/debug.html
    this_dag.test()
