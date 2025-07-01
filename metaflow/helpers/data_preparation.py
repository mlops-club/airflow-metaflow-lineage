"""Data preparation utilities for training data."""

from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from .athena import execute_query, query_pandas_from_athena


def compute_actuals(
    sql_file_path: Path,
    glue_database: str,
    s3_bucket: str,
    region: str,
    as_of_datetime: str,
    lookback_days: int,
) -> str:
    """
    Compute actuals from raw taxi data.

    Args:
        sql_file_path: Path to the compute actuals SQL file
        glue_database: AWS Glue database name
        s3_bucket: S3 bucket for query results
        region: AWS region
        as_of_datetime: As of datetime for the computation
        lookback_days: Number of lookback days for training

    Returns:
        Query execution ID for the DDL/DML operation
    """
    # Calculate start and end datetime
    as_of_dt = datetime.fromisoformat(as_of_datetime.replace("Z", "+00:00"))
    start_dt = as_of_dt - timedelta(days=lookback_days)

    # Read SQL file
    sql_query = sql_file_path.read_text()

    # Prepare context for Jinja2 templating
    ctx = {
        "glue_database": glue_database,
        "s3_bucket": s3_bucket,
        "start_datetime": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "end_datetime": as_of_dt.strftime("%Y-%m-%d %H:%M:%S"),
    }

    return execute_query(
        sql_query=sql_query,
        glue_database=glue_database,
        s3_bucket=s3_bucket,
        region=region,
        ctx=ctx,
    )


def prepare_training_data(
    sql_file_path: Path,
    glue_database: str,
    s3_bucket: str,
    region: str,
    as_of_datetime: str,
    lookback_days: int,
) -> pd.DataFrame:
    """
    Prepare training data from actuals table.

    Args:
        sql_file_path: Path to the prepare training data SQL file
        glue_database: AWS Glue database name
        s3_bucket: S3 bucket for query results
        region: AWS region
        as_of_datetime: As of datetime for the training
        lookback_days: Number of lookback days for training

    Returns:
        DataFrame with training data
    """
    # Read SQL file
    sql_query = sql_file_path.read_text()

    # Prepare context for Jinja2 templating
    ctx = {
        "glue_database": glue_database,
        "s3_bucket": s3_bucket,
        "as_of_datetime": as_of_datetime,
        "lookback_days": lookback_days,
    }

    return query_pandas_from_athena(
        sql_query=sql_query,
        glue_database=glue_database,
        s3_bucket=s3_bucket,
        region=region,
        ctx=ctx,
    )
