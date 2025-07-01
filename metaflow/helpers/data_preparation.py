"""Data preparation utilities for training data."""

from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from .athena import execute_query, query_pandas_from_athena



def prepare_training_data(
    sql_file_path: Path,
    glue_database: str,
    s3_bucket: str,
    as_of_datetime: str,
    lookback_days: int,
) -> pd.DataFrame:
    """
    Prepare training data from actuals table.

    Args:
        sql_file_path: Path to the prepare training data SQL file
        glue_database: AWS Glue database name
        s3_bucket: S3 bucket for query results
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
        ctx=ctx,
    )
