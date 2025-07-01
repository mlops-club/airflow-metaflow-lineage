"""Forecasting utilities for seasonal naive and other forecast methods."""

import pandas as pd
from .athena import execute_query, query_pandas_from_athena
from pathlib import Path


def generate_seasonal_naive_forecast(
    sql_file_path: Path,
    glue_database: str,
    s3_bucket: str,
    region: str,
    as_of_datetime: str,
    lookback_days: int,
    predict_horizon_hours: int,
) -> pd.DataFrame:
    """
    Generate seasonal naive forecast using SQL.

    Args:
        sql_file_path: Path to the seasonal naive SQL file
        glue_database: AWS Glue database name
        s3_bucket: S3 bucket for query results
        region: AWS region
        as_of_datetime: As of datetime for the forecast
        lookback_days: Number of lookback days for training
        predict_horizon_hours: Number of hours to forecast

    Returns:
        DataFrame with forecast results
    """
    # Read SQL file
    sql_query = sql_file_path.read_text()

    # Prepare context for Jinja2 templating
    ctx = {
        "glue_database": glue_database,
        "s3_bucket": s3_bucket,
        "as_of_datetime": as_of_datetime,
        "lookback_days": lookback_days,
        "predict_horizon_hours": predict_horizon_hours,
    }

    return query_pandas_from_athena(
        sql_query=sql_query,
        glue_database=glue_database,
        s3_bucket=s3_bucket,
        region=region,
        ctx=ctx,
    )


def write_forecasts_to_table(
    write_sql_path: Path,
    seasonal_sql_path: Path,  # Keep for compatibility but won't use
    glue_database: str,
    s3_bucket: str,
    region: str,
    as_of_datetime: str,
    lookback_days: int,
    predict_horizon_hours: int,
) -> str:
    """
    Write forecasts to the forecast table in an idempotent manner.

    Args:
        write_sql_path: Path to the write forecasts SQL file
        seasonal_sql_path: Path to the seasonal naive forecast SQL file (unused, kept for compatibility)
        glue_database: AWS Glue database name
        s3_bucket: S3 bucket for query results
        region: AWS region
        as_of_datetime: As of datetime for the forecast
        lookback_days: Number of lookback days for training
        predict_horizon_hours: Number of hours to forecast

    Returns:
        Query execution ID for the write operation
    """
    # Read the write SQL file
    write_sql = write_sql_path.read_text()

    # Prepare context for write query templating
    write_ctx = {
        "glue_database": glue_database,
        "s3_bucket": s3_bucket,
        "as_of_datetime": as_of_datetime,
        "lookback_days": lookback_days,
        "predict_horizon_hours": predict_horizon_hours,
    }

    return execute_query(
        sql_query=write_sql,
        glue_database=glue_database,
        s3_bucket=s3_bucket,
        region=region,
        ctx=write_ctx,
    )
