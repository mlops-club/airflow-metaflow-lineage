"""SQL execution utilities for AWS Athena/Glue using AWS Data Wrangler."""

import awswrangler as wr
import pandas as pd
from typing import Optional, Dict, Any
from typing import Any
from jinja2 import DebugUndefined, Template



def query_pandas_from_athena(
    sql_query: str,
    glue_database: str,
    s3_bucket: str,
    s3_output_location: Optional[str] = None,
    ctx: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Execute a SQL query using AWS Data Wrangler and return results as a DataFrame.
    Use this for SELECT queries that return data.

    Args:
        sql_query: SQL query string (can contain Jinja2 template variables)
        glue_database: AWS Glue database name
        s3_bucket: S3 bucket for query results
        region: AWS region
        s3_output_location: S3 location for query results (optional)
        ctx: Optional context dictionary for Jinja2 template substitution

    Returns:
        DataFrame with query results
    """
    # Apply Jinja2 templating if context is provided
    if ctx is not None:
        sql_query = substitute_map_into_string(sql_query, ctx)

    if s3_output_location is None:
        s3_output_location = f"s3://{s3_bucket}/athena-results/"

    # Execute query using AWS Data Wrangler
    df = wr.athena.read_sql_query(
        sql=sql_query,
        database=glue_database,
        s3_output=s3_output_location,
    )
    print(f"Query executed successfully. Returned {len(df)} rows.")
    return df


def execute_query(
    sql_query: str,
    glue_database: str,
    s3_bucket: str,
    s3_output_location: Optional[str] = None,
    ctx: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Execute a DDL/DML SQL query using AWS Data Wrangler.
    Use this for CREATE, INSERT, UPDATE, DELETE, MERGE operations.

    Args:
        sql_query: SQL query string (can contain Jinja2 template variables)
        glue_database: AWS Glue database name
        s3_bucket: S3 bucket for query results
        region: AWS region
        s3_output_location: S3 location for query results (optional)
        ctx: Optional context dictionary for Jinja2 template substitution

    Returns:
        Query execution ID
    """
    # Apply Jinja2 templating if context is provided
    if ctx is not None:
        sql_query = substitute_map_into_string(sql_query, ctx)

    if s3_output_location is None:
        s3_output_location = f"s3://{s3_bucket}/athena-results/"

    # Execute DDL/DML query
    # Returns Query execution ID if wait is set to False, dictionary with the get_query_execution response otherwise.
    # https://aws-sdk-pandas.readthedocs.io/en/stable/stubs/awswrangler.athena.start_query_execution.html
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/get_query_execution.html#
    query_response = wr.athena.start_query_execution(
        sql=sql_query,
        database=glue_database,
        s3_output=s3_output_location,
        # Indicates whether to wait for the query to finish and return a dictionary with the query execution response.
        wait=True,
    )

    # Extract query execution details
    query_execution_id = query_response["QueryExecutionId"]
    query_state = query_response["Status"]["State"]

    # Check if query succeeded
    if query_state == "SUCCEEDED":
        print(f"DDL/DML query executed successfully. Query ID: {query_execution_id}")
        return query_execution_id
    elif query_state == "FAILED":
        failure_reason = query_response["Status"].get(
            "StateChangeReason", "Unknown error"
        )
        raise RuntimeError(
            f"Query failed. Query ID: {query_execution_id}. Reason: {failure_reason}"
        )
    elif query_state == "CANCELLED":
        raise RuntimeError(f"Query was cancelled. Query ID: {query_execution_id}")
    else:
        # This shouldn't happen with wait=True, but just in case
        raise RuntimeError(
            f"Query finished with unexpected state: {query_state}. Query ID: {query_execution_id}"
        )

def substitute_map_into_string(string: str, values: dict[str, Any]) -> str:
    """Format a string using a dictionary with Jinja2 templating.

    :param string: The template string containing placeholders
    :param values: A dictionary of values to substitute into the template
    """
    template = Template(string, undefined=DebugUndefined)
    return template.render(values)
