"""SQL execution utilities for AWS Athena/Glue using AWS Data Wrangler."""

import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import awswrangler as wr
import boto3
import pandas as pd
from jinja2 import DebugUndefined, Template
from openlineage.client.facet import ParentRunFacet
from openlineage.client.facet_v2 import sql_job
from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.uuid import generate_new_uuid
from rich import print

from .openlineage import (
    FLOW_LINEAGE_SINGLETON,
    _create_openlineage_client,
    _create_processing_engine_facet,
    get_current_step_context,
)
from .sql_openlineage.sqlparser import LineageInfo


def _is_valid_snake_case_identifier(name: str) -> bool:
    """
    Validate that a string is a valid lower snake case identifier.

    Args:
        name: String to validate

    Returns:
        True if valid, False otherwise

    """
    if not name:
        return False

    # Check if it matches snake_case pattern: lowercase letters, numbers, underscores
    # Must start with a letter or underscore, no consecutive underscores, no trailing underscore
    pattern = r"^[a-z_][a-z0-9_]*[a-z0-9]$|^[a-z]$"
    return bool(re.match(pattern, name)) and "__" not in name


def query_pandas_from_athena(
    sql_query: str,
    glue_database: str,
    datalake_s3_bucket: str,
    job_name: str,
    s3_output_location: Optional[str] = None,
    ctx: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Execute a SQL query using AWS Data Wrangler and return results as a DataFrame.
    Use this for SELECT queries that return data.

    Args:
        sql_query: SQL query string (can contain Jinja2 template variables)
        glue_database: AWS Glue database name
        datalake_s3_bucket: S3 bucket for query results
        job_name: Job name identifier (must be valid lower snake case identifier)
        s3_output_location: S3 location for query results (optional)
        ctx: Optional context dictionary for Jinja2 template substitution

    Returns:
        DataFrame with query results

    """
    # Validate job_name format
    if not _is_valid_snake_case_identifier(job_name):
        raise ValueError(f"job_name must be a valid lower snake case identifier. Got: {job_name}")

    # Apply Jinja2 templating if context is provided
    if ctx is not None:
        sql_query = substitute_map_into_string(sql_query, ctx)

    if s3_output_location is None:
        s3_output_location = f"s3://{datalake_s3_bucket}/athena-results"  # /{job_name}"

    # Emit OpenLineage START event for SQL query
    query_run_id = str(generate_new_uuid())
    emit_openlineage_start_event(job_name, sql_query, query_run_id)

    # Execute query using AWS Data Wrangler
    df = wr.athena.read_sql_query(
        sql=sql_query,
        database=glue_database,
        s3_output=s3_output_location,
    )

    # Extract lineage information after successful query execution
    try:
        from .sql_openlineage.athena_extractor import (
            AthenaConfig,
            extract_athena_lineage,
        )

        # Get region from current session or default
        session = boto3.Session()
        region_name = session.region_name or "us-east-1"

        # Create Athena configuration
        config = AthenaConfig(
            region_name=region_name,
            database=glue_database,
            catalog="AwsDataCatalog",
            output_location=s3_output_location,
        )

        # Create Athena client
        athena_client = boto3.client("athena", region_name=config.region_name)

        # Extract lineage information
        lineage_info = extract_athena_lineage(
            task_name=job_name,
            query=sql_query,
            config=config,
            athena_client=athena_client,
            query_execution_id=None,  # SELECT queries don't have execution IDs readily available
        )

        # print(f"=== Lineage Information for '{job_name}' ===")
        # print(f"Job facets: {lineage_info.job_facets}")
        # print(f"Run facets: {lineage_info.run_facets}")
        # print(f"Input datasets: {[ds.name for ds in lineage_info.inputs]}")
        # print(f"Output datasets: {[ds.name for ds in lineage_info.outputs]}")
        # print("=" * 50)

        # Emit OpenLineage COMPLETE event using the same run_id as the START event
        emit_openlineage_complete_event(lineage_info, job_name, query_run_id)

    except Exception as e:
        print(f"Warning: Failed to extract lineage for '{job_name}': {e}")

    print(f"Query '{job_name}' executed successfully. Returned {len(df)} rows.")
    return df


def execute_query(
    sql_query: str,
    glue_database: str,
    datalake_s3_bucket: str,
    job_name: str,
    s3_output_location: Optional[str] = None,
    ctx: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Execute a DDL/DML SQL query using AWS Data Wrangler.
    Use this for CREATE, INSERT, UPDATE, DELETE, MERGE operations.

    Args:
        sql_query: SQL query string (can contain Jinja2 template variables)
        glue_database: AWS Glue database name
        datalake_s3_bucket: S3 bucket for query results
        job_name: Job name identifier (must be valid lower snake case identifier)
        s3_output_location: S3 location for query results (optional)
        ctx: Optional context dictionary for Jinja2 template substitution

    Returns:
        Query execution ID

    """
    # Validate job_name format
    if not _is_valid_snake_case_identifier(job_name):
        raise ValueError(f"job_name must be a valid lower snake case identifier. Got: {job_name}")

    # Apply Jinja2 templating if context is provided
    if ctx is not None:
        sql_query = substitute_map_into_string(sql_query, ctx)

    if s3_output_location is None:
        s3_output_location = f"s3://{datalake_s3_bucket}/athena-results"  # /{job_name}"

    # Emit OpenLineage START event for SQL query
    query_run_id = str(generate_new_uuid())
    emit_openlineage_start_event(job_name, sql_query, query_run_id)

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

    # Extract lineage information after successful query execution
    if query_state == "SUCCEEDED":
        try:
            from .sql_openlineage.athena_extractor import (
                AthenaConfig,
                extract_athena_lineage,
            )

            # Get region from current session or default
            session = boto3.Session()
            region_name = session.region_name or "us-east-1"

            # Create Athena configuration
            config = AthenaConfig(
                region_name=region_name,
                database=glue_database,
                catalog="AwsDataCatalog",
                output_location=s3_output_location,
            )

            # Extract lineage information
            lineage_info = extract_athena_lineage(
                task_name=job_name,
                query=sql_query,
                config=config,
                query_execution_id=query_execution_id,
            )

            # print(f"=== Lineage Information for '{job_name}' ===")
            # print(f"Job facets: {lineage_info.job_facets}")
            # print(f"Run facets: {lineage_info.run_facets}")
            # print(f"Input datasets: {[ds.name for ds in lineage_info.inputs]}")
            # print(f"Output datasets: {[ds.name for ds in lineage_info.outputs]}")
            # print("=" * 50)

            # Emit OpenLineage COMPLETE event
            emit_openlineage_complete_event(lineage_info, job_name, query_run_id)

        except Exception as e:
            print(f"Warning: Failed to extract lineage for '{job_name}': {e}")

    # Check if query succeeded
    if query_state == "SUCCEEDED":
        print(f"DDL/DML query '{job_name}' executed successfully. Query ID: {query_execution_id}")
        return query_execution_id
    elif query_state == "FAILED":
        failure_reason = query_response["Status"].get("StateChangeReason", "Unknown error")
        raise RuntimeError(f"Query '{job_name}' failed. Query ID: {query_execution_id}. Reason: {failure_reason}")
    elif query_state == "CANCELLED":
        raise RuntimeError(f"Query '{job_name}' was cancelled. Query ID: {query_execution_id}")
    else:
        # This shouldn't happen with wait=True, but just in case
        raise RuntimeError(
            f"Query '{job_name}' finished with unexpected state: {query_state}. Query ID: {query_execution_id}"
        )


def substitute_map_into_string(string: str, values: dict[str, Any]) -> str:
    """
    Format a string using a dictionary with Jinja2 templating.

    :param string: The template string containing placeholders
    :param values: A dictionary of values to substitute into the template
    """
    template = Template(string, undefined=DebugUndefined)
    return template.render(values)


def emit_openlineage_start_event(job_name: str, sql_query: str, run_id: str, namespace: str = "default") -> None:
    """
    Emit an OpenLineage START event for SQL query with proper parent context.

    Args:
        job_name: Name of the SQL job/query
        sql_query: The SQL query being executed
        run_id: The run ID for this query execution
        namespace: OpenLineage namespace (default: "default")
    """
    client = _create_openlineage_client()

    # Get parent step context
    step_context = get_current_step_context()

    # Create job facets with SQL
    job_facets = {"sql": sql_job.SQLJobFacet(query=sql_query)}

    # Create run facets with parent and root references
    run_facets: dict[str, Any] = {
        "processing_engine": _create_processing_engine_facet(name="sagemaker", version="1.0.0"),
    }
    if step_context:
        # Get flow context for root reference
        if FLOW_LINEAGE_SINGLETON.flow_run and FLOW_LINEAGE_SINGLETON.flow_job:
            #     run_facets["parent"] = {
            #         "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.30.1/client/python",
            #         # "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet",
            #         "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
            #         "job": {"namespace": step_context.job.namespace, "name": step_context.job.name},
            #         "run": {"runId": step_context.run.runId},
            #         "root": {
            #             "job": {
            #                 "namespace": FLOW_LINEAGE_SINGLETON.flow_job.namespace,
            #                 "name": FLOW_LINEAGE_SINGLETON.flow_job.name,
            #             },
            #             "run": {"runId": FLOW_LINEAGE_SINGLETON.flow_run.runId},
            #         },
            #     }
            run_facets["parent"] = ParentRunFacet(
                run={"runId": step_context.run.runId},
                job={"namespace": step_context.job.namespace, "name": step_context.job.name},
            )

    # Create job and run objects
    job = Job(namespace=namespace, name=job_name, facets=job_facets)
    run = Run(runId=run_id, facets=run_facets)

    # Create and emit START event
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=run,
        job=job,
        producer="https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/sagemaker",
    )

    client.emit(event)
    print(f"Emitted OpenLineage START event for SQL job '{job_name}'")


def emit_openlineage_complete_event(
    lineage_info: LineageInfo, job_name: str, run_id: str, namespace: str = "default"
) -> None:
    """
    Emit an OpenLineage COMPLETE event with lineage information.

    Args:
        lineage_info: LineageInfo object containing job facets, run facets, inputs, and outputs
        job_name: Name of the job/task
        run_id: The run ID for this query execution
        namespace: OpenLineage namespace (default: "default")

    """
    client = _create_openlineage_client()

    # Create job and run objects
    job = Job(namespace=namespace, name=job_name, facets=lineage_info.job_facets)
    run = Run(runId=run_id, facets=lineage_info.run_facets)

    # Convert input datasets
    run_inputs = []
    for dataset in lineage_info.inputs:
        run_inputs.append(
            Dataset(
                namespace=dataset.namespace,
                name=dataset.name,
                facets=dataset.facets or {},
            )
        )

    # Convert output datasets
    run_outputs = []
    for dataset in lineage_info.outputs:
        run_outputs.append(
            Dataset(
                namespace=dataset.namespace,
                name=dataset.name,
                facets=dataset.facets or {},
            )
        )

    # Create and emit the COMPLETE event with input and output datasets
    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=run,
        job=job,
        inputs=run_inputs if run_inputs else None,
        outputs=run_outputs if run_outputs else None,
        producer="https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/sagemaker",
    )

    # from openlineage.client.serde import Serde
    # from pathlib import Path

    # Path("openlineage-events").mkdir(parents=True, exist_ok=True)
    # with open(f"openlineage-events/{job_name}_{run_id}.json", "w") as f:
    #     f.write(Serde.to_json(event))

    client.emit(event)
    print(
        f"Emitted OpenLineage COMPLETE event for job '{job_name}' with {len(lineage_info.inputs)} inputs and {len(lineage_info.outputs)} outputs"
    )
