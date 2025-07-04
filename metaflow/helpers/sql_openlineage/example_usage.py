#!/usr/bin/env python3
"""
Example usage of the refactored framework-agnostic SQL parser and lineage extraction.

This shows how to use the pure functions instead of the Airflow-specific classes.
"""

from .sqlparser import SQLParser, DatabaseInfo
from .athena_extractor import extract_athena_lineage, AthenaConfig
import boto3


def example_sql_parser_usage():
    """Example of how to use the framework-agnostic SQL parser."""
    
    # Create database info for your specific database
    database_info = DatabaseInfo(
        scheme="postgresql",
        authority="localhost:5432",
        database="my_database",
        is_uppercase_names=False,
    )
    
    # Create SQL parser
    sql_parser = SQLParser(dialect="postgresql", default_schema="public")
    
    # Example SQL query
    query = """
    CREATE TABLE weather_features AS
    SELECT 
        date,
        temperature,
        humidity,
        wind_speed
    FROM raw_weather 
    WHERE date >= '2023-01-01'
    """
    
    # Parse and extract lineage
    lineage_info = sql_parser.generate_openlineage_metadata_from_sql(
        sql=query,
        database_info=database_info,
        database="my_database"
    )
    
    print(f"Job facets: {lineage_info.job_facets}")
    print(f"Run facets: {lineage_info.run_facets}")
    print(f"Input datasets: {[ds.name for ds in lineage_info.inputs]}")
    print(f"Output datasets: {[ds.name for ds in lineage_info.outputs]}")


def example_athena_usage():
    """Example of how to use the framework-agnostic Athena extractor."""
    
    # Configuration for Athena
    config = AthenaConfig(
        region_name="us-east-1",
        database="my_database", 
        output_location="s3://my-bucket/query-results/",
        catalog="AwsDataCatalog"
    )
    
    # Example SQL query
    query = """
    CREATE TABLE weather_features AS
    SELECT 
        date,
        temperature,
        humidity,
        wind_speed
    FROM raw_weather 
    WHERE date >= '2023-01-01'
    """
    
    # Task name (could be from Metaflow, Airflow, or any other orchestrator)
    task_name = "my_flow.create_weather_features"
    
    # Optional: provide your own Athena client
    athena_client = boto3.client('athena', region_name=config.region_name)
    
    # Extract lineage information
    try:
        lineage_info = extract_athena_lineage(
            task_name=task_name,
            query=query,
            config=config,
            athena_client=athena_client  # Optional - will create one if None
        )
        
        print(f"Task: {task_name}")
        print(f"Job facets: {lineage_info.job_facets}")
        print(f"Run facets: {lineage_info.run_facets}")
        print(f"Input datasets: {[ds.name for ds in lineage_info.inputs]}")
        print(f"Output datasets: {[ds.name for ds in lineage_info.outputs]}")
        
        # You can now emit this to your OpenLineage backend
        # emit_openlineage_event(lineage_info)
        
    except Exception as e:
        print(f"Error extracting lineage: {e}")


def emit_openlineage_event(lineage_info):
    """
    Example function showing how you might emit the lineage information 
    to an OpenLineage backend.
    """
    from openlineage.client.run import RunEvent, RunState, Job, Run
    from openlineage.client.client import OpenLineageClient
    from datetime import datetime
    import uuid
    
    # Create OpenLineage client
    client = OpenLineageClient(url="http://your-openlineage-backend:5000")
    
    # Create run event
    run_event = RunEvent(
        eventTime=datetime.now().isoformat(),
        producer="https://github.com/your-org/your-repo",
        schemaURL="https://openlineage.io/spec/1-0-3/OpenLineage.json#/definitions/RunEvent",
        eventType=RunState.COMPLETE,
        run=Run(
            runId=str(uuid.uuid4()),
            facets=lineage_info.run_facets
        ),
        job=Job(
            namespace="your-namespace",
            name="your-job-name",
            facets=lineage_info.job_facets
        ),
        inputs=lineage_info.inputs,
        outputs=lineage_info.outputs
    )
    
    # Emit the event
    client.emit(run_event)


if __name__ == "__main__":
    print("=== SQL Parser Example ===")
    example_sql_parser_usage()
    
    print("\n=== Athena Extractor Example ===")
    example_athena_usage()
