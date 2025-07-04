from typing import Dict, List, Optional
from urllib.parse import urlparse
import logging
from dataclasses import dataclass

from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import (
    BaseFacet,
    DatasetFacet,
    extraction_error_run,
    external_query_run,
    schema_dataset,
    sql_job,
    symlinks_dataset,
)

from .sqlparser import SQLParser, LineageInfo


logger = logging.getLogger(__name__)


@dataclass
class AthenaConfig:
    """Configuration for Athena lineage extraction."""

    region_name: str
    database: str
    catalog: str = "AwsDataCatalog"
    output_location: Optional[str] = None


def extract_athena_lineage(
    task_name: str, query: str, config: AthenaConfig, athena_client=None, query_execution_id: Optional[str] = None
) -> LineageInfo:
    """
    Extract lineage information from an Athena SQL query.

    Args:
        task_name: Name of the task/job executing this query
        query: SQL query to analyze
        config: Athena configuration
        athena_client: Optional boto3 Athena client (will create if None)
        query_execution_id: Optional Athena query execution ID

    Returns:
        LineageInfo containing OpenLineage facets and datasets
    """
    import boto3

    if athena_client is None:
        athena_client = boto3.client("athena", region_name=config.region_name)

    return get_openlineage_facets_on_complete(
        query=query,
        database=config.database,
        athena_client=athena_client,
        catalog=config.catalog,
        query_execution_id=query_execution_id,
        output_location=config.output_location,
        region_name=config.region_name,
    )


def get_openlineage_facets_on_complete(
    query: str,
    database: str,
    athena_client,
    catalog: str = "AwsDataCatalog",
    query_execution_id: Optional[str] = None,
    output_location: Optional[str] = None,
    region_name: str = "us-east-1",
) -> LineageInfo:
    """
    Retrieve OpenLineage data by parsing SQL queries and enriching them with Athena API.

    In addition to CTAS query, query and calculation results are stored in S3 location.
    For that reason additional output is attached with this location. Instead of using the complete
    path where the results are saved (user's prefix + some UUID), we are creating a dataset with the
    user-provided path only. This should make it easier to match this dataset across different processes.
    """

    sql_parser = SQLParser(dialect="generic")

    job_facets: Dict[str, BaseFacet] = {"sql": sql_job.SQLJobFacet(query=sql_parser.normalize_sql(query))}
    parse_result = sql_parser.parse(sql=query)

    if not parse_result:
        return LineageInfo(job_facets=job_facets)

    run_facets: Dict[str, BaseFacet] = {}
    if parse_result.errors:
        run_facets["extractionError"] = extraction_error_run.ExtractionErrorRunFacet(
            totalTasks=len(query) if isinstance(query, list) else 1,
            failedTasks=len(parse_result.errors),
            errors=[
                extraction_error_run.Error(
                    errorMessage=error.message,
                    stackTrace=None,
                    task=error.origin_statement,
                    taskNumber=error.index,
                )
                for error in parse_result.errors
            ],
        )

    inputs: List[Dataset] = list(
        filter(
            None,
            [
                get_openlineage_dataset(table.schema or database, table.name, athena_client, catalog, region_name)
                for table in parse_result.in_tables
            ],
        )
    )

    outputs: List[Dataset] = list(
        filter(
            None,
            [
                get_openlineage_dataset(table.schema or database, table.name, athena_client, catalog, region_name)
                for table in parse_result.out_tables
            ],
        )
    )

    if query_execution_id:
        run_facets["externalQuery"] = external_query_run.ExternalQueryRunFacet(
            externalQueryId=query_execution_id, source="awsathena"
        )

    if output_location:
        parsed = urlparse(output_location)
        outputs.append(Dataset(namespace=f"{parsed.scheme}://{parsed.netloc}", name=parsed.path or "/"))

    return LineageInfo(job_facets=job_facets, run_facets=run_facets, inputs=inputs, outputs=outputs)


def get_openlineage_dataset(
    database: str, table: str, athena_client, catalog: str = "AwsDataCatalog", region_name: str = "us-east-1"
) -> Optional[Dataset]:
    """Get OpenLineage dataset information for an Athena table."""

    try:
        table_metadata = athena_client.get_table_metadata(CatalogName=catalog, DatabaseName=database, TableName=table)

        # Dataset has also its' physical location which we can add in symlink facet.
        s3_location = table_metadata["TableMetadata"]["Parameters"]["location"]
        parsed_path = urlparse(s3_location)
        facets: Dict[str, DatasetFacet] = {
            "symlinks": symlinks_dataset.SymlinksDatasetFacet(
                identifiers=[
                    symlinks_dataset.Identifier(
                        namespace=f"{parsed_path.scheme}://{parsed_path.netloc}",
                        name=str(parsed_path.path),
                        type="TABLE",
                    )
                ]
            )
        }
        fields = [
            schema_dataset.SchemaDatasetFacetFields(
                name=column["Name"],
                type=column["Type"],
                description=column.get("Comment"),
            )
            for column in table_metadata["TableMetadata"]["Columns"]
        ]
        if fields:
            facets["schema"] = schema_dataset.SchemaDatasetFacet(fields=fields)
        return Dataset(
            namespace=f"awsathena://athena.{region_name}.amazonaws.com",
            name=".".join(filter(None, (catalog, database, table))),
            facets=facets,
        )

    except Exception as e:
        logger.error("Cannot retrieve table metadata from Athena.Client. %s", e)
        return None
