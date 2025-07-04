from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TypedDict, Dict, List, Optional, Any, Protocol

import sqlparse
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import (
    BaseFacet, 
    column_lineage_dataset, 
    extraction_error_run, 
    sql_job
)
from openlineage.common.sql import DbTableMeta, SqlMeta, parse


# Type aliases to replace Airflow-specific types
TablesHierarchy = Dict[Optional[str], Dict[Optional[str], List[str]]]


class DatabaseConnection(Protocol):
    """Protocol for database connections."""
    
    def execute(self, query: str) -> Any:
        """Execute a query and return results."""
        ...
    
    def get_schema_info(self, database: str, schema: str, table: str) -> Dict[str, Any]:
        """Get schema information for a table."""
        ...

log = logging.getLogger(__name__)

DEFAULT_NAMESPACE = "default"
DEFAULT_INFORMATION_SCHEMA_COLUMNS = [
    "table_schema",
    "table_name",
    "column_name",
    "ordinal_position",
    "udt_name",
]
DEFAULT_INFORMATION_SCHEMA_TABLE_NAME = "information_schema.columns"


def default_normalize_name_method(name: str) -> str:
    return name.lower()


class LineageInfo:
    """Container for lineage information, replaces OperatorLineage."""
    
    def __init__(
        self, 
        job_facets: Optional[Dict[str, BaseFacet]] = None,
        run_facets: Optional[Dict[str, BaseFacet]] = None,
        inputs: Optional[List[Dataset]] = None,
        outputs: Optional[List[Dataset]] = None
    ):
        self.job_facets = job_facets or {}
        self.run_facets = run_facets or {}
        self.inputs = inputs or []
        self.outputs = outputs or []


class GetTableSchemasParams(TypedDict):
    """get_table_schemas params."""

    normalize_name: Callable[[str], str]
    is_cross_db: bool
    information_schema_columns: List[str]
    information_schema_table: str
    use_flat_cross_db_query: bool
    is_uppercase_names: bool
    database: Optional[str]


class DatabaseInfo:
    """
    Contains database specific information needed to process SQL statement parse result.

    :param scheme: Scheme part of URI in OpenLineage namespace.
    :param authority: Authority part of URI in OpenLineage namespace.
        For most cases it should return `{host}:{port}` part of connection.
        See: https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
    :param database: Takes precedence over parsed database name.
    :param information_schema_columns: List of columns names from information schema table.
    :param information_schema_table_name: Information schema table name.
    :param use_flat_cross_db_query: Specifies whether a single, "global" information schema table should
        be used for cross-database queries (e.g., in Redshift), or if multiple, per-database "local"
        information schema tables should be queried individually.

        If True, assumes a single, universal information schema table is available
        (for example, in Redshift, the `SVV_REDSHIFT_COLUMNS` view)
        [https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_REDSHIFT_COLUMNS.html].
        In this mode, we query only `information_schema_table_name` directly.
        Depending on the `is_information_schema_cross_db` argument, you can also filter
        by database name in the WHERE clause.

        If False, treats each database as having its own local information schema table containing
        metadata for that database only. As a result, one query per database may be generated
        and then combined (often via `UNION ALL`).
        This approach is necessary for dialects that do not maintain a single global view of
        all metadata or that require per-database queries.
        Depending on the `is_information_schema_cross_db` argument, queries can
        include or omit database information in both identifiers and filters.

        See `is_information_schema_cross_db` which also affects how final queries are constructed.
    :param is_information_schema_cross_db: Specifies whether database information should be tracked
        and included in queries that retrieve schema information from the information_schema_table.
        In short, this determines whether queries are capable of spanning multiple databases.

        If True, database identifiers are included wherever applicable, allowing retrieval of
        metadata from more than one database. For instance, in Snowflake or MS SQL
        (where each database is treated as a top-level namespace), you might have a query like:

        ```
        SELECT ...
        FROM db1.information_schema.columns WHERE ...
        UNION ALL
        SELECT ...
        FROM db2.information_schema.columns WHERE ...
        ```

        In Redshift, setting this to True together with `use_flat_cross_db_query=True` allows
        adding database filters to the query, for example:

        ```
        SELECT ...
        FROM SVV_REDSHIFT_COLUMNS
        WHERE
        SVV_REDSHIFT_COLUMNS.database == db1  # This is skipped when False
        AND SVV_REDSHIFT_COLUMNS.schema == schema1
        AND SVV_REDSHIFT_COLUMNS.table IN (table1, table2)
        OR ...
        ```

        However, certain databases (e.g., PostgreSQL) do not permit true cross-database queries.
        In such dialects, enabling cross-database support may lead to errors or be unnecessary.
        Always consult your dialect's documentation or test sample queries to confirm if
        cross-database querying is supported.

        If False, database qualifiers are ignored, effectively restricting queries to a single
        database (or making the database-level qualifier optional). This is typically
        safer for databases that do not support cross-database operations or only provide a
        two-level namespace (schema + table) instead of a three-level one (database + schema + table).
        For example, some MySQL or PostgreSQL contexts might not need or permit cross-database queries at all.

        See `use_flat_cross_db_query` which also affects how final queries are constructed.
    :param is_uppercase_names: Specifies if database accepts only uppercase names (e.g. Snowflake).
    :param normalize_name_method: Method to normalize database, schema and table names.
        Defaults to `name.lower()`.
    """

    def __init__(
        self,
        scheme: str,
        authority: Optional[str] = None,
        database: Optional[str] = None,
        information_schema_columns: Optional[List[str]] = None,
        information_schema_table_name: str = DEFAULT_INFORMATION_SCHEMA_TABLE_NAME,
        use_flat_cross_db_query: bool = False,
        is_information_schema_cross_db: bool = False,
        is_uppercase_names: bool = False,
        normalize_name_method: Callable[[str], str] = default_normalize_name_method,
    ):
        self.scheme = scheme
        self.authority = authority
        self.database = database
        self.information_schema_columns = information_schema_columns or DEFAULT_INFORMATION_SCHEMA_COLUMNS
        self.information_schema_table_name = information_schema_table_name
        self.use_flat_cross_db_query = use_flat_cross_db_query
        self.is_information_schema_cross_db = is_information_schema_cross_db
        self.is_uppercase_names = is_uppercase_names
        self.normalize_name_method = normalize_name_method


def from_table_meta(
    table_meta: DbTableMeta, database: Optional[str], namespace: str, is_uppercase: bool
) -> Dataset:
    if table_meta.database:
        name = table_meta.qualified_name
    elif database:
        name = f"{database}.{table_meta.schema}.{table_meta.name}"
    else:
        name = f"{table_meta.schema}.{table_meta.name}"
    return Dataset(namespace=namespace, name=name if not is_uppercase else name.upper())


class SQLParser:
    """
    Interface for openlineage-sql.

    :param dialect: dialect specific to the database
    :param default_schema: schema applied to each table with no schema parsed
    """

    def __init__(self, dialect: Optional[str] = None, default_schema: Optional[str] = None) -> None:
        self.dialect = dialect
        self.default_schema = default_schema
        self.log = logging.getLogger(self.__class__.__name__)

    def parse(self, sql: List[str] | str) -> Optional[SqlMeta]:
        """Parse a single or a list of SQL statements."""
        self.log.debug(
            "OpenLineage calling SQL parser with SQL %s dialect %s schema %s",
            sql,
            self.dialect,
            self.default_schema,
        )
        return parse(sql=sql, dialect=self.dialect, default_schema=self.default_schema)

    def get_metadata_from_parser(
        self,
        inputs: List[DbTableMeta],
        outputs: List[DbTableMeta],
        database_info: DatabaseInfo,
        namespace: str = DEFAULT_NAMESPACE,
        database: Optional[str] = None,
    ) -> tuple[List[Dataset], List[Dataset]]:
        database = database if database else database_info.database
        return [
            from_table_meta(dataset, database, namespace, database_info.is_uppercase_names)
            for dataset in inputs
        ], [
            from_table_meta(dataset, database, namespace, database_info.is_uppercase_names)
            for dataset in outputs
        ]

    def attach_column_lineage(
        self, datasets: List[Dataset], database: Optional[str], parse_result: SqlMeta
    ) -> None:
        """
        Attaches column lineage facet to the list of datasets.

        Note that currently each dataset has the same column lineage information set.
        This would be a matter of change after OpenLineage SQL Parser improvements.
        """
        if not len(parse_result.column_lineage):
            return
        for dataset in datasets:
            dataset.facets = dataset.facets or {}
            dataset.facets["columnLineage"] = column_lineage_dataset.ColumnLineageDatasetFacet(
                fields={
                    column_lineage.descendant.name: column_lineage_dataset.Fields(
                        inputFields=[
                            column_lineage_dataset.InputField(
                                namespace=dataset.namespace,
                                name=".".join(
                                    filter(
                                        None,
                                        (
                                            column_meta.origin.database or database,
                                            column_meta.origin.schema or self.default_schema,
                                            column_meta.origin.name,
                                        ),
                                    )
                                )
                                if column_meta.origin
                                else "",
                                field=column_meta.name,
                            )
                            for column_meta in column_lineage.lineage
                        ],
                        transformationType="",
                        transformationDescription="",
                    )
                    for column_lineage in parse_result.column_lineage
                }
            )

    def generate_openlineage_metadata_from_sql(
        self,
        sql: List[str] | str,
        database_info: DatabaseInfo,
        database: Optional[str] = None,
        sqlalchemy_engine: Optional[Any] = None,
    ) -> LineageInfo:
        """
        Parse SQL statement(s) and generate OpenLineage metadata.

        Generated OpenLineage metadata contains:

        * input tables with schemas parsed
        * output tables with schemas parsed
        * run facets
        * job facets.

        :param sql: a SQL statement or list of SQL statement to be parsed
        :param database_info: database specific information
        :param database: when passed it takes precedence over parsed database name
        :param sqlalchemy_engine: when passed, engine's dialect is used to compile SQL queries
        """
        job_facets: Dict[str, Any] = {"sql": sql_job.SQLJobFacet(query=self.normalize_sql(sql))}
        parse_result = self.parse(sql=self.split_sql_string(sql))
        if not parse_result:
            return LineageInfo(job_facets=job_facets)

        run_facets: Dict[str, Any] = {}
        if parse_result.errors:
            run_facets["extractionError"] = extraction_error_run.ExtractionErrorRunFacet(
                totalTasks=len(sql) if isinstance(sql, list) else 1,
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

        namespace = self.create_namespace(database_info=database_info)
        
        # Use parser-only metadata extraction (no database connection required)
        inputs, outputs = self.get_metadata_from_parser(
            inputs=parse_result.in_tables,
            outputs=parse_result.out_tables,
            namespace=namespace,
            database=database,
            database_info=database_info,
        )

        self.attach_column_lineage(outputs, database or database_info.database, parse_result)

        return LineageInfo(
            inputs=inputs,
            outputs=outputs,
            run_facets=run_facets,
            job_facets=job_facets,
        )

    @staticmethod
    def create_namespace(database_info: DatabaseInfo) -> str:
        return (
            f"{database_info.scheme}://{database_info.authority}"
            if database_info.authority
            else database_info.scheme
        )

    @classmethod
    def normalize_sql(cls, sql: List[str] | str) -> str:
        """Make sure to return a semicolon-separated SQL statement."""
        return ";\n".join(stmt.rstrip(" ;\r\n") for stmt in cls.split_sql_string(sql))

    @classmethod
    def split_sql_string(cls, sql: List[str] | str) -> List[str]:
        """
        Split SQL string into list of statements.

        Uses sqlparse to split SQL statements.
        """
        def split_statement(sql: str, strip_semicolon: bool = False) -> List[str]:
            splits = sqlparse.split(
                sql=sqlparse.format(sql, strip_comments=True),
                strip_semicolon=strip_semicolon,
            )
            return [s for s in splits if s]

        if isinstance(sql, str):
            return split_statement(sql)
        return [obj for stmt in sql for obj in cls.split_sql_string(stmt) if obj != ""]

    @staticmethod
    def _get_tables_hierarchy(
        tables: List[DbTableMeta],
        normalize_name: Callable[[str], str],
        database: Optional[str] = None,
        is_cross_db: bool = False,
    ) -> TablesHierarchy:
        """
        Create a hierarchy of database -> schema -> table name.

        This helps to create simpler information schema query grouped by
        database and schema.
        :param tables: List of tables.
        :param normalize_name: A method to normalize all names.
        :param is_cross_db: If false, set top (database) level to None
            when creating hierarchy.
        """
        hierarchy: TablesHierarchy = {}
        for table in tables:
            if is_cross_db:
                db = table.database or database
            else:
                db = None
            schemas = hierarchy.setdefault(normalize_name(db) if db else db, {})
            table_names = schemas.setdefault(normalize_name(table.schema) if table.schema else None, [])
            table_names.append(table.name)
        return hierarchy
