"""
Note: if Airflow uses OpenLineage, and Airflow dags trigger
Metaflow flows, then we will need Airflow to pass
it's Job/Run ID to the Metaflow flow so we can set it as the root.

Note: if we use the 'resume' command, then start gets skipped.
But with this code, the flow-level job is created in the start
step... so how do we account for this?
"""

import functools
import inspect
import os
import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

import pandas as pd
from openlineage.client import OpenLineageClient
from openlineage.client.facet import ParentRunFacet
from openlineage.client.facet_v2 import processing_engine_run, source_code_job, source_code_location_job
from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.uuid import generate_new_uuid

from metaflow import current

# Use "default" namespace or environment variable
DEFAULT_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "default")


@dataclass
class StepContext:
    job: Job
    run: Run


@dataclass
class FlowLineage:
    """Container for OpenLineage baggage accumulated across steps of a flow."""

    flow_job: Optional[Job] = None
    flow_run: Optional[Run] = None
    steps: Dict[str, StepContext] = field(default_factory=dict)

    def get_step_context(self, step_name: str) -> Optional[StepContext]:
        """Get the context for a step by its name."""
        return self.steps.get(step_name)


FLOW_LINEAGE_SINGLETON = FlowLineage()


def get_current_step_context() -> Optional[StepContext]:
    """Get the current step context for SQL queries to use."""
    try:
        step_name = current.step_name
        if step_name:
            return FLOW_LINEAGE_SINGLETON.get_step_context(step_name)
        return None
    except Exception:
        return None


def openlineage(func: Callable) -> Callable:
    """
    Decorator for Metaflow steps that emits OpenLineage events.

    For 'start' step: Emits START event for both the whole flow and the start step

    For 'end' step: Emits COMPLETE event for both the whole flow and the end step

    For any step failure: Emits FAIL event for the whole flow

    Job name format: <FlowName> for flow-level, <FlowName>.<step_name> for step-level
    Namespace: metaflow
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        client = _create_openlineage_client()

        flow_name = self.__class__.__name__
        step_name = func.__name__

        # Use Metaflow's current.run_id as the flow run ID for consistency across all steps
        # Convert Metaflow's run_id (which is a number) to a deterministic UUID
        flow_run_id = str(uuid.uuid5(uuid.NAMESPACE_OID, current.run_id))

        # Create flow job and run if not already created
        if not FLOW_LINEAGE_SINGLETON.flow_job or not FLOW_LINEAGE_SINGLETON.flow_run:
            flow_job, flow_run = _create_flow_job_and_run(flow_name, flow_run_id)

            # Store flow job and run in singleton
            FLOW_LINEAGE_SINGLETON.flow_job = flow_job
            FLOW_LINEAGE_SINGLETON.flow_run = flow_run

            # Emit START event for the flow at the start step
            if step_name == "start":
                _emit_run_event(client, RunState.START, flow_job, flow_run)

        step_run_id = str(generate_new_uuid())
        step_job, step_run = _create_step_job_and_run(flow_name, step_name, step_run_id, flow_run_id)

        # Store step job and run in singleton
        FLOW_LINEAGE_SINGLETON.steps[step_name] = StepContext(job=step_job, run=step_run)

        # Log source code and source code location facets
        _log_source_code_facet(func, step_job)
        _log_source_code_location_facet(func, step_job)

        try:
            _emit_run_event(client, RunState.START, step_job, step_run)

            # Execute the original step function
            result = func(self, *args, **kwargs)

            _emit_run_event(client, RunState.COMPLETE, step_job, step_run)

            # Emit COMPLETE events for 'end' step (both flow and step level)
            if step_name == "end":
                # if FLOW_LINEAGE_SINGLETON.flow_job and FLOW_LINEAGE_SINGLETON.flow_run:
                _emit_run_event(
                    client, RunState.COMPLETE, FLOW_LINEAGE_SINGLETON.flow_job, FLOW_LINEAGE_SINGLETON.flow_run
                )

            return result
        except Exception:
            # Emit FAIL event for the whole flow on any step failure
            if FLOW_LINEAGE_SINGLETON.flow_job and FLOW_LINEAGE_SINGLETON.flow_run:
                _emit_run_event(
                    client, RunState.FAIL, FLOW_LINEAGE_SINGLETON.flow_job, FLOW_LINEAGE_SINGLETON.flow_run
                )
            _emit_run_event(client, RunState.FAIL, step_job, step_run)
            raise

    return wrapper


def _create_openlineage_client() -> OpenLineageClient:
    """Initialize OpenLineage client from environment variables."""
    return OpenLineageClient().from_environment()


def _create_processing_engine_facet(name: str | None, version: str) -> processing_engine_run.ProcessingEngineRunFacet:
    """Create a common processing engine facet for Metaflow runs."""
    return processing_engine_run.ProcessingEngineRunFacet(name=name, version=version)


def _create_step_job_and_run(flow_name: str, step_name: str, step_run_id: str, flow_run_id: str) -> tuple[Job, Run]:
    """Create OpenLineage Job and Run objects for step-level events."""
    job_name = f"{flow_name}.{step_name}"

    # Create parent facet for the step
    parent_facet = ParentRunFacet(
        run={"runId": flow_run_id},
        job={"namespace": DEFAULT_NAMESPACE, "name": flow_name},
    )

    job = Job(namespace=DEFAULT_NAMESPACE, name=job_name)
    run = Run(
        runId=step_run_id,
        facets={
            "parent": parent_facet,
            "processing_engine": _create_processing_engine_facet(name="sagemaker", version="1.0.0"),
        },
    )
    return job, run


def _create_flow_job_and_run(flow_name: str, run_id: str) -> tuple[Job, Run]:
    """Create OpenLineage Job and Run objects for flow-level events."""

    job = Job(namespace=DEFAULT_NAMESPACE, name=flow_name)
    run = Run(
        runId=run_id,
        facets={
            "processing_engine": _create_processing_engine_facet(name="sagemaker", version="1.0.0"),
        },
    )
    return job, run


def _emit_run_event(client: OpenLineageClient, event_type: RunState, job: Job, run: Run) -> None:
    """Emit an OpenLineage run event."""
    event = RunEvent(
        eventType=event_type,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=run,
        job=job,
        producer="https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/sagemaker",
    )

    # if os.environ.get("OPENLINEAGE_DEBUG"):
    #     from openlineage.client.serde import Serde
    #     from rich import print_json

    #     print_json(Serde.to_json(event))

    client.emit(event)
    print(f"Emitted OpenLineage {event_type} event for job '{job.name}'")


def _log_source_code_facet(func: Callable, job: Job) -> None:
    """Add source code facet to the job."""
    try:
        source_code = inspect.getsource(func)
        job.facets["sourceCode"] = source_code_job.SourceCodeJobFacet(language="python", sourceCode=source_code)
    except Exception as e:
        print(f"Could not extract source code for {func.__name__}: {e}")


def _log_source_code_location_facet(func: Callable, job: Job) -> None:
    """Add source code location facet to the job using proper OpenLineage client class."""
    try:
        # Get git information
        try:
            # Get git repository URL
            repo_url = subprocess.check_output(
                ["git", "config", "--get", "remote.origin.url"], cwd=os.getcwd(), text=True
            ).strip()

            # Get current commit hash
            commit_hash = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=os.getcwd(), text=True).strip()

            # Get current branch
            try:
                branch = subprocess.check_output(
                    ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=os.getcwd(), text=True
                ).strip()
            except subprocess.CalledProcessError:
                branch = None

            # Get file path relative to repo root
            try:
                file_path = inspect.getfile(func)
                repo_root = subprocess.check_output(
                    ["git", "rev-parse", "--show-toplevel"], cwd=os.getcwd(), text=True
                ).strip()
                relative_path = os.path.relpath(file_path, repo_root)
            except (OSError, subprocess.CalledProcessError):
                relative_path = None

            # Create source code location facet
            job.facets["sourceCodeLocation"] = source_code_location_job.SourceCodeLocationJobFacet(
                type="git",
                url=f"{repo_url}/blob/{commit_hash}/{relative_path}" if relative_path else repo_url,
                repoUrl=repo_url,
                path=relative_path,
                version=commit_hash,
                branch=branch,
            )

        except subprocess.CalledProcessError:
            # Not in a git repository or git not available
            print(
                f"Could not extract git information for {func.__name__}: not in a git repository or git not available"
            )

    except Exception as e:
        print(f"Could not extract source code location for {func.__name__}: {e}")


def emit_model_lineage_event(
    inputs: List[Dict],
    outputs: List[Dict],
    job_name: str,
    run_id: Optional[str] = None,
) -> None:
    """
    Emit OpenLineage event for model training/prediction operations.

    Args:
        inputs: List of input dataset specifications
        outputs: List of output dataset specifications
        job_name: Name for the job
        run_id: Optional run ID, will generate if not provided
    """
    client = _create_openlineage_client()

    if not run_id:
        run_id = str(generate_new_uuid())

    # Get current step context for parent information
    step_context = get_current_step_context()  # Create job and run
    job = Job(namespace=DEFAULT_NAMESPACE, name=job_name)

    run_facets: Dict = {"processing_engine": _create_processing_engine_facet(name="sagemaker", version="1.0.0")}

    # Add parent facet if we have step context
    if step_context:
        parent_facet = ParentRunFacet(
            run={"runId": step_context.run.runId},
            job={"namespace": step_context.job.namespace, "name": step_context.job.name},
        )
        run_facets["parent"] = parent_facet

    run = Run(runId=run_id, facets=run_facets)

    # Convert input and output specifications to Dataset objects
    input_datasets = []
    for input_spec in inputs:
        input_datasets.append(
            Dataset(
                namespace=input_spec.get("namespace", DEFAULT_NAMESPACE),
                name=input_spec["name"],
                facets=input_spec.get("facets", {}),
            )
        )

    output_datasets = []
    for output_spec in outputs:
        output_datasets.append(
            Dataset(
                namespace=output_spec.get("namespace", DEFAULT_NAMESPACE),
                name=output_spec["name"],
                facets=output_spec.get("facets", {}),
            )
        )

    # Create and emit the event
    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=run,
        job=job,
        inputs=input_datasets if input_datasets else None,
        outputs=output_datasets if output_datasets else None,
        producer="https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/sagemaker",
    )

    from pathlib import Path

    from openlineage.client.serde import Serde

    Path("openlineage-events").mkdir(parents=True, exist_ok=True)
    with open(f"openlineage-events/{job_name}_{run_id}.json", "w") as f:
        f.write(Serde.to_json(event))

    client.emit(event)
    print(f"Emitted model lineage event for '{job_name}' with {len(inputs)} inputs and {len(outputs)} outputs")


def create_dataframe_facets(df: pd.DataFrame, metadata: Optional[Dict] = None) -> Dict:
    """
    Create facets for a pandas DataFrame.

    Args:
        df: The pandas DataFrame
        metadata: Optional additional metadata

    Returns:
        Dictionary of facets describing the DataFrame
    """
    facets = {
        "dataFrame": {
            "shape": list(df.shape),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            "index_name": df.index.name,
            "is_empty": df.empty,
        }
    }

    # Add data quality information
    if not df.empty:
        facets["dataQuality"] = {
            "total_rows": len(df),
            "missing_values": df.isnull().sum().sum(),
            "duplicate_rows": df.duplicated().sum(),
            "completeness": round(1 - (df.isnull().sum().sum() / (len(df) * len(df.columns))), 3),
        }

    # Add temporal information if datetime columns exist
    datetime_cols = df.select_dtypes(include=["datetime64"]).columns
    if not datetime_cols.empty:
        for col in datetime_cols:
            facets["temporalCoverage"] = {
                "column": col,
                "start": df[col].min().isoformat() if pd.notna(df[col].min()) else None,
                "end": df[col].max().isoformat() if pd.notna(df[col].max()) else None,
                "span_days": (df[col].max() - df[col].min()).days
                if pd.notna(df[col].min()) and pd.notna(df[col].max())
                else None,
            }
            break  # Just use the first datetime column

    # Add any additional metadata
    if metadata:
        facets.update(metadata)

    return facets


def create_version_facet(version: str, version_type: str = "metaflow_run_id") -> Dict:
    """
    Create a version facet for OpenLineage datasets.

    Args:
        version: The version identifier (e.g., Metaflow run_id)
        version_type: Type of versioning system

    Returns:
        Dictionary containing version facet
    """
    return {
        "version": {
            "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/sagemaker",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/VersionDatasetFacet.json",
            "datasetVersion": version,
            "versionType": version_type,
        }
    }


def create_datasource_facet(name: str, uri: str) -> Dict:
    """
    Create a dataSource facet following OpenLineage specification.

    Args:
        name: The name of the data source
        uri: The URI of the data source

    Returns:
        Dictionary containing dataSource facet
    """
    return {
        "dataSource": {
            "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/sagemaker",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
            "name": name,
            "uri": uri,
        }
    }
