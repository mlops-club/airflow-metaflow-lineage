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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

from openlineage.client import OpenLineageClient
from openlineage.client.facet import ParentRunFacet
from openlineage.client.facet_v2 import processing_engine_run, source_code_job, source_code_location_job
from openlineage.client.run import Job, Run, RunEvent, RunState
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

        # Generate flow run ID only once for the entire flow (on start step)
        if step_name == "start":
            flow_run_id = str(generate_new_uuid())
            flow_job, flow_run = _create_flow_job_and_run(flow_name, flow_run_id)
            # Store flow job and run in singleton
            FLOW_LINEAGE_SINGLETON.flow_job = flow_job
            FLOW_LINEAGE_SINGLETON.flow_run = flow_run
            _emit_run_event(client, RunState.START, flow_job, flow_run)
        # Use existing flow run ID from singleton
        elif FLOW_LINEAGE_SINGLETON.flow_run:
            flow_run_id = FLOW_LINEAGE_SINGLETON.flow_run.runId
        else:
            # Fallback if start step was skipped (e.g., resume)
            flow_run_id = str(generate_new_uuid())
            flow_job, flow_run = _create_flow_job_and_run(flow_name, flow_run_id)
            FLOW_LINEAGE_SINGLETON.flow_job = flow_job
            FLOW_LINEAGE_SINGLETON.flow_run = flow_run

        step_run_id = str(generate_new_uuid())
        step_job, step_run = _create_step_job_and_run(flow_name, step_name, step_run_id, flow_run_id)

        # Store step job and run in singleton
        FLOW_LINEAGE_SINGLETON.steps[step_name] = StepContext(job=step_job, run=step_run)

        # Log source code and source code location facets
        _log_source_code_facet(func, step_job)
        # _log_source_code_location_facet(func, step_job)
        _emit_run_event(client, RunState.START, step_job, step_run)

        try:
            # Execute the original step function
            result = func(self, *args, **kwargs)

            _emit_run_event(client, RunState.COMPLETE, step_job, step_run)

            # Emit COMPLETE events for 'end' step (both flow and step level)
            if step_name == "end":
                if FLOW_LINEAGE_SINGLETON.flow_job and FLOW_LINEAGE_SINGLETON.flow_run:
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
    return OpenLineageClient()


def _create_processing_engine_facet(name: str | None, version: str) -> processing_engine_run.ProcessingEngineRunFacet:
    """Create a common processing engine facet for Metaflow runs."""
    return processing_engine_run.ProcessingEngineRunFacet(name=name, version=version)


def _create_step_job_and_run(flow_name: str, step_name: str, step_run_id: str, flow_run_id: str) -> tuple[Job, Run]:
    """Create OpenLineage Job and Run objects for step-level events."""
    job_name = f"{flow_name}.{step_name}"

    # Create parent facet for the step
    parent_facet = ParentRunFacet(
        run={"runId": flow_run_id},
        job={"namespace": DEFAULT_NAMESPACE, "name": flow_name}
    )

    job = Job(namespace=DEFAULT_NAMESPACE, name=job_name)
    run = Run(
        runId=step_run_id,
        facets={
            "parent": parent_facet,
            "processing_engine": _create_processing_engine_facet(name="python", version="3.11.4"),
        },
    )
    return job, run


def _create_flow_job_and_run(flow_name: str, run_id: str) -> tuple[Job, Run]:
    """Create OpenLineage Job and Run objects for flow-level events."""

    job = Job(namespace=DEFAULT_NAMESPACE, name=flow_name)
    run = Run(
        runId=run_id,
        facets={
            "processing_engine": _create_processing_engine_facet(name="python", version="3.11.4"),
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
        producer="https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/metaflow",
    )
    client.emit(event)
    print(f"Emitted OpenLineage {event_type} event for job '{job.name}'")


def _log_source_code_facet(func: Callable, job: Job) -> None:
    """Add source code facet to the job."""
    try:
        source_code = inspect.getsource(func)
        job.facets["sourceCode"] = source_code_job.SourceCodeJobFacet(
            language="python",
            sourceCode=source_code
        )
    except Exception as e:
        print(f"Could not extract source code for {func.__name__}: {e}")


def _log_source_code_location_facet(func: Callable, job: Job) -> None:
    """Add source code location facet to the job using proper OpenLineage client class."""
    try:
        # Get git information
        try:
            # Get git repository URL
            repo_url = subprocess.check_output(
                ["git", "config", "--get", "remote.origin.url"], 
                cwd=os.getcwd(), 
                text=True
            ).strip()
            
            # Get current commit hash
            commit_hash = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], 
                cwd=os.getcwd(), 
                text=True
            ).strip()
            
            # Get current branch
            try:
                branch = subprocess.check_output(
                    ["git", "rev-parse", "--abbrev-ref", "HEAD"], 
                    cwd=os.getcwd(), 
                    text=True
                ).strip()
            except subprocess.CalledProcessError:
                branch = None
            
            # Get file path relative to repo root
            try:
                file_path = inspect.getfile(func)
                repo_root = subprocess.check_output(
                    ["git", "rev-parse", "--show-toplevel"], 
                    cwd=os.getcwd(), 
                    text=True
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
                branch=branch
            )
            
        except subprocess.CalledProcessError:
            # Not in a git repository or git not available
            print(f"Could not extract git information for {func.__name__}: not in a git repository or git not available")
            
    except Exception as e:
        print(f"Could not extract source code location for {func.__name__}: {e}")
