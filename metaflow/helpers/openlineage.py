"""
Note: if Airflow uses OpenLineage, and Airflow dags trigger
Metaflow flows, then we will need Airflow to pass
it's Job/Run ID to the Metaflow flow so we can set it as the root.

Note: if we use the 'resume' command, then start gets skipped.
But with this code, the flow-level job is created in the start
step... so how do we account for this?
"""

import functools
from datetime import datetime, timezone
from typing import Callable
from metaflow import current
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job
from openlineage.client.uuid import generate_new_uuid
from openlineage.client.facet import ParentRunFacet
from typing import TypedDict
from dataclasses import dataclass, field


SCHEDULER_NAMESPACE = "metaflow"


@dataclass
class StepContext:
    job: Job
    run: Run


@dataclass
class FlowLineage:
    """Container for OpenLineage baggage accumulated across steps of a flow."""

    flow_job: Job | None = None
    flow_run: Run | None = None
    steps: dict[str, StepContext] = field(default_factory=dict)


FLOW_LINEAGE_SINGLETON = FlowLineage()


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
        else:
            # Use existing flow run ID from singleton
            if FLOW_LINEAGE_SINGLETON.flow_run:
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


def _create_step_job_and_run(flow_name: str, step_name: str, step_run_id: str, flow_run_id: str) -> tuple[Job, Run]:
    """Create OpenLineage Job and Run objects for step-level events."""
    job_name = f"{flow_name}.{step_name}"
    namespace = flow_name

    parent_run_facet = ParentRunFacet(
        run={"runId": flow_run_id}, job={"namespace": SCHEDULER_NAMESPACE, "name": flow_name}
    )

    job = Job(namespace=namespace, name=job_name)
    run = Run(runId=step_run_id, facets={"parent": parent_run_facet})

    return job, run


def _create_flow_job_and_run(flow_name: str, run_id: str) -> tuple[Job, Run]:
    """Create OpenLineage Job and Run objects for flow-level events."""
    job = Job(namespace=SCHEDULER_NAMESPACE, name=flow_name)
    run = Run(runId=run_id)
    return job, run


def _emit_run_event(client: OpenLineageClient, event_type: RunState, job: Job, run: Run) -> None:
    """Emit an OpenLineage run event."""
    event = RunEvent(
        eventType=event_type,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=run,
        job=job,
        producer="metaflow-openlineage-extension",
    )
    client.emit(event)
