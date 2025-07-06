from typing import Any, Dict

from openlineage.client.facet_v2 import parent_run, processing_engine_run
from openlineage.client.utils import RedactMixin

# Constants
OPENLINEAGE_PROVIDER_VERSION = "1.0.0"
DEFAULT_NAMESPACE = "default"


def get_task_parent_run_facet(
    parent_run_id: str, parent_job_name: str, parent_job_namespace: str = DEFAULT_NAMESPACE
) -> Dict[str, Any]:
    """
    Retrieve the parent run facet for task-level events.

    This facet currently always points to the DAG-level run ID and name,
    as external events for DAG runs are not yet handled.
    """
    return {
        "parent": parent_run.ParentRunFacet(
            run=parent_run.Run(runId=parent_run_id),
            job=parent_run.Job(namespace=parent_job_namespace, name=parent_job_name),
            root=parent_run.Root(
                run=parent_run.RootRun(runId=parent_run_id),
                job=parent_run.RootJob(namespace=parent_job_namespace, name=parent_job_name),
            ),
        )
    }


def get_processing_engine_facet() -> dict[str, processing_engine_run.ProcessingEngineRunFacet]:
    from openlineage.client.facet_v2 import processing_engine_run

    return {
        "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
            version="1.0.0",  # AIRFLOW_VERSION,
            name="sagemaker",
            openlineageAdapterVersion=OPENLINEAGE_PROVIDER_VERSION,
        )
    }


# def get_airflow_debug_facet() -> dict[str, AirflowDebugRunFacet]:
#     if not conf.debug_mode():
#         return {}
#     log.warning("OpenLineage debug_mode is enabled. Be aware that this may log and emit extensive details.")
#     return {
#         "debug": AirflowDebugRunFacet(
#             packages=_get_all_packages_installed(),
#         )
#     }

# def get_airflow_run_facet(
#     dag_run: DagRun,
#     dag: DAG,
#     task_instance: TaskInstance,
#     task: BaseOperator,
#     task_uuid: str,
# ) -> dict[str, AirflowRunFacet]:
#     return {
#         "airflow": AirflowRunFacet(
#             dag=DagInfo(dag),
#             dagRun=DagRunInfo(dag_run),
#             taskInstance=TaskInstanceInfo(task_instance),
#             task=TaskInfoComplete(task) if conf.include_full_task_info() else TaskInfo(task),
#             taskUuid=task_uuid,
#         )
#     }
