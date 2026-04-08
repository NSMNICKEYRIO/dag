import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent, indent
from typing import Any, Dict, List, Optional, Tuple

import uvicorn
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Setup logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("dynamic-dag-service")

app = FastAPI(title="Dynamic Airflow DAG Service", version="9.0.0")

# Env vars and constants
DAGS_FOLDER = Path(os.getenv("AIRFLOW_DAGS_DIR", "./dag_configs"))
DAGS_FOLDER.mkdir(parents=True, exist_ok=True)

KAFKA_CONN_ID = os.getenv("KAFKA_CONN_ID", "genesis_kafka_conn")
KAFKA_RUN_TOPIC = os.getenv("KAFKA_RUN_TOPIC", "genesis.hub.run.events.v1")

SUPPORTED_EXECUTION_MODES = {"sync", "async_no_wait", "fire_and_forget"}
SUPPORTED_TRIGGER_TYPES = {"O", "M", "S"}
TRIGGER_TYPE_MAPPING = {"0": "O", "1": "M", "2": "S"}

DEFAULT_TASK_RETRIES = int(os.getenv("DEFAULT_TASK_RETRIES", "2"))
DEFAULT_RETRY_DELAY_SECONDS = int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", "60"))
DEFAULT_FINALIZE_MAX_WORKERS = int(os.getenv("DEFAULT_FINALIZE_MAX_WORKERS", "10"))
DEFAULT_STATUS_TIMEOUT_SECONDS = int(os.getenv("DEFAULT_STATUS_TIMEOUT_SECONDS", "15"))

EVENTS = {
    "run_started": "run.started.v1",
    "run_succeeded": "run.succeeded.v1",
    "run_failed": "run.failed.v1",
}


class Node(BaseModel):
    id: str = Field(min_length=1)
    engine: str = Field(min_length=1)
    name: str = Field(min_length=1)
    executor_order_id: int = Field(ge=1)
    executor_sequence_id: int = Field(ge=1)
    execution_mode: str = Field(default="sync")
    branch_on_status: bool = False
    on_success_node_ids: List[str] = Field(default_factory=list)
    on_failure_node_ids: List[str] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @field_validator("id", "engine", "name", mode="before")
    @classmethod
    def normalize_string_fields(cls, value: Any) -> str:
        if value is None:
            raise ValueError("Field cannot be null")
        clean_value = str(value).strip()
        if not clean_value:
            raise ValueError("Field cannot be empty or whitespace")
        return clean_value

    @field_validator("execution_mode", mode="before")
    @classmethod
    def validate_execution_mode(cls, value: Any) -> str:
        clean_value = str(value or "sync").strip().lower()
        if clean_value not in SUPPORTED_EXECUTION_MODES:
            raise ValueError(f"Unsupported execution_mode: {clean_value}")
        return clean_value

    @field_validator("on_success_node_ids", "on_failure_node_ids", mode="before")
    @classmethod
    def normalize_node_id_lists(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("Must be a list")
        normalized: List[str] = []
        for item in value:
            clean_value = str(item).strip()
            if not clean_value:
                raise ValueError("Node ids in branch lists cannot be empty")
            normalized.append(clean_value)
        return normalized

    @model_validator(mode="after")
    def validate_branch_config(self):
        if self.branch_on_status:
            if self.execution_mode != "sync":
                raise ValueError("branch_on_status is only supported for execution_mode='sync'")
            if not self.on_success_node_ids and not self.on_failure_node_ids:
                raise ValueError(
                    "branch_on_status=true requires on_success_node_ids and/or on_failure_node_ids"
                )
        else:
            if self.on_success_node_ids or self.on_failure_node_ids:
                raise ValueError(
                    "on_success_node_ids/on_failure_node_ids must be empty when branch_on_status=false"
                )
        return self


class BuildDagPayload(BaseModel):
    run_control_id: str = Field(min_length=1)
    trigger_type: Optional[str] = Field(default=None, alias="triggerType")
    schedule: Optional[str] = None
    nodes: List[Node] = Field(min_length=1)

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @field_validator("run_control_id", mode="before")
    @classmethod
    def normalize_run_control_id(cls, value: Any) -> str:
        if value is None:
            raise ValueError("run_control_id cannot be null")
        clean_value = str(value).strip()
        if not clean_value:
            raise ValueError("run_control_id cannot be empty or whitespace")
        return clean_value

    @field_validator("trigger_type", mode="before")
    @classmethod
    def normalize_trigger_type(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        clean_value = str(value).strip().upper()
        if not clean_value:
            return None
        clean_value = TRIGGER_TYPE_MAPPING.get(clean_value, clean_value)
        if clean_value not in SUPPORTED_TRIGGER_TYPES:
            raise ValueError(f"Unsupported triggerType: {clean_value}")
        return clean_value

    @field_validator("schedule", mode="before")
    @classmethod
    def normalize_schedule(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        clean_value = str(value).strip()
        return clean_value or None

    @field_validator("nodes")
    @classmethod
    def validate_nodes(cls, nodes: List[Node]) -> List[Node]:
        seen_ids = set()
        duplicate_ids = set()
        seen_order_sequence = set()
        duplicate_order_sequence = set()

        all_ids = {node.id for node in nodes}

        for node in nodes:
            if node.id in seen_ids:
                duplicate_ids.add(node.id)
            seen_ids.add(node.id)

            pair = (node.executor_order_id, node.executor_sequence_id)
            if pair in seen_order_sequence:
                duplicate_order_sequence.add(pair)
            seen_order_sequence.add(pair)

            for target_id in node.on_success_node_ids + node.on_failure_node_ids:
                if target_id not in all_ids:
                    raise ValueError(f"Branch target node id not found: {target_id}")
                if target_id == node.id:
                    raise ValueError(f"Node cannot branch to itself: {node.id}")

        if duplicate_ids:
            raise ValueError(f"Duplicate node ids found: {sorted(duplicate_ids)}")

        if duplicate_order_sequence:
            pairs = [f"(order={o}, seq={s})" for o, s in sorted(duplicate_order_sequence)]
            raise ValueError(f"Duplicate executor ordering found: {pairs}")

        return nodes


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors()},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Global exception caught at: %s", request.url.path)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Internal server error",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        },
    )


def sanitize_identifier(raw: str) -> str:
    value = (raw or "").strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "dag"


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
        tmp.write(content)
        tmp_name = tmp.name
    os.chmod(tmp_name, 0o644)
    Path(tmp_name).replace(path)


def validate_generated_python(code: str) -> None:
    compile(code, "<generated_dag>", "exec")


def build_node_map(nodes: List[Node]) -> Dict[str, Dict[str, Any]]:
    ordered_nodes = sorted(nodes, key=lambda n: (n.executor_order_id, n.executor_sequence_id, n.id))
    node_map: Dict[str, Dict[str, Any]] = {}
    for idx, node in enumerate(ordered_nodes, start=1):
        node_map[node.id] = {
            "task_id": f"task_{idx}_{sanitize_identifier(node.id)}",
            "id": node.id,
            "name": node.name,
            "engine": node.engine,
            "executor_order_id": node.executor_order_id,
            "executor_sequence_id": node.executor_sequence_id,
            "execution_mode": node.execution_mode,
            "branch_on_status": node.branch_on_status,
            "on_success_node_ids": list(node.on_success_node_ids),
            "on_failure_node_ids": list(node.on_failure_node_ids),
        }
    return node_map


def build_dependencies(nodes: List[Node]) -> List[Tuple[str, str]]:
    ordered_nodes = sorted(nodes, key=lambda n: (n.executor_order_id, n.executor_sequence_id, n.id))
    by_order: Dict[int, List[Node]] = {}
    for node in ordered_nodes:
        by_order.setdefault(node.executor_order_id, []).append(node)

    branch_source_ids = {node.id for node in ordered_nodes if node.branch_on_status}
    branch_target_ids = {
        target_id
        for node in ordered_nodes
        for target_id in (node.on_success_node_ids + node.on_failure_node_ids)
    }

    deps: List[Tuple[str, str]] = []
    orders = sorted(by_order.keys())

    for i, order in enumerate(orders):
        if i == 0:
            continue

        prev_order = orders[i - 1]
        current_nodes = by_order[order]
        prev_nodes = by_order[prev_order]

        for child in current_nodes:
            if child.id in branch_target_ids:
                continue

            # JOIN condition: only one node in current order
            if len(current_nodes) == 1:
                for parent in prev_nodes:
                    if parent.id not in branch_source_ids:
                        deps.append((parent.id, child.id))
            else:
                same_seq_parents = [
                    parent for parent in prev_nodes
                    if parent.executor_sequence_id == child.executor_sequence_id
                    and parent.id not in branch_source_ids
                ]

                for parent in same_seq_parents:
                    deps.append((parent.id, child.id))

    for node in ordered_nodes:
        if node.branch_on_status:
            for target_id in node.on_success_node_ids + node.on_failure_node_ids:
                deps.append((node.id, target_id))

    deduped: List[Tuple[str, str]] = []
    seen = set()
    for edge in deps:
        if edge not in seen:
            seen.add(edge)
            deduped.append(edge)
    return deduped


def generate_dag_code(
    dag_id: str,
    run_control_id: str,
    nodes: List[Node],
    schedule: Optional[str],
) -> str:
    ordered_nodes = sorted(nodes, key=lambda n: (n.executor_order_id, n.executor_sequence_id, n.id))
    node_map = build_node_map(ordered_nodes)
    dependency_pairs = build_dependencies(ordered_nodes)

    task_defs: List[str] = []
    deps: List[str] = []
    branch_task_defs: List[str] = []
    task_end_map: Dict[str, str] = {}
    all_task_ids: List[str] = []
    async_no_wait_task_ids: List[str] = []
    for idx, node in enumerate(ordered_nodes, start=1):
        task_id = node_map[node.id]["task_id"]
        all_task_ids.append(task_id)

        if node.execution_mode == "sync":
            execute_callable = "execute_sync_node"
        elif node.execution_mode == "async_no_wait":
            execute_callable = "execute_async_no_wait_node"
        else:
            execute_callable = "execute_fire_and_forget_node"

        if node.execution_mode == "async_no_wait":
            async_no_wait_task_ids.append(task_id)

        task_code = dedent(
            f"""
            {task_id} = PythonOperator(
                task_id=\"{task_id}\",
                python_callable={execute_callable},
                op_kwargs={{
                    \"task_key\": \"task{idx}\",
                    \"node_id\": \"{node.id}\",
                    \"node_name\": \"{node.name}\",
                    \"engine\": \"{node.engine}\",
                }},
                retries=DEFAULT_TASK_RETRIES,
                retry_delay=timedelta(seconds=DEFAULT_RETRY_DELAY_SECONDS),
            )
            """
        ).strip()
        task_defs.append(indent(task_code, "    "))

        terminal_task_ref = task_id

        if node.branch_on_status:
            branch_task_id = f"branch_{sanitize_identifier(node.id)}"
            success_targets = [node_map[target_id]["task_id"] for target_id in node.on_success_node_ids]
            failure_targets = [node_map[target_id]["task_id"] for target_id in node.on_failure_node_ids]
            branch_code = dedent(
                f"""
                {branch_task_id} = BranchPythonOperator(
                    task_id=\"{branch_task_id}\",
                    python_callable=decide_branch,
                    op_kwargs={{
                        \"source_task_id\": \"{terminal_task_ref}\",
                        \"success_task_ids\": {success_targets!r},
                        \"failure_task_ids\": {failure_targets!r},
                    }},
                    retries=DEFAULT_TASK_RETRIES,
                    retry_delay=timedelta(seconds=DEFAULT_RETRY_DELAY_SECONDS),
                )
                """
            ).strip()
            branch_task_defs.append(indent(branch_code, "    "))
            deps.append(f"{terminal_task_ref} >> {branch_task_id}")
            task_end_map[node.id] = branch_task_id
        else:
            task_end_map[node.id] = terminal_task_ref

    incoming_children = {child for _, child in dependency_pairs}
    root_nodes = [node for node in ordered_nodes if node.id not in incoming_children]

    for node in root_nodes:
        deps.append(f"run_started_event >> {node_map[node.id]['task_id']}")

    for parent_id, child_id in dependency_pairs:
        parent_ref = task_end_map[parent_id]
        child_ref = node_map[child_id]["task_id"]
        deps.append(f"{parent_ref} >> {child_ref}")

    leaf_node_ids = {node.id for node in ordered_nodes} - {parent_id for parent_id, _ in dependency_pairs}
    for node_id in sorted(leaf_node_ids):
        deps.append(f"{task_end_map[node_id]} >> finalize_results_task")

    deps.append("finalize_results_task >> run_final_event")

    joined_tasks = "\n\n".join(task_defs)
    joined_branch_defs = "\n\n".join(branch_task_defs)
    joined_deps = "\n    ".join(deps)
    schedule_str = repr(schedule) if schedule else "None"
    task_ids_literal = repr(all_task_ids)
    async_task_ids_literal = repr(async_no_wait_task_ids)

    code = f'''from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import json
import logging
import requests

from airflow.exceptions import AirflowException
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator, get_current_context
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger("airflow.task")

KAFKA_CONN_ID = {KAFKA_CONN_ID!r}
KAFKA_RUN_TOPIC = {KAFKA_RUN_TOPIC!r}
RUN_CONTROL_ID = {run_control_id!r}
FINAL_TASK_IDS = {task_ids_literal}
ASYNC_NO_WAIT_TASK_IDS = {async_task_ids_literal}
DEFAULT_TASK_RETRIES = {DEFAULT_TASK_RETRIES}
DEFAULT_RETRY_DELAY_SECONDS = {DEFAULT_RETRY_DELAY_SECONDS}
DEFAULT_FINALIZE_MAX_WORKERS = {DEFAULT_FINALIZE_MAX_WORKERS}
DEFAULT_STATUS_TIMEOUT_SECONDS = {DEFAULT_STATUS_TIMEOUT_SECONDS}

HTTP_SUCCESS_CODES = (200, 201, 202, 204)
SUCCESS_STATES = ("SUCCEEDED", "SUCCESS", "COMPLETED", "DONE")
FAILURE_STATES = ("FAILED", "ERROR", "ABORTED")


def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_json(value):
    return json.dumps(value, default=str)


def _get_conf(context):
    dag_run = context["dag_run"]
    return dag_run.conf or {{}}


def _get_correlation_id(context):
    conf = _get_conf(context)
    return conf.get("correlation_id") or context["dag_run"].run_id


def _build_safe_trigger_payload(conf):
    task_keys = sorted([key for key in conf.keys() if str(key).startswith("task")])
    return {{
        "correlation_id": conf.get("correlation_id"),
        "task_keys": task_keys,
        "task_count": len(task_keys),
    }}


def _request_payload(task_key, context):
    payload = _get_conf(context).get(task_key, {{}})
    if not payload or not payload.get("url"):
        raise AirflowException(f"Missing URL for {{task_key}}")
    return payload


def _submit_request(task_key, **context):
    payload = _request_payload(task_key, context)
    request_timeout = int(payload.get("timeout", 300))
    verify_ssl = payload.get("verify_ssl", False)

    response = requests.request(
        method=payload.get("method", "POST"),
        url=payload["url"],
        json=payload.get("json"),
        headers=payload.get("headers", {{}}),
        timeout=request_timeout,
        verify=verify_ssl,
    )
    if response.status_code not in HTTP_SUCCESS_CODES:
        raise AirflowException(f"HTTP {{response.status_code}}")
    return response


def execute_sync_node(task_key, node_id, node_name, engine, **context):
    ti = context["ti"]
    try:
        response = _submit_request(task_key, **context)
        ti.xcom_push(key="task_status", value="SUCCEEDED")
        if response.text:
            try:
                return response.json()
            except Exception:
                return {{"raw_response": response.text}}
        return {{}}
    except Exception as exc:
        ti.xcom_push(key="task_status", value="FAILED")
        raise AirflowException(str(exc))


def execute_async_no_wait_node(task_key, node_id, node_name, engine, **context):
    ti = context["ti"]
    try:
        response = _submit_request(task_key, **context)
        response_payload = response.json() if response.text else {{}}
        payload = _request_payload(task_key, context)
        async_meta = {{
            "node_id": node_id,
            "node_name": node_name,
            "job_id": response_payload.get("job_id") or response_payload.get("id"),
            "status_url": response_payload.get("status_url") or payload.get("status_url"),
            "submitted_at": _utc_now(),
        }}
        ti.xcom_push(key="task_status", value="SUBMITTED")
        ti.xcom_push(key="async_meta", value=async_meta)
        return response_payload if response_payload else async_meta
    except Exception as exc:
        ti.xcom_push(key="task_status", value="FAILED")
        raise AirflowException(str(exc))


def execute_fire_and_forget_node(task_key, node_id, node_name, engine, **context):
    ti = context["ti"]
    try:
        response = _submit_request(task_key, **context)
        ti.xcom_push(key="task_status", value="SUCCEEDED")
        if response.text:
            try:
                return response.json()
            except Exception:
                return {{"raw_response": response.text}}
        return {{}}
    except Exception as exc:
        ti.xcom_push(key="task_status", value="FAILED")
        raise AirflowException(str(exc))


def decide_branch(source_task_id, success_task_ids, failure_task_ids, **context):
    ti = context["ti"]
    status = ti.xcom_pull(task_ids=source_task_id, key="task_status") or "FAILED"
    if status == "SUCCEEDED":
        if success_task_ids:
            return success_task_ids
        raise AirflowException("No success branch target configured")
    if failure_task_ids:
        return failure_task_ids
    raise AirflowException("No failure branch target configured")


def _poll_async_node(async_meta):
    status_url = async_meta.get("status_url")
    if not status_url:
        return {{
            "node_id": async_meta.get("node_id"),
            "status": "FAILED",
            "detail": "Missing status_url",
        }}

    try:
        response = requests.get(
            status_url,
            timeout=DEFAULT_STATUS_TIMEOUT_SECONDS,
            verify=False,
        )
        response.raise_for_status()
        payload = response.json() if response.text else {{}}
        remote_status = str(payload.get("status", "")).upper()
        if remote_status in SUCCESS_STATES:
            return {{"node_id": async_meta.get("node_id"), "status": "SUCCEEDED", "payload": payload}}
        if remote_status in FAILURE_STATES:
            return {{"node_id": async_meta.get("node_id"), "status": "FAILED", "payload": payload}}
        return {{
            "node_id": async_meta.get("node_id"),
            "status": "FAILED",
            "payload": payload,
            "detail": f"Unexpected remote status: {{remote_status or 'UNKNOWN'}}",
        }}
    except Exception as exc:
        return {{
            "node_id": async_meta.get("node_id"),
            "status": "FAILED",
            "detail": f"Polling failed: {{exc}}",
        }}


def finalize_results(task_ids, async_task_ids, **context):
    ti = context["ti"]
    failed, success = [], []
    async_poll_success, async_poll_failed = [], []

    async_meta_by_node = {{}}
    for task_id in task_ids:
        execute_task_id = f"{{gid}}.execute"
        status = ti.xcom_pull(task_ids=execute_task_id, key="task_status") or "FAILED"
        if status == "FAILED":
            failed.append(task_id)
        else:
            success.append(task_id)

        async_meta = ti.xcom_pull(task_ids=task_id, key="async_meta")
        if async_meta and async_meta.get("node_id"):
            async_meta_by_task[task_id] = async_meta

    async_metas = []
    for task_id in async_task_ids:
        async_meta = async_meta_by_task.get(task_id)
        if async_meta:
            async_metas.append(async_meta)
        else:
            async_poll_failed.append({{"node_id": node_id, "detail": "Missing async metadata"}})

    if async_metas:
        max_workers = max(1, min(DEFAULT_FINALIZE_MAX_WORKERS, len(async_metas)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_poll_async_node, meta) for meta in async_metas]
            for future in as_completed(futures):
                result = future.result()
                if result["status"] == "SUCCEEDED":
                    async_poll_success.append(result["node_id"])
                else:
                    async_poll_failed.append(result)

    final_status = "SUCCEEDED" if not failed and not async_poll_failed else "FAILED"
    summary = {{
        "successful_tasks": success,
        "failed_tasks": failed,
        "async_poll_success": async_poll_success,
        "async_poll_failed": async_poll_failed,
    }}

    ti.xcom_push(key="final_status", value=final_status)
    ti.xcom_push(key="final_summary", value=summary)
    return {{"final_status": final_status, **summary}}


def build_run_started_event_messages():
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    conf = _get_conf(ctx)

    payload = {{
        "eventType": {EVENTS['run_started']!r},
        "run_control_id": conf.get("run_control_id") or RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "event_source": "AIRFLOW",
        "status": "RUNNING",
        "trigger_payload": _safe_json(_build_safe_trigger_payload(conf)),
        "dagId": ctx["dag"].dag_id,
        "dagRunId": dag_run.run_id,
        "timestamp": _utc_now(),
    }}

    yield (dag_run.run_id, _safe_json(payload).encode())


def build_run_final_event_messages():
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    ti = ctx["ti"]
    conf = _get_conf(ctx)

    status = ti.xcom_pull(task_ids="finalize_results", key="final_status") or "FAILED"
    summary = ti.xcom_pull(task_ids="finalize_results", key="final_summary") or {{}}

    payload = {{
        "eventType": {EVENTS['run_succeeded']!r} if status == "SUCCEEDED" else {EVENTS['run_failed']!r},
        "run_control_id": conf.get("run_control_id") or RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "event_source": "AIRFLOW",
        "status": status,
        "trigger_payload": _safe_json(_build_safe_trigger_payload(conf)),
        "dagId": ctx["dag"].dag_id,
        "dagRunId": dag_run.run_id,
        "timestamp": _utc_now(),
        "summary": summary,
    }}

    yield (dag_run.run_id, _safe_json(payload).encode())


with DAG(
    dag_id={dag_id!r},
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule={schedule_str},
    catchup=False,
    tags=["dynamic", "generated"],
) as dag:
    prepare_inputs_task = PythonOperator(
        task_id="prepare_inputs",
        python_callable=lambda: True,
        retries=DEFAULT_TASK_RETRIES,
        retry_delay=timedelta(seconds=DEFAULT_RETRY_DELAY_SECONDS),
    )

    run_started_event = ProduceToTopicOperator(
        task_id="run_started_event",
        kafka_config_id=KAFKA_CONN_ID,
        topic=KAFKA_RUN_TOPIC,
        producer_function=build_run_started_event_messages,
        retries=DEFAULT_TASK_RETRIES,
        retry_delay=timedelta(seconds=DEFAULT_RETRY_DELAY_SECONDS),
    )

{joined_tasks}

{joined_branch_defs}

    finalize_results_task = PythonOperator(
        task_id="finalize_results",
        python_callable=finalize_results,
        op_kwargs={{"task_ids": FINAL_TASK_IDS, "async_task_ids": ASYNC_NO_WAIT_TASK_IDS}},
        trigger_rule=TriggerRule.ALL_DONE,
        retries=DEFAULT_TASK_RETRIES,
        retry_delay=timedelta(seconds=DEFAULT_RETRY_DELAY_SECONDS),
    )

    run_final_event = ProduceToTopicOperator(
        task_id="run_final_event",
        kafka_config_id=KAFKA_CONN_ID,
        topic=KAFKA_RUN_TOPIC,
        producer_function=build_run_final_event_messages,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=DEFAULT_TASK_RETRIES,
        retry_delay=timedelta(seconds=DEFAULT_RETRY_DELAY_SECONDS),
    )

    prepare_inputs_task >> run_started_event
    {joined_deps}
'''
    return code


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "UP", "version": app.version}


@app.post("/build_dag", status_code=status.HTTP_201_CREATED)
def build_dag(payload: BuildDagPayload) -> Dict[str, str]:
    dag_id = f"{sanitize_identifier(payload.run_control_id)}_dag"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dag_file_name = f"{dag_id}_{timestamp}.py"
    dag_file_path = DAGS_FOLDER / dag_file_name

    code = generate_dag_code(
        dag_id=dag_id,
        run_control_id=payload.run_control_id,
        nodes=payload.nodes,
        schedule=payload.schedule,
    )

    validate_generated_python(code)
    atomic_write_text(dag_file_path, code)

    logger.info("DAG generated: %s", dag_id)

    return {
        "status": "SUCCESS",
        "dag_id": dag_id,
        "file": dag_file_name,
        "path": str(dag_file_path.resolve()),
    }


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8443"))
    uvicorn.run(app, host=host, port=port, log_level=LOG_LEVEL.lower())
