import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent
from typing import Any, Dict, List, Optional, Set, Tuple

import uvicorn
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("dynamic-dag-service")

app = FastAPI(title="Dynamic Airflow DAG Service", version="8.1.0")

DAGS_FOLDER = Path(os.getenv("AIRFLOW_DAGS_DIR", "./dag_configs"))
DAGS_FOLDER.mkdir(parents=True, exist_ok=True)

KAFKA_CONN_ID = os.getenv("KAFKA_CONN_ID", "genesis_kafka_conn")
KAFKA_RUN_TOPIC = os.getenv("KAFKA_RUN_TOPIC", "genesis.hub.run.events.v1")

SUPPORTED_EXECUTION_MODES = {"sync", "async_no_wait"}
SUPPORTED_TRIGGER_TYPES = {"O", "M", "S"}
TRIGGER_TYPE_MAPPING = {"0": "O", "1": "M", "2": "S"}

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
        if value in (None, ""):
            return []
        if not isinstance(value, list):
            raise ValueError("Must be a list")
        cleaned: List[str] = []
        seen: Set[str] = set()
        for item in value:
            item_str = str(item).strip()
            if not item_str:
                continue
            if item_str not in seen:
                cleaned.append(item_str)
                seen.add(item_str)
        return cleaned


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
    def validate_unique_ids_and_positions(cls, nodes: List[Node]) -> List[Node]:
        seen_ids: Set[str] = set()
        duplicate_ids: Set[str] = set()
        seen_sequence_pairs: Set[Tuple[int, int]] = set()
        duplicate_sequence_pairs: Set[Tuple[int, int]] = set()

        for node in nodes:
            if node.id in seen_ids:
                duplicate_ids.add(node.id)
            seen_ids.add(node.id)

            seq_pair = (node.executor_order_id, node.executor_sequence_id)
            if seq_pair in seen_sequence_pairs:
                duplicate_sequence_pairs.add(seq_pair)
            seen_sequence_pairs.add(seq_pair)

        if duplicate_ids:
            raise ValueError(f"Duplicate node ids found: {sorted(duplicate_ids)}")

        if duplicate_sequence_pairs:
            pairs = [f"(order={o}, seq={s})" for o, s in sorted(duplicate_sequence_pairs)]
            raise ValueError(f"Duplicate executor ordering found: {pairs}")

        return nodes

    @model_validator(mode="after")
    def validate_branch_references(self) -> "BuildDagPayload":
        node_ids = {node.id for node in self.nodes}
        errors: List[str] = []

        for node in self.nodes:
            referenced_ids = node.on_success_node_ids + node.on_failure_node_ids
            missing_ids = sorted({ref for ref in referenced_ids if ref not in node_ids})
            if missing_ids:
                errors.append(f"Node {node.id} references unknown node ids: {missing_ids}")

            if node.branch_on_status and not (node.on_success_node_ids or node.on_failure_node_ids):
                errors.append(f"Node {node.id} has branch_on_status=true but no branch targets")

        if errors:
            raise ValueError("; ".join(errors))

        return self


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


def build_layers(nodes: List[Node]) -> List[Tuple[int, List[Dict[str, Any]]]]:
    layers: Dict[int, List[Dict[str, Any]]] = {}
    for node in nodes:
        layers.setdefault(node.executor_order_id, []).append(node.model_dump())
    return [
        (order_id, sorted(items, key=lambda item: item["executor_sequence_id"]))
        for order_id, items in sorted(layers.items(), key=lambda item: item[0])
    ]


def build_stage_dependencies(sorted_layers: List[Tuple[int, List[Dict[str, Any]]]]) -> List[Tuple[str, str]]:
    deps: List[Tuple[str, str]] = []
    branch_source_ids = {
        node["id"]
        for _, nodes in sorted_layers
        for node in nodes
        if node.get("branch_on_status")
    }

    for index, (_, current_nodes) in enumerate(sorted_layers):
        if index == 0:
            for child in current_nodes:
                deps.append(("run_started_event", child["id"]))
            continue

        _, prev_nodes = sorted_layers[index - 1]

        if len(current_nodes) == 1:
            child = current_nodes[0]
            for parent in prev_nodes:
                if parent["id"] not in branch_source_ids:
                    deps.append((parent["id"], child["id"]))
        else:
            for child in current_nodes:
                same_seq_parents = [
                    parent for parent in prev_nodes
                    if parent["executor_sequence_id"] == child["executor_sequence_id"]
                    and parent["id"] not in branch_source_ids
                ]
                for parent in same_seq_parents:
                    deps.append((parent["id"], child["id"]))

    return deps


def validate_generated_python(code: str) -> None:
    compile(code, "<generated_dag>", "exec")


def generate_dag_code(
    dag_id: str,
    run_control_id: str,
    sorted_layers: List[Tuple[int, List[Dict[str, Any]]]],
    schedule: Optional[str],
) -> str:
    all_nodes = [node for _, layer_nodes in sorted_layers for node in layer_nodes]
    node_ids = [node["id"] for node in all_nodes]
    async_node_ids = [node["id"] for node in all_nodes if node["execution_mode"] == "async_no_wait"]
    node_name_map = {node["id"]: node["name"] for node in all_nodes}
    branch_task_ids = {
        node["id"]: f"branch_{node['id']}"
        for node in all_nodes
        if node.get("branch_on_status")
    }

    deps = build_stage_dependencies(sorted_layers)

    for node in all_nodes:
        if node.get("branch_on_status"):
            deps.append((node["id"], branch_task_ids[node["id"]]))
            for target in node.get("on_success_node_ids", []):
                deps.append((branch_task_ids[node["id"]], target))
            for target in node.get("on_failure_node_ids", []):
                deps.append((branch_task_ids[node["id"]], target))

    deduped_deps: List[Tuple[str, str]] = []
    seen_deps: Set[Tuple[str, str]] = set()
    for dep in deps:
        if dep not in seen_deps:
            deduped_deps.append(dep)
            seen_deps.add(dep)

    upstream_map: Dict[str, Set[str]] = {node_id: set() for node_id in node_ids}
    for parent, child in deduped_deps:
        if child in upstream_map and parent != "run_started_event":
            upstream_map[child].add(parent)

    task_defs: List[str] = []
    for node in all_nodes:
        node_id = node["id"]
        trigger_rule = "TriggerRule.ALL_SUCCESS"
        if len(upstream_map[node_id]) > 1:
            trigger_rule = "TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS"

        task_defs.append(
            dedent(
                f"""
                {node_id} = PythonOperator(
                    task_id={node_id!r},
                    python_callable=execute_node,
                    op_kwargs={{
                        "task_key": {node_id!r},
                        "node_id": {node_id!r},
                        "node_name": {node['name']!r},
                        "engine": {node['engine']!r},
                        "execution_mode": {node['execution_mode']!r},
                    }},
                    trigger_rule={trigger_rule},
                )
                """
            ).strip()
        )

        if node.get("branch_on_status"):
            task_defs.append(
                dedent(
                    f"""
                    {branch_task_ids[node_id]} = BranchPythonOperator(
                        task_id={branch_task_ids[node_id]!r},
                        python_callable=choose_branch,
                        op_kwargs={{
                            "node_task_id": {node_id!r},
                            "success_task_ids": {node.get('on_success_node_ids', [])!r},
                            "failure_task_ids": {node.get('on_failure_node_ids', [])!r},
                        }},
                        trigger_rule=TriggerRule.ALL_DONE,
                    )
                    """
                ).strip()
            )

    for node_id in node_ids:
        deduped_deps.append((node_id, "finalize_results_task"))
    deduped_deps.append(("finalize_results_task", "run_final_event"))

    dep_lines = [f"{parent} >> {child}" for parent, child in deduped_deps]
    joined_task_defs = "\n\n    ".join(task_defs)
    joined_deps = "\n    ".join(dep_lines)
    schedule_str = repr(schedule) if schedule else "None"

    code = f'''from airflow import DAG
from airflow.exceptions import AirflowException
try:
    from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
except Exception:
    from airflow.operators.python import PythonOperator, BranchPythonOperator
try:
    from airflow.sdk import get_current_context
except Exception:
    from airflow.operators.python import get_current_context
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timezone
import json
import logging
import requests

log = logging.getLogger("airflow.task")

KAFKA_CONN_ID = {KAFKA_CONN_ID!r}
KAFKA_RUN_TOPIC = {KAFKA_RUN_TOPIC!r}
RUN_CONTROL_ID = {run_control_id!r}
FINAL_NODE_IDS = {node_ids!r}
ASYNC_NODE_IDS = {async_node_ids!r}
NODE_NAME_MAP = {node_name_map!r}
HTTP_SUCCESS_CODES = (200, 201, 202, 204)
DEFAULT_ASYNC_SUCCESS_STATUSES = {{"SUCCESS", "SUCCEEDED", "COMPLETED", "DONE", "FINISHED"}}
DEFAULT_ASYNC_FAILURE_STATUSES = {{"FAILED", "FAILURE", "ERROR", "ERRORED", "CANCELLED", "CANCELED", "ABORTED"}}
DEFAULT_ASYNC_RUNNING_STATUSES = {{"RUNNING", "IN_PROGRESS", "PENDING", "QUEUED", "SUBMITTED", "PROCESSING", "STARTED"}}


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


def _to_upper_set(values, fallback):
    if not values:
        return set(fallback)
    return {{str(v).strip().upper() for v in values if str(v).strip()}}


def _extract_by_path(data, path, default=None):
    if not path:
        return data if data is not None else default
    current = data
    for part in str(path).split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except Exception:
                return default
        else:
            return default
        if current is None:
            return default
    return current


def _render_value(value, replacements):
    if isinstance(value, str):
        try:
            return value.format(**replacements)
        except Exception:
            return value
    if isinstance(value, dict):
        return {{k: _render_value(v, replacements) for k, v in value.items()}}
    if isinstance(value, list):
        return [_render_value(v, replacements) for v in value]
    return value


def _parse_response_body(response):
    if not response.text:
        return {{}}
    try:
        return response.json()
    except Exception:
        return {{"raw_response": response.text}}


def _normalize_terminal_status(status_value, success_statuses, failure_statuses, running_statuses):
    status_text = str(status_value or "UNKNOWN").strip()
    status_upper = status_text.upper()
    if status_upper in success_statuses:
        return "SUCCESS", status_text
    if status_upper in failure_statuses:
        return "FAILED", status_text
    if status_upper in running_statuses:
        return "RUNNING", status_text
    return "UNKNOWN", status_text


def execute_node(task_key, node_id, node_name, engine, execution_mode, **context):
    conf = _get_conf(context)
    payload = conf.get(task_key, {{}})
    ti = context["ti"]

    if not payload or not payload.get("url"):
        raise AirflowException(f"Missing URL for {{task_key}}")

    request_timeout = int(payload.get("timeout", 300))
    verify_ssl = payload.get("verify_ssl", False)

    try:
        response = requests.request(
            method=payload.get("method", "POST"),
            url=payload["url"],
            json=payload.get("json"),
            params=payload.get("params"),
            headers=payload.get("headers", {{}}),
            timeout=request_timeout,
            verify=verify_ssl,
        )
        if response.status_code not in HTTP_SUCCESS_CODES:
            raise AirflowException(f"HTTP {{response.status_code}}: {{response.text}}")
    except Exception as exc:
        raise AirflowException(str(exc))

    response_body = _parse_response_body(response)

    if execution_mode == "async_no_wait":
        async_cfg = payload.get("async_status", {{}})
        response_id_key = async_cfg.get("response_id_key") or payload.get("response_id_key") or "job_id"
        tracking_id = (
            async_cfg.get("tracking_id")
            or payload.get("tracking_id")
            or _extract_by_path(response_body, response_id_key)
            or _extract_by_path(response_body, "id")
            or _extract_by_path(response_body, "jobId")
            or _extract_by_path(response_body, "request_id")
        )
        ti.xcom_push(key=f"{{node_id}}_submit_response", value=response_body)
        ti.xcom_push(key=f"{{node_id}}_tracking_id", value=tracking_id)
        ti.xcom_push(key=f"{{node_id}}_submit_http_status", value=response.status_code)
        return {{
            "status": "submitted",
            "node_id": node_id,
            "tracking_id": tracking_id,
            "submit_response": response_body,
        }}

    return response_body


def choose_branch(node_task_id, success_task_ids, failure_task_ids, **context):
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()
    task_instance = next((ti for ti in task_instances if ti.task_id == node_task_id), None)
    state = str(task_instance.state).lower() if task_instance and task_instance.state else ""

    if state == "success":
        return success_task_ids or failure_task_ids
    return failure_task_ids or success_task_ids


def _check_async_status(node_id, node_label, payload, context, current_ti):
    async_cfg = payload.get("async_status", {{}})
    verify_ssl = async_cfg.get("verify_ssl", payload.get("verify_ssl", False))
    request_timeout = int(async_cfg.get("timeout", payload.get("timeout", 300)))
    tracking_id = (
        current_ti.xcom_pull(task_ids=node_id, key=f"{{node_id}}_tracking_id")
        or async_cfg.get("tracking_id")
        or payload.get("tracking_id")
    )
    submit_response = current_ti.xcom_pull(task_ids=node_id, key=f"{{node_id}}_submit_response") or {{}}

    replacements = {{
        "tracking_id": tracking_id or "",
        "job_id": tracking_id or "",
        "node_id": node_id,
        "dag_id": context["dag"].dag_id,
        "dag_run_id": context["dag_run"].run_id,
        "run_control_id": RUN_CONTROL_ID,
    }}

    status_url = _render_value(async_cfg.get("url") or payload.get("status_url"), replacements)
    method = str(async_cfg.get("method") or payload.get("status_method") or "GET").upper()
    headers = _render_value(async_cfg.get("headers") or payload.get("status_headers") or payload.get("headers") or {{}}, replacements)
    params = _render_value(async_cfg.get("params") or payload.get("status_params") or {{}}, replacements)
    json_body = _render_value(async_cfg.get("json") or payload.get("status_json") or None, replacements)

    response_status_key = async_cfg.get("response_status_key") or payload.get("response_status_key") or "status"
    response_id_key = async_cfg.get("response_id_key") or payload.get("response_id_key") or "job_id"

    success_statuses = _to_upper_set(async_cfg.get("success_statuses") or payload.get("success_statuses"), DEFAULT_ASYNC_SUCCESS_STATUSES)
    failure_statuses = _to_upper_set(async_cfg.get("failure_statuses") or payload.get("failure_statuses"), DEFAULT_ASYNC_FAILURE_STATUSES)
    running_statuses = _to_upper_set(async_cfg.get("running_statuses") or payload.get("running_statuses"), DEFAULT_ASYNC_RUNNING_STATUSES)

    if not status_url:
        return {{
            "task_id": node_id,
            "name": node_label,
            "state": "unknown",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "reason": "Missing status URL for async node",
        }}, "FAILED"

    if not tracking_id:
        tracking_id = _extract_by_path(submit_response, response_id_key)

    try:
        response = requests.request(
            method=method,
            url=status_url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=request_timeout,
            verify=verify_ssl,
        )
        status_body = _parse_response_body(response)
    except Exception as exc:
        return {{
            "task_id": node_id,
            "name": node_label,
            "state": "unknown",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "reason": f"Status API call failed: {{exc}}",
        }}, "FAILED"

    if response.status_code == 404:
        return {{
            "task_id": node_id,
            "name": node_label,
            "state": "failed",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "http_status": response.status_code,
            "reason": "Status endpoint returned 404",
            "status_response": status_body,
        }}, "FAILED"

    if response.status_code >= 500:
        return {{
            "task_id": node_id,
            "name": node_label,
            "state": "failed",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "http_status": response.status_code,
            "reason": "Status endpoint returned 5xx",
            "status_response": status_body,
        }}, "FAILED"

    if response.status_code not in HTTP_SUCCESS_CODES:
        return {{
            "task_id": node_id,
            "name": node_label,
            "state": "failed",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "http_status": response.status_code,
            "reason": f"Unexpected status endpoint HTTP code {{response.status_code}}",
            "status_response": status_body,
        }}, "FAILED"

    if not tracking_id:
        tracking_id = _extract_by_path(status_body, response_id_key)

    raw_status = _extract_by_path(status_body, response_status_key)
    normalized_status, status_text = _normalize_terminal_status(raw_status, success_statuses, failure_statuses, running_statuses)

    entry = {{
        "task_id": node_id,
        "name": node_label,
        "state": status_text.lower() if status_text else "unknown",
        "normalized_status": normalized_status,
        "status_source": "external_async_status",
        "tracking_id": tracking_id,
        "http_status": response.status_code,
        "status_response": status_body,
    }}
    return entry, normalized_status


def finalize_results(node_ids, **context):
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()
    conf = _get_conf(context)
    failed, success, skipped, running, unknown = [], [], [], [], []

    for node_id in node_ids:
        task_instance = next((ti for ti in task_instances if ti.task_id == node_id), None)
        state = str(task_instance.state).lower() if task_instance and task_instance.state else ""
        node_label = NODE_NAME_MAP.get(node_id, node_id)
        payload = conf.get(node_id, {{}})

        if node_id in ASYNC_NODE_IDS and state == "success":
            async_entry, async_result = _check_async_status(node_id, node_label, payload, context, context["ti"])
            if async_result == "SUCCESS":
                success.append(async_entry)
            elif async_result == "FAILED":
                failed.append(async_entry)
            elif async_result == "RUNNING":
                running.append(async_entry)
            else:
                unknown.append(async_entry)
            continue

        entry = {{"task_id": node_id, "name": node_label, "state": state or "unknown", "status_source": "airflow_task_state"}}

        if state == "success":
            success.append(entry)
        elif state in {{"skipped"}}:
            skipped.append(entry)
        elif state in {{"failed", "upstream_failed"}}:
            failed.append(entry)
        else:
            unknown.append(entry)

    final_status = "SUCCESS" if not (failed or running or unknown) else "FAILED"

    summary = {{
        "successful_tasks": success,
        "failed_tasks": failed,
        "skipped_tasks": skipped,
        "running_tasks": running,
        "unknown_tasks": unknown,
    }}

    context["ti"].xcom_push(key="final_status", value=final_status)
    context["ti"].xcom_push(key="final_summary", value=summary)
    return {{"final_status": final_status, **summary}}


def build_run_started_event_messages():
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    payload = {{
        "eventType": {EVENTS['run_started']!r},
        "run_control_id": RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "event_source": "AIRFLOW",
        "status": "RUNNING",
        "trigger_payload": _safe_json(_get_conf(ctx)),
        "dagId": ctx["dag"].dag_id,
        "dagRunId": dag_run.run_id,
        "timestamp": _utc_now(),
    }}
    yield (dag_run.run_id, _safe_json(payload).encode())


def build_run_final_event_messages():
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    ti = ctx["ti"]
    status = ti.xcom_pull(task_ids="finalize_results", key="final_status") or "FAILED"
    payload = {{
        "eventType": {EVENTS['run_succeeded']!r} if status == "SUCCESS" else {EVENTS['run_failed']!r},
        "run_control_id": RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "event_source": "AIRFLOW",
        "status": status,
        "trigger_payload": _safe_json(_get_conf(ctx)),
        "dagId": ctx["dag"].dag_id,
        "dagRunId": dag_run.run_id,
        "timestamp": _utc_now(),
    }}
    yield (dag_run.run_id, _safe_json(payload).encode())


with DAG(
    dag_id={dag_id!r},
    start_date=datetime(2024, 1, 1),
    schedule={schedule_str},
    catchup=False,
    tags=["dynamic", "generated"],
) as dag:
    prepare_inputs_task = PythonOperator(task_id="prepare_inputs", python_callable=lambda: True)

    run_started_event = ProduceToTopicOperator(
        task_id="run_started_event",
        kafka_config_id=KAFKA_CONN_ID,
        topic=KAFKA_RUN_TOPIC,
        producer_function=build_run_started_event_messages,
    )

    {joined_task_defs}

    finalize_results_task = PythonOperator(
        task_id="finalize_results",
        python_callable=finalize_results,
        op_kwargs={{"node_ids": FINAL_NODE_IDS}},
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    run_final_event = ProduceToTopicOperator(
        task_id="run_final_event",
        kafka_config_id=KAFKA_CONN_ID,
        topic=KAFKA_RUN_TOPIC,
        producer_function=build_run_final_event_messages,
        trigger_rule=TriggerRule.ALL_DONE,
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

    layers = build_layers(payload.nodes)
    code = generate_dag_code(
        dag_id=dag_id,
        run_control_id=payload.run_control_id,
        sorted_layers=layers,
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
