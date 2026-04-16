import hashlib
import json
import logging
import os
import re
from contextlib import contextmanager
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

# -----------------------------------------------------------------------------
# Service configuration
# -----------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("dynamic-dag-service")

app = FastAPI(title="Dynamic Airflow DAG Service", version="8.3.0")

DAGS_FOLDER = Path(os.getenv("AIRFLOW_DAGS_DIR", "./dag_configs"))
DAGS_FOLDER.mkdir(parents=True, exist_ok=True)

REGISTRY_FILE = Path(os.getenv("BUILD_IDEMPOTENCY_REGISTRY", str(DAGS_FOLDER / "build_registry.json")))
REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)

KAFKA_CONN_ID = os.getenv("KAFKA_CONN_ID", "genesis_kafka_conn")
KAFKA_RUN_TOPIC = os.getenv("KAFKA_RUN_TOPIC", "genesis.hub.run.events.v1")

SUPPORTED_EXECUTION_MODES = {"sync", "async_no_wait", "fire_and_forget"}
SUPPORTED_TRIGGER_TYPES = {"O", "M", "S"}
TRIGGER_TYPE_MAPPING = {"0": "O", "1": "M", "2": "S"}

EVENTS = {
    "run_started": "run.started.v1",
    "run_succeeded": "run.succeeded.v1",
    "run_failed": "run.failed.v1",
}


# -----------------------------------------------------------------------------
# Pydantic models (request validation)
# -----------------------------------------------------------------------------


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

            if node.branch_on_status and not referenced_ids:
                errors.append(f"Node {node.id} has branch_on_status=true but no branch targets")

            if not node.branch_on_status and referenced_ids:
                errors.append(f"Node {node.id} has branch targets but branch_on_status=false")

            if node.execution_mode == "fire_and_forget" and node.branch_on_status:
                errors.append(
                    f"Node {node.id} uses fire_and_forget and cannot branch on final status"
                )

        if errors:
            raise ValueError("; ".join(errors))

        return self


# -----------------------------------------------------------------------------
# FastAPI exception handlers
# -----------------------------------------------------------------------------


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


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def sanitize_identifier(raw: str) -> str:
    value = (raw or "").strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "dag"


def python_var_safe(raw: str) -> str:
    base = sanitize_identifier(raw)
    if base[0].isdigit():
        base = f"n_{base}"
    return base


@contextmanager
def file_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        try:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
        tmp.write(content)
        tmp_name = tmp.name
    os.chmod(tmp_name, 0o644)
    Path(tmp_name).replace(path)


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        logger.warning("Failed to load JSON file %s; using default", path)
        return default


def write_json_file(path: Path, data: Any) -> None:
    atomic_write_text(path, json.dumps(data, indent=2, sort_keys=True, default=str))


def canonicalize_payload_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): canonicalize_payload_value(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, list):
        return [canonicalize_payload_value(item) for item in value]
    if isinstance(value, tuple):
        return [canonicalize_payload_value(item) for item in value]
    if isinstance(value, str):
        return value.strip()
    return value


def canonicalize_build_payload(payload: BuildDagPayload) -> Dict[str, Any]:
    normalized_nodes: List[Dict[str, Any]] = []
    ordered_nodes = sorted(
        payload.nodes,
        key=lambda node: (node.executor_order_id, node.executor_sequence_id, node.id),
    )
    for node in ordered_nodes:
        normalized_nodes.append(
            {
                "id": node.id,
                "name": node.name,
                "engine": node.engine,
                "executor_order_id": node.executor_order_id,
                "executor_sequence_id": node.executor_sequence_id,
                "execution_mode": node.execution_mode,
                "branch_on_status": node.branch_on_status,
                "on_success_node_ids": sorted(node.on_success_node_ids),
                "on_failure_node_ids": sorted(node.on_failure_node_ids),
            }
        )
    return {
        "run_control_id": payload.run_control_id,
        "trigger_type": payload.trigger_type,
        "schedule": payload.schedule,
        "nodes": normalized_nodes,
    }


def compute_sha256(payload: Any) -> str:
    canonical = canonicalize_payload_value(payload)
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_layers(nodes: List[Node]) -> List[Tuple[int, List[Dict[str, Any]]]]:
    layers: Dict[int, List[Dict[str, Any]]] = {}
    for node in nodes:
        layers.setdefault(node.executor_order_id, []).append(node.model_dump())
    return [
        (order_id, sorted(items, key=lambda item: item["executor_sequence_id"]))
        for order_id, items in sorted(layers.items(), key=lambda item: item[0])
    ]


def build_stage_dependencies(
    sorted_layers: List[Tuple[int, List[Dict[str, Any]]]]
) -> List[Tuple[str, str]]:
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
                    parent
                    for parent in prev_nodes
                    if parent["executor_sequence_id"] == child["executor_sequence_id"]
                    and parent["id"] not in branch_source_ids
                ]
                for parent in same_seq_parents:
                    deps.append((parent["id"], child["id"]))

    return deps


def validate_generated_python(code: str) -> None:
    compile(code, "<generated_dag>", "exec")


# -----------------------------------------------------------------------------
# Build idempotency registry
# -----------------------------------------------------------------------------


def get_existing_registry_entry(idempotency_key: str) -> Optional[Dict[str, Any]]:
    with file_lock(REGISTRY_FILE.with_suffix(".lock")):
        registry = load_json_file(REGISTRY_FILE, default={})
        entry = registry.get(idempotency_key)
        if not entry:
            return None
        path_str = entry.get("path")
        if not path_str:
            return None
        dag_file_path = Path(path_str)
        if not dag_file_path.exists():
            logger.warning("Registry entry exists but DAG file is missing for key %s", idempotency_key)
            registry.pop(idempotency_key, None)
            write_json_file(REGISTRY_FILE, registry)
            return None
        return entry


def store_registry_entry(idempotency_key: str, entry: Dict[str, Any]) -> None:
    with file_lock(REGISTRY_FILE.with_suffix(".lock")):
        registry = load_json_file(REGISTRY_FILE, default={})
        registry[idempotency_key] = entry
        write_json_file(REGISTRY_FILE, registry)


# -----------------------------------------------------------------------------
# DAG code generator (Airflow 3.0.6 oriented)
# -----------------------------------------------------------------------------


def generate_dag_code(
    dag_id: str,
    run_control_id: str,
    sorted_layers: List[Tuple[int, List[Dict[str, Any]]]],
    schedule: Optional[str],
) -> str:
    all_nodes = [node for _, layer_nodes in sorted_layers for node in layer_nodes]
    node_ids = [node["id"] for node in all_nodes]
    async_node_ids = [node["id"] for node in all_nodes if node["execution_mode"] == "async_no_wait"]
    fire_and_forget_node_ids = [node["id"] for node in all_nodes if node["execution_mode"] == "fire_and_forget"]
    node_name_map = {node["id"]: node["name"] for node in all_nodes}

    var_map: Dict[str, str] = {}
    used_vars: Set[str] = set()
    for nid in node_ids:
        base = python_var_safe(nid)
        candidate = base
        i = 2
        while candidate in used_vars:
            candidate = f"{base}_{i}"
            i += 1
        used_vars.add(candidate)
        var_map[nid] = candidate

    branch_task_ids: Dict[str, str] = {
        node["id"]: f"branch_{node['id']}"
        for node in all_nodes
        if node.get("branch_on_status")
    }

    branch_var_map: Dict[str, str] = {}
    for nid, branch_tid in branch_task_ids.items():
        base = f"branch_{var_map[nid]}"
        candidate = base
        i = 2
        while candidate in used_vars:
            candidate = f"{base}_{i}"
            i += 1
        used_vars.add(candidate)
        branch_var_map[branch_tid] = candidate

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

    branch_skip_whitelist: Set[str] = set()
    for node in all_nodes:
        if node.get("branch_on_status"):
            branch_skip_whitelist.update(node.get("on_success_node_ids", []))
            branch_skip_whitelist.update(node.get("on_failure_node_ids", []))

    merge_node_ids = {node_id for node_id, parents in upstream_map.items() if len(parents) > 1}

    task_defs: List[str] = []
    for node in all_nodes:
        node_id = node["id"]
        py_var = var_map[node_id]

        trigger_rule = "TriggerRule.ALL_SUCCESS"
        if len(upstream_map[node_id]) > 1:
            trigger_rule = "TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS"

        task_defs.append(
            dedent(
                f"""
                {py_var} = PythonOperator(
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
                    retries=0,
                )
                """.strip()
            )
        )

        if node.get("branch_on_status"):
            branch_tid = branch_task_ids[node_id]
            branch_var = branch_var_map[branch_tid]
            task_defs.append(
                dedent(
                    f"""
                    {branch_var} = BranchPythonOperator(
                        task_id={branch_tid!r},
                        python_callable=choose_branch,
                        op_kwargs={{
                            "node_task_id": {node_id!r},
                            "success_task_ids": {node.get('on_success_node_ids', [])!r},
                            "failure_task_ids": {node.get('on_failure_node_ids', [])!r},
                        }},
                        trigger_rule=TriggerRule.ALL_DONE,
                        retries=0,
                    )
                    """.strip()
                )
            )

    for node_id in node_ids:
        deduped_deps.append((node_id, "finalize_results"))
    deduped_deps.append(("finalize_results", "run_final_event"))

    special_task_vars = {
        "prepare_inputs": "prepare_inputs_task",
        "run_started_event": "run_started_event",
        "finalize_results": "finalize_results_task",
        "run_final_event": "run_final_event",
    }

    def to_var(task_ref: str) -> str:
        if task_ref in special_task_vars:
            return special_task_vars[task_ref]
        if task_ref in var_map:
            return var_map[task_ref]
        if task_ref in branch_var_map:
            return branch_var_map[task_ref]
        return python_var_safe(task_ref)

    dep_lines = [f"{to_var(parent)} >> {to_var(child)}" for parent, child in deduped_deps]
    joined_task_defs = "\n\n    ".join(task_defs)
    joined_deps = "\n    ".join(dep_lines)
    schedule_str = repr(schedule) if schedule else "None"

    template = """from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import BranchPythonOperator, PythonOperator, get_current_context
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger("airflow.task")

KAFKA_CONN_ID = __KAFKA_CONN_ID__
KAFKA_RUN_TOPIC = __KAFKA_RUN_TOPIC__
RUN_CONTROL_ID = __RUN_CONTROL_ID__
FINAL_NODE_IDS = __FINAL_NODE_IDS__
ASYNC_NODE_IDS = __ASYNC_NODE_IDS__
FIRE_AND_FORGET_NODE_IDS = __FIRE_AND_FORGET_NODE_IDS__
NODE_NAME_MAP = __NODE_NAME_MAP__
BRANCH_SKIP_WHITELIST = __BRANCH_SKIP_WHITELIST__
MERGE_NODE_IDS = __MERGE_NODE_IDS__
IDEMPOTENCY_DIR = __IDEMPOTENCY_DIR__
IDEMPOTENCY_STATE_FILE = str(Path(IDEMPOTENCY_DIR) / "node_execution_journal.json")
IDEMPOTENCY_LOCK_FILE = str(Path(IDEMPOTENCY_DIR) / "node_execution_journal.lock")

HTTP_SUCCESS_CODES = (200, 201, 202, 204)
DEFAULT_ASYNC_SUCCESS_STATUSES = {"SUCCESS", "SUCCEEDED", "COMPLETED", "DONE", "FINISHED"}
DEFAULT_ASYNC_FAILURE_STATUSES = {"FAILED", "FAILURE", "ERROR", "ERRORED", "CANCELLED", "CANCELED", "ABORTED"}
DEFAULT_ASYNC_RUNNING_STATUSES = {"RUNNING", "IN_PROGRESS", "PENDING", "QUEUED", "SUBMITTED", "PROCESSING", "STARTED"}


def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_json(value):
    return json.dumps(value, default=str)


def _get_conf(ctx):
    dag_run = ctx["dag_run"]
    return dag_run.conf or {}


def _get_correlation_id(ctx):
    conf = _get_conf(ctx)
    return conf.get("correlation_id") or ctx["dag_run"].run_id


def _get_effective_idempotency_key(ctx):
    conf = _get_conf(ctx)
    explicit = str(conf.get("idempotency_key") or "").strip()
    if explicit:
        return explicit
    return f"{RUN_CONTROL_ID}:{ctx['dag'].dag_id}"


def _to_upper_set(values, fallback):
    if not values:
        return set(fallback)
    return {str(v).strip().upper() for v in values if str(v).strip()}


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
        return {k: _render_value(v, replacements) for k, v in value.items()}
    if isinstance(value, list):
        return [_render_value(v, replacements) for v in value]
    return value


def _parse_response_body(response):
    if not response.text:
        return {}
    try:
        return response.json()
    except Exception:
        return {"raw_response": response.text}


def _canonicalize(value):
    if isinstance(value, dict):
        return {str(k): _canonicalize(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    if isinstance(value, tuple):
        return [_canonicalize(item) for item in value]
    if isinstance(value, str):
        return value.strip()
    return value


def _hash_payload(value):
    raw = json.dumps(_canonicalize(value), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@contextmanager
def _file_lock(lock_path):
    lock_file = Path(lock_path)
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_file, "a+", encoding="utf-8") as handle:
        try:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass


def _atomic_write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=target.parent, delete=False) as tmp:
        json.dump(payload, tmp, indent=2, sort_keys=True, default=str)
        tmp_name = tmp.name
    Path(tmp_name).replace(target)


def _load_json(path, default):
    target = Path(path)
    if not target.exists():
        return default
    try:
        with target.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        log.warning("Failed to read JSON file %s; using default", path)
        return default


def _get_journal():
    with _file_lock(IDEMPOTENCY_LOCK_FILE):
        data = _load_json(IDEMPOTENCY_STATE_FILE, {"operations": {}})
        if not isinstance(data, dict):
            data = {"operations": {}}
        data.setdefault("operations", {})
        return data


def _save_journal(journal):
    with _file_lock(IDEMPOTENCY_LOCK_FILE):
        _atomic_write_json(IDEMPOTENCY_STATE_FILE, journal)


def _upsert_operation(operation_key, record):
    with _file_lock(IDEMPOTENCY_LOCK_FILE):
        journal = _load_json(IDEMPOTENCY_STATE_FILE, {"operations": {}})
        journal.setdefault("operations", {})
        journal["operations"][operation_key] = record
        _atomic_write_json(IDEMPOTENCY_STATE_FILE, journal)


def _get_operation(operation_key):
    with _file_lock(IDEMPOTENCY_LOCK_FILE):
        journal = _load_json(IDEMPOTENCY_STATE_FILE, {"operations": {}})
        journal.setdefault("operations", {})
        return journal["operations"].get(operation_key)


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


def _build_execution_identity(ctx, node_id, payload, execution_mode):
    idempotency_key = _get_effective_idempotency_key(ctx)
    request_fingerprint_payload = {
        "node_id": node_id,
        "execution_mode": execution_mode,
        "method": str(payload.get("method", "POST")).upper(),
        "url": payload.get("url"),
        "params": payload.get("params"),
        "json": payload.get("json"),
        "headers": payload.get("headers", {}),
        "timeout": int(payload.get("timeout", 300)),
        "verify_ssl": payload.get("verify_ssl", False),
        "async_status": payload.get("async_status", {}),
        "status_url": payload.get("status_url"),
        "status_method": payload.get("status_method"),
        "status_headers": payload.get("status_headers"),
        "status_params": payload.get("status_params"),
        "status_json": payload.get("status_json"),
        "response_id_key": payload.get("response_id_key"),
        "response_status_key": payload.get("response_status_key"),
        "success_statuses": payload.get("success_statuses"),
        "failure_statuses": payload.get("failure_statuses"),
        "running_statuses": payload.get("running_statuses"),
    }
    request_hash = _hash_payload(request_fingerprint_payload)
    operation_key = f"{RUN_CONTROL_ID}:{node_id}:{idempotency_key}"
    return operation_key, request_hash, idempotency_key


def _write_success_xcom(ti, node_id, response_body, http_status=None, tracking_id=None, marker_status="success"):
    ti.xcom_push(key=f"{node_id}_task_state", value=marker_status)
    ti.xcom_push(key=f"{node_id}_branch", value="success")
    ti.xcom_push(key=f"{node_id}_submit_response", value=response_body)
    if http_status is not None:
        ti.xcom_push(key=f"{node_id}_submit_http_status", value=http_status)
    if tracking_id is not None:
        ti.xcom_push(key=f"{node_id}_tracking_id", value=tracking_id)


def execute_node(task_key, node_id, node_name, engine, execution_mode, **kwargs):
    ctx = get_current_context()
    conf = _get_conf(ctx)
    payload = conf.get(task_key, {})
    ti = ctx["ti"]

    ti.xcom_push(key=f"{node_id}_task_state", value="started")

    if not payload or not payload.get("url"):
        ti.xcom_push(key=f"{node_id}_task_state", value="failed")
        ti.xcom_push(key=f"{node_id}_branch", value="failure")
        raise AirflowException(f"Missing URL for {task_key}")

    operation_key, request_hash, idempotency_key = _build_execution_identity(ctx, node_id, payload, execution_mode)
    existing = _get_operation(operation_key)

    if existing and existing.get("request_hash") == request_hash and existing.get("status") == "SUCCESS":
        stored_response = existing.get("response_body") or {}
        stored_http_status = existing.get("http_status")
        stored_tracking_id = existing.get("tracking_id")
        _write_success_xcom(
            ti=ti,
            node_id=node_id,
            response_body=stored_response,
            http_status=stored_http_status,
            tracking_id=stored_tracking_id,
        )
        return {
            "status": "idempotent_replay",
            "node_id": node_id,
            "operation_key": operation_key,
            "idempotency_key": idempotency_key,
            "tracking_id": stored_tracking_id,
            "response": stored_response,
        }

    if existing and existing.get("request_hash") == request_hash and existing.get("status") == "IN_PROGRESS":
        raise AirflowException(
            f"Duplicate in-progress operation detected for node_id={node_id}, operation_key={operation_key}"
        )

    request_timeout = int(payload.get("timeout", 300))
    verify_ssl = payload.get("verify_ssl", False)
    request_record = {
        "status": "IN_PROGRESS",
        "operation_key": operation_key,
        "idempotency_key": idempotency_key,
        "request_hash": request_hash,
        "run_control_id": RUN_CONTROL_ID,
        "dag_id": ctx["dag"].dag_id,
        "dag_run_id": ctx["dag_run"].run_id,
        "node_id": node_id,
        "node_name": node_name,
        "execution_mode": execution_mode,
        "engine": engine,
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
    }
    _upsert_operation(operation_key, request_record)

    try:
        response = requests.request(
            method=payload.get("method", "POST"),
            url=payload["url"],
            json=payload.get("json"),
            params=payload.get("params"),
            headers=payload.get("headers", {}),
            timeout=request_timeout,
            verify=verify_ssl,
        )
        response_body = _parse_response_body(response)
    except Exception as exc:
        _upsert_operation(
            operation_key,
            {
                **request_record,
                "status": "FAILED",
                "updated_at": _utc_now(),
                "error": str(exc),
            },
        )
        ti.xcom_push(key=f"{node_id}_task_state", value="failed")
        ti.xcom_push(key=f"{node_id}_branch", value="failure")
        ti.xcom_push(key=f"{node_id}_error", value=str(exc))
        raise AirflowException(str(exc))

    if response.status_code not in HTTP_SUCCESS_CODES:
        _upsert_operation(
            operation_key,
            {
                **request_record,
                "status": "FAILED",
                "updated_at": _utc_now(),
                "http_status": response.status_code,
                "response_body": response_body,
                "error": f"HTTP {response.status_code}: {response.text}",
            },
        )
        ti.xcom_push(key=f"{node_id}_task_state", value="failed")
        ti.xcom_push(key=f"{node_id}_branch", value="failure")
        ti.xcom_push(key=f"{node_id}_error", value=f"HTTP {response.status_code}: {response.text}")
        raise AirflowException(f"HTTP {response.status_code}: {response.text}")

    if execution_mode == "fire_and_forget":
        _write_success_xcom(
            ti=ti,
            node_id=node_id,
            response_body=response_body,
            http_status=response.status_code,
        )
        _upsert_operation(
            operation_key,
            {
                **request_record,
                "status": "SUCCESS",
                "updated_at": _utc_now(),
                "http_status": response.status_code,
                "response_body": response_body,
                "tracking_id": None,
                "result_kind": "submitted_no_track",
            },
        )
        return {
            "status": "submitted_no_track",
            "node_id": node_id,
            "operation_key": operation_key,
            "submit_response": response_body,
        }

    if execution_mode == "async_no_wait":
        async_cfg = payload.get("async_status", {})
        response_id_key = async_cfg.get("response_id_key") or payload.get("response_id_key") or "job_id"
        tracking_id = (
            async_cfg.get("tracking_id")
            or payload.get("tracking_id")
            or _extract_by_path(response_body, response_id_key)
            or _extract_by_path(response_body, "id")
            or _extract_by_path(response_body, "jobId")
            or _extract_by_path(response_body, "request_id")
        )
        _write_success_xcom(
            ti=ti,
            node_id=node_id,
            response_body=response_body,
            http_status=response.status_code,
            tracking_id=tracking_id,
        )
        _upsert_operation(
            operation_key,
            {
                **request_record,
                "status": "SUCCESS",
                "updated_at": _utc_now(),
                "http_status": response.status_code,
                "response_body": response_body,
                "tracking_id": tracking_id,
                "result_kind": "submitted_async",
            },
        )
        return {
            "status": "submitted",
            "node_id": node_id,
            "operation_key": operation_key,
            "tracking_id": tracking_id,
            "submit_response": response_body,
        }

    _write_success_xcom(
        ti=ti,
        node_id=node_id,
        response_body=response_body,
        http_status=response.status_code,
    )
    _upsert_operation(
        operation_key,
        {
            **request_record,
            "status": "SUCCESS",
            "updated_at": _utc_now(),
            "http_status": response.status_code,
            "response_body": response_body,
            "tracking_id": None,
            "result_kind": "sync_response",
        },
    )
    return response_body


def choose_branch(node_task_id, success_task_ids, failure_task_ids, **kwargs):
    ctx = get_current_context()
    ti = ctx["ti"]
    marker = ti.xcom_pull(task_ids=node_task_id, key=f"{node_task_id}_branch")

    if marker == "success":
        if not success_task_ids:
            raise AirflowException("No success_task_ids configured")
        return success_task_ids

    if not failure_task_ids:
        raise AirflowException("No failure_task_ids configured")
    return failure_task_ids


def _check_async_status(node_id, node_label, payload):
    ctx = get_current_context()
    ti = ctx["ti"]

    async_cfg = payload.get("async_status", {})
    verify_ssl = async_cfg.get("verify_ssl", payload.get("verify_ssl", False))
    request_timeout = int(async_cfg.get("timeout", payload.get("timeout", 300)))
    tracking_id = (
        ti.xcom_pull(task_ids=node_id, key=f"{node_id}_tracking_id")
        or async_cfg.get("tracking_id")
        or payload.get("tracking_id")
    )
    submit_response = ti.xcom_pull(task_ids=node_id, key=f"{node_id}_submit_response") or {}

    replacements = {
        "tracking_id": tracking_id or "",
        "job_id": tracking_id or "",
        "node_id": node_id,
        "dag_id": ctx["dag"].dag_id,
        "dag_run_id": ctx["dag_run"].run_id,
        "run_control_id": RUN_CONTROL_ID,
    }

    status_url = _render_value(async_cfg.get("url") or payload.get("status_url"), replacements)
    method = str(async_cfg.get("method") or payload.get("status_method") or "GET").upper()
    headers = _render_value(async_cfg.get("headers") or payload.get("status_headers") or payload.get("headers") or {}, replacements)
    params = _render_value(async_cfg.get("params") or payload.get("status_params") or {}, replacements)
    json_body = _render_value(async_cfg.get("json") or payload.get("status_json") or None, replacements)

    response_status_key = async_cfg.get("response_status_key") or payload.get("response_status_key") or "status"
    response_id_key = async_cfg.get("response_id_key") or payload.get("response_id_key") or "job_id"

    success_statuses = _to_upper_set(async_cfg.get("success_statuses") or payload.get("success_statuses"), DEFAULT_ASYNC_SUCCESS_STATUSES)
    failure_statuses = _to_upper_set(async_cfg.get("failure_statuses") or payload.get("failure_statuses"), DEFAULT_ASYNC_FAILURE_STATUSES)
    running_statuses = _to_upper_set(async_cfg.get("running_statuses") or payload.get("running_statuses"), DEFAULT_ASYNC_RUNNING_STATUSES)

    if not status_url:
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "unknown",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "reason": "Missing status URL for async node",
        }, "FAILED"

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
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "unknown",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "reason": f"Status API call failed: {exc}",
        }, "FAILED"

    if response.status_code == 404:
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "failed",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "http_status": response.status_code,
            "reason": "Status endpoint returned 404",
            "status_response": status_body,
        }, "FAILED"

    if response.status_code >= 500:
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "failed",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "http_status": response.status_code,
            "reason": "Status endpoint returned 5xx",
            "status_response": status_body,
        }, "FAILED"

    if response.status_code not in HTTP_SUCCESS_CODES:
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "failed",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "http_status": response.status_code,
            "reason": f"Unexpected status endpoint HTTP code {response.status_code}",
            "status_response": status_body,
        }, "FAILED"

    if not tracking_id:
        tracking_id = _extract_by_path(status_body, response_id_key)

    raw_status = _extract_by_path(status_body, response_status_key)
    normalized_status, status_text = _normalize_terminal_status(raw_status, success_statuses, failure_statuses, running_statuses)

    entry = {
        "task_id": node_id,
        "name": node_label,
        "state": status_text.lower() if status_text else "unknown",
        "normalized_status": normalized_status,
        "status_source": "external_async_status",
        "tracking_id": tracking_id,
        "http_status": response.status_code,
        "status_response": status_body,
    }
    return entry, normalized_status


def finalize_results(node_ids, **kwargs):
    ctx = get_current_context()
    conf = _get_conf(ctx)
    ti = ctx["ti"]

    failed = []
    success = []
    expected_skipped = []
    unexpected_skipped = []
    running = []
    unknown = []

    for node_id in node_ids:
        node_label = NODE_NAME_MAP.get(node_id, node_id)
        payload = conf.get(node_id, {})

        marker = ti.xcom_pull(task_ids=node_id, key=f"{node_id}_task_state")
        marker = (str(marker).lower().strip() if marker is not None else "")

        if node_id in FIRE_AND_FORGET_NODE_IDS:
            entry = {
                "task_id": node_id,
                "name": node_label,
                "state": "submitted_no_track" if marker == "success" else (marker or "unknown"),
                "status_source": "airflow_submit_only",
            }
            if marker == "success":
                success.append(entry)
            elif marker == "failed":
                failed.append(entry)
            elif not marker and node_id in BRANCH_SKIP_WHITELIST and node_id not in MERGE_NODE_IDS:
                expected_skipped.append({**entry, "skip_type": "expected_branch_skip"})
            else:
                unknown.append(entry)
            continue

        if node_id in ASYNC_NODE_IDS and marker == "success":
            async_entry, async_result = _check_async_status(node_id, node_label, payload)
            if async_result == "SUCCESS":
                success.append(async_entry)
            elif async_result == "FAILED":
                failed.append(async_entry)
            elif async_result == "RUNNING":
                running.append(async_entry)
            else:
                unknown.append(async_entry)
            continue

        entry = {
            "task_id": node_id,
            "name": node_label,
            "state": marker or "unknown",
            "status_source": "xcom_task_state_marker",
        }

        if marker == "success":
            success.append(entry)
        elif marker == "failed":
            failed.append(entry)
        elif not marker:
            if node_id in BRANCH_SKIP_WHITELIST and node_id not in MERGE_NODE_IDS:
                expected_skipped.append({**entry, "skip_type": "expected_branch_skip"})
            else:
                unknown.append(entry)
        else:
            unknown.append(entry)

    has_any_problem = bool(failed or unexpected_skipped or running or unknown)
    final_status = "FAILED" if has_any_problem else "SUCCESS"

    summary = {
        "final_status": final_status,
        "successful_tasks": success,
        "failed_tasks": failed,
        "expected_skipped_tasks": expected_skipped,
        "unexpected_skipped_tasks": unexpected_skipped,
        "running_tasks": running,
        "unknown_tasks": unknown,
    }

    ti.xcom_push(key="final_status", value=final_status)
    ti.xcom_push(key="final_summary", value=summary)

    if has_any_problem:
        raise AirflowException(
            f"Run failed during finalize_results. "
            f"failed={len(failed)}, expected_skipped={len(expected_skipped)}, "
            f"unexpected_skipped={len(unexpected_skipped)}, running={len(running)}, "
            f"unknown={len(unknown)}"
        )

    return summary


def build_run_started_event_messages():
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    payload = {
        "eventType": __EVENT_RUN_STARTED__,
        "run_control_id": RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "event_source": "AIRFLOW",
        "status": "RUNNING",
        "trigger_payload": _safe_json(_get_conf(ctx)),
        "dagId": ctx["dag"].dag_id,
        "dagRunId": dag_run.run_id,
        "timestamp": _utc_now(),
    }
    yield (dag_run.run_id, _safe_json(payload).encode())


def build_run_final_event_messages():
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    ti = ctx["ti"]
    status_value = ti.xcom_pull(task_ids="finalize_results", key="final_status") or "FAILED"

    payload = {
        "eventType": __EVENT_RUN_SUCCEEDED__ if status_value == "SUCCESS" else __EVENT_RUN_FAILED__,
        "run_control_id": RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "event_source": "AIRFLOW",
        "status": status_value,
        "trigger_payload": _safe_json(_get_conf(ctx)),
        "dagId": ctx["dag"].dag_id,
        "dagRunId": dag_run.run_id,
        "timestamp": _utc_now(),
    }
    yield (dag_run.run_id, _safe_json(payload).encode())


with DAG(
    dag_id=__DAG_ID__,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=__SCHEDULE__,
    catchup=False,
    tags=["dynamic", "generated", "idempotent"],
    default_args={"retries": 0},
) as dag:
    prepare_inputs_task = PythonOperator(task_id="prepare_inputs", python_callable=lambda: True, retries=0)

    run_started_event = ProduceToTopicOperator(
        task_id="run_started_event",
        kafka_config_id=KAFKA_CONN_ID,
        topic=KAFKA_RUN_TOPIC,
        producer_function=build_run_started_event_messages,
    )

    __JOINED_TASK_DEFS__

    finalize_results_task = PythonOperator(
        task_id="finalize_results",
        python_callable=finalize_results,
        op_kwargs={"node_ids": FINAL_NODE_IDS},
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
    __JOINED_DEPS__
"""

    replacements = {
        "__KAFKA_CONN_ID__": repr(KAFKA_CONN_ID),
        "__KAFKA_RUN_TOPIC__": repr(KAFKA_RUN_TOPIC),
        "__RUN_CONTROL_ID__": repr(run_control_id),
        "__FINAL_NODE_IDS__": repr(node_ids),
        "__ASYNC_NODE_IDS__": repr(async_node_ids),
        "__FIRE_AND_FORGET_NODE_IDS__": repr(fire_and_forget_node_ids),
        "__NODE_NAME_MAP__": repr(node_name_map),
        "__BRANCH_SKIP_WHITELIST__": repr(sorted(branch_skip_whitelist)),
        "__MERGE_NODE_IDS__": repr(sorted(merge_node_ids)),
        "__IDEMPOTENCY_DIR__": repr(str(DAGS_FOLDER / "idempotency_state")),
        "__EVENT_RUN_STARTED__": repr(EVENTS["run_started"]),
        "__EVENT_RUN_SUCCEEDED__": repr(EVENTS["run_succeeded"]),
        "__EVENT_RUN_FAILED__": repr(EVENTS["run_failed"]),
        "__DAG_ID__": repr(dag_id),
        "__SCHEDULE__": schedule_str,
        "__JOINED_TASK_DEFS__": joined_task_defs,
        "__JOINED_DEPS__": joined_deps,
    }

    result = template
    for key, val in replacements.items():
        result = result.replace(key, val)
    return result


# -----------------------------------------------------------------------------
# API endpoints
# -----------------------------------------------------------------------------


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "UP", "version": app.version}


@app.post("/build_dag", status_code=status.HTTP_201_CREATED)
def build_dag(payload: BuildDagPayload) -> Dict[str, Any]:
    dag_id = f"{sanitize_identifier(payload.run_control_id)}_dag"
    canonical_build_payload = canonicalize_build_payload(payload)
    idempotency_key = compute_sha256(canonical_build_payload)

    existing_entry = get_existing_registry_entry(idempotency_key)
    if existing_entry:
        logger.info("Idempotent build hit for dag_id=%s", existing_entry.get("dag_id"))
        return {
            "status": "SUCCESS",
            "dag_id": existing_entry["dag_id"],
            "file": existing_entry["file"],
            "path": existing_entry["path"],
            "idempotency_key": idempotency_key,
            "idempotent_reused": True,
        }

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

    registry_entry = {
        "dag_id": dag_id,
        "file": dag_file_name,
        "path": str(dag_file_path.resolve()),
        "run_control_id": payload.run_control_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "idempotency_key": idempotency_key,
        "canonical_build_payload": canonical_build_payload,
    }
    store_registry_entry(idempotency_key, registry_entry)

    logger.info("DAG generated: %s", dag_id)
    return {
        "status": "SUCCESS",
        "dag_id": dag_id,
        "file": dag_file_name,
        "path": str(dag_file_path.resolve()),
        "idempotency_key": idempotency_key,
        "idempotent_reused": False,
    }


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8043"))
    uvicorn.run(app, host=host, port=port, log_level=LOG_LEVEL.lower())
