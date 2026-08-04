# internal/base.py - 基础模块
from __future__ import annotations

import json as _json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from enum import IntEnum
from typing import Optional, Any

# ============================================================================
# 常量
# ============================================================================

DEFAULT_QUEUE_NAME: str = "default"
DEFAULT_MAX_RETRY: int = 25
DEFAULT_TIMEOUT: int = 0
DEFAULT_RETENTION: int = 0
DEFAULT_SHUTDOWN_TIMEOUT: int = 8
SERVER_HEARTBEAT_INTERVAL: int = 5
LEASE_DURATION: int = 30
DEFAULT_GROUP_MAX_DELAY: int = 30
DEFAULT_GROUP_GRACE_PERIOD: int = 60
DEFAULT_GROUP_MAX_SIZE: int = 20
QUEUE_PAUSED_TTL: int = 10
UNIQUE_LOCK_TTL: int = 60
ASYNQ_VERSION: str = "0.1.0"
REDIS_PREFIX: str = "asyrq"

# 任务队列 key 前缀: asyrq:tasks:{task_type}:{route}:{queue}:{suffix}
# 例: client 生产任务推送到的任务数据 key 为
#     asyrq:tasks:{task_type}:{route}:{queue}:{task_id}
TASKS_PREFIX: str = "asyrq:tasks"


def split_typename(typename: str) -> tuple[str, str]:
    """把完整类型名拆成 task_type 和 route。

    "pmos_liaoning:rqdj" -> ("pmos_liaoning", "rqdj")
    "demo:task"          -> ("demo", "task")
    "email"              -> ("email", "")
    """
    task_type, sep, route = typename.partition(":")
    return task_type, (route if sep else "")


def task_base(task_type: str, route: str, queue: str) -> str:
    """任务类 key 的公共前缀: asyrq:tasks:{task_type}:{route}:{queue}"""
    return f"{TASKS_PREFIX}:{task_type}:{route}:{queue}"


def pending_key(task_type: str, route: str, queue: str) -> str:
    return f"{task_base(task_type, route, queue)}:pending"


def active_key(task_type: str, route: str, queue: str) -> str:
    return f"{task_base(task_type, route, queue)}:active"


def scheduled_key(task_type: str, route: str, queue: str) -> str:
    return f"{task_base(task_type, route, queue)}:scheduled"


def retry_key(task_type: str, route: str, queue: str) -> str:
    return f"{task_base(task_type, route, queue)}:retry"


def archived_key(task_type: str, route: str, queue: str) -> str:
    return f"{task_base(task_type, route, queue)}:archived"


def completed_key(task_type: str, route: str, queue: str) -> str:
    return f"{task_base(task_type, route, queue)}:completed"


def task_key(task_type: str, route: str, queue: str, task_id: str) -> str:
    return f"{task_base(task_type, route, queue)}:{task_id}"


def paused_key(task_type: str, route: str, queue: str) -> str:
    return f"{task_base(task_type, route, queue)}:paused"


def lease_key(task_type: str, route: str, queue: str) -> str:
    return f"{task_base(task_type, route, queue)}:lease"


def group_key(task_type: str, route: str, queue: str, gname: str) -> str:
    return f"{task_base(task_type, route, queue)}:aggregation:{gname}"


def all_groups_key(task_type: str, route: str, queue: str) -> str:
    return f"{task_base(task_type, route, queue)}:groups"


def processed_key(task_type: str, route: str, queue: str) -> str:
    return f"{task_base(task_type, route, queue)}:processed"


def failed_key(task_type: str, route: str, queue: str) -> str:
    return f"{task_base(task_type, route, queue)}:failed"


def unique_key(task_type: str, route: str, queue: str, payload: bytes) -> str:
    import hashlib
    h = hashlib.sha256(payload).hexdigest()
    return f"{task_base(task_type, route, queue)}:unique:{h}"

# 全局 key
def all_queues_key() -> str:
    return f"{REDIS_PREFIX}:queues"

def servers_key() -> str:
    return f"{REDIS_PREFIX}:servers"

def workers_key(hostname: str, pid: int, server_id: str) -> str:
    return f"{REDIS_PREFIX}:workers:{hostname}:{pid}:{server_id}"

def scheduler_key(entry_id: str) -> str:
    return f"{REDIS_PREFIX}:schedulers:{entry_id}"

def scheduler_history_key(entry_id: str) -> str:
    return f"{REDIS_PREFIX}:scheduler_history:{entry_id}"

def server_info_key(server_id: str) -> str:
    return f"{REDIS_PREFIX}:servers:{server_id}"

# 常量别名
ALL_QUEUES_KEY: str = all_queues_key()
SERVERS_KEY: str = servers_key()

# ============================================================================
# TaskState
# ============================================================================

class TaskState(IntEnum):
    ACTIVE = 1
    PENDING = 2
    SCHEDULED = 3
    RETRY = 4
    ARCHIVED = 5
    COMPLETED = 6
    AGGREGATING = 7

    def __str__(self) -> str:
        return {1: "active", 2: "pending", 3: "scheduled", 4: "retry", 5: "archived", 6: "completed", 7: "aggregating"}.get(self.value, "unknown")

# ============================================================================
# TaskMessage
# ============================================================================

@dataclass
class TaskMessage:
    type: str = ""
    payload: bytes = b""
    id: str = ""
    queue: str = DEFAULT_QUEUE_NAME
    retry: int = DEFAULT_MAX_RETRY
    retried: int = 0
    error_msg: str = ""
    timeout: int = 0
    deadline: int = 0
    unique_key: str = ""
    last_failed_at: int = 0
    retention: int = 0
    completed_at: int = 0
    group_key: str = ""
    headers: dict = field(default_factory=dict)

    def to_json(self) -> str:
        import base64
        data = asdict(self)
        data["payload"] = base64.b64encode(self.payload).decode("ascii")
        return _json.dumps(data, ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str) -> "TaskMessage":
        import base64
        data = _json.loads(json_str)
        data["payload"] = base64.b64decode(data["payload"])
        return cls(**data)

    def to_dict(self) -> dict:
        import base64
        return {
            "type": self.type, "payload": base64.b64encode(self.payload).decode("ascii"),
            "id": self.id, "queue": self.queue, "retry": str(self.retry),
            "retried": str(self.retried), "error_msg": self.error_msg,
            "timeout": str(self.timeout), "deadline": str(self.deadline),
            "unique_key": self.unique_key, "last_failed_at": str(self.last_failed_at),
            "retention": str(self.retention), "completed_at": str(self.completed_at),
            "group_key": self.group_key, "headers": _json.dumps(self.headers, ensure_ascii=False),
        }

# ============================================================================
# ServerInfo
# ============================================================================

@dataclass
class ServerInfo:
    host: str = ""
    pid: int = 0
    server_id: str = ""
    concurrency: int = 0
    queues: dict = field(default_factory=dict)
    strict_priority: bool = False
    status: str = ""
    start_time: int = 0
    active_worker_count: int = 0
    routes: list = field(default_factory=list)

# ============================================================================
# Broker 接口
# ============================================================================

class Broker(ABC):
    @abstractmethod
    async def ping(self) -> bool: ...
    @abstractmethod
    async def close(self) -> None: ...
    @abstractmethod
    async def enqueue(self, msg: TaskMessage) -> str: ...
    @abstractmethod
    async def enqueue_unique(self, msg: TaskMessage, ttl: int) -> str: ...
    @abstractmethod
    async def schedule(self, msg: TaskMessage, process_at: int) -> str: ...
    @abstractmethod
    async def schedule_unique(self, msg: TaskMessage, process_at: int, ttl: int) -> str: ...
    @abstractmethod
    async def dequeue(self, task_routes: list[str], queues: list[str], strict_priority: bool, weights: Optional[dict[str, int]] = None) -> Optional[TaskMessage]: ...
    @abstractmethod
    async def done(self, msg: TaskMessage) -> None: ...
    @abstractmethod
    async def mark_as_complete(self, msg: TaskMessage) -> None: ...
    @abstractmethod
    async def retry(self, msg: TaskMessage, process_at: int, error_msg: str, is_failure: bool) -> None: ...
    @abstractmethod
    async def archive(self, msg: TaskMessage, error_msg: str) -> None: ...
    @abstractmethod
    async def forward_if_ready(self, task_routes: list[str], queues: list[str]) -> None: ...
    @abstractmethod
    async def write_server_state(self, info: ServerInfo, workers: list[dict[str, Any]], ttl: int) -> None: ...
    @abstractmethod
    async def clear_server_state(self, host: str, pid: int, server_id: str) -> None: ...
    @abstractmethod
    async def list_servers(self) -> list[ServerInfo]: ...
    @abstractmethod
    async def list_lease_expired(self, task_routes: list[str], queues: list[str]) -> list[TaskMessage]: ...
    @abstractmethod
    async def extend_lease(self, task_type: str, route: str, queue: str, task_ids: list[str], lease_seconds: int) -> None: ...
    @abstractmethod
    async def get_task_info(self, task_type: str, route: str, queue: str, task_id: str) -> Optional[dict[str, Any]]: ...
    @abstractmethod
    async def list_tasks(self, task_type: str, route: str, queue: str, state: TaskState, page_size: int = 100, page_num: int = 1) -> list[dict[str, Any]]: ...
    @abstractmethod
    async def list_queues(self) -> list[str]: ...
    @abstractmethod
    async def pause_queue(self, task_type: str, route: str, queue: str) -> None: ...
    @abstractmethod
    async def unpause_queue(self, task_type: str, route: str, queue: str) -> None: ...
    @abstractmethod
    async def delete_queue(self, task_type: str, route: str, queue: str, force: bool = False) -> None: ...
    @abstractmethod
    async def current_queue_stats(self, task_type: str, route: str, queue: str) -> dict[str, int]: ...
    @abstractmethod
    async def add_to_group(self, msg: TaskMessage, gname: str) -> str: ...
    @abstractmethod
    async def add_to_group_unique(self, msg: TaskMessage, gname: str, ttl: int) -> str: ...
    @abstractmethod
    async def list_groups(self, task_type: str, route: str, queue: str) -> list[str]: ...
    @abstractmethod
    async def aggregation_check(self, task_type: str, route: str, queue: str, gname: str, max_delay: int, max_size: int, grace_period: int) -> Optional[list[str]]: ...
    @abstractmethod
    async def write_scheduler_entries(self, entries: list[dict[str, Any]], ttl: int) -> None: ...
    @abstractmethod
    async def list_scheduler_entries(self) -> list[dict[str, Any]]: ...
    @abstractmethod
    async def record_scheduler_enqueue_event(self, entry_id: str, task_id: str) -> None: ...
