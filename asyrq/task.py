# task.py - Task 和 TaskInfo
from __future__ import annotations

import time as _time
from typing import Any, Optional
from dataclasses import dataclass, field

from .internal.base import TaskState, DEFAULT_MAX_RETRY, DEFAULT_QUEUE_NAME, DEFAULT_TIMEOUT
from .options import Option


@dataclass
class TaskInfo:
    """任务入队后的元数据。"""
    id: str = ""
    queue: str = DEFAULT_QUEUE_NAME
    type: str = ""
    payload: bytes = field(default=b"", repr=False)
    state: TaskState = TaskState.PENDING
    max_retry: int = DEFAULT_MAX_RETRY
    retried: int = 0
    last_err: str = ""
    last_failed_at: int = 0
    timeout: int = DEFAULT_TIMEOUT
    deadline: int = 0
    group: str = ""
    next_process_at: int = 0
    is_orphaned: bool = False
    retention: int = 0
    completed_at: int = 0
    result: bytes = field(default=b"", repr=False)
    headers: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id, "queue": self.queue, "type": self.type,
            "payload": self.payload, "state": str(self.state),
            "max_retry": self.max_retry, "retried": self.retried,
            "last_err": self.last_err, "last_failed_at": self.last_failed_at,
            "timeout": self.timeout, "deadline": self.deadline,
            "group": self.group, "next_process_at": self.next_process_at,
            "is_orphaned": self.is_orphaned, "retention": self.retention,
            "completed_at": self.completed_at, "result": self.result,
            "headers": self.headers,
        }


class Task:
    """任务对象。

    Pythonic API:
        task = Task("email:send", payload)
        task.type        # 属性访问
        task.payload     # 属性访问
        task.id          # 任务 ID（处理时由 Server 设置）
        task.queue       # 队列名
        task.timeout     # 超时秒数
        task.remaining   # 剩余时间
        task.cancelled   # 是否取消

    兼容旧 API:
        task.type()      # 仍可用，但建议改用 task.type
        task.payload()   # 仍可用
    """

    def __init__(
        self,
        typename: str,
        payload: bytes = b"",
        *opts: Option,
        headers: Optional[dict[str, str]] = None,
    ):
        if not typename:
            raise ValueError("任务类型名不能为空")

        self._typename = typename
        self._payload = payload
        self._opts: list[Option] = list(opts)
        self._headers: dict[str, str] = dict(headers) if headers else {}
        self._writer = None

        # 运行时上下文（原 Context 的功能，处理时由 Server 注入）
        self._id: str = ""
        self._queue: str = ""
        self._timeout: int = 0
        self._deadline: int = 0
        self._start_ns: int = 0
        self._cancelled: bool = False

    # ======== 属性访问（Pythonic）========

    @property
    def typename(self) -> str:
        """任务类型名。"""
        return self._typename

    @property
    def type(self) -> str:
        """任务类型名（别名）。"""
        return self._typename

    @property
    def payload(self) -> bytes:
        """负载数据。"""
        return self._payload

    @property
    def headers(self) -> dict[str, str]:
        """请求头。"""
        return dict(self._headers)

    @property
    def options(self) -> list[Option]:
        """任务选项。"""
        return list(self._opts)

    @property
    def id(self) -> str:
        """任务 ID（处理时由 Server 设置）。"""
        return self._id

    @property
    def queue(self) -> str:
        """队列名。"""
        return self._queue

    @property
    def timeout(self) -> int:
        """超时秒数。"""
        return self._timeout

    @property
    def deadline(self) -> int:
        """截止时间戳（纳秒）。"""
        return self._deadline

    @property
    def cancelled(self) -> bool:
        """是否已被取消。"""
        return self._cancelled

    @property
    def remaining(self) -> int:
        """剩余处理时间（秒），无超时返回 -1。"""
        if self._timeout <= 0:
            return -1
        if self._start_ns == 0:
            return self._timeout
        elapsed = (int(_time.time() * 1e9) - self._start_ns) // 1_000_000_000
        return max(0, self._timeout - elapsed)

    # ======== 结果写入 ========

    def result_writer(self):
        """返回结果写入器。"""
        return self._writer

    def _set_writer(self, writer) -> None:
        self._writer = writer

    async def update_state(self, data: dict) -> None:
        """上报中间状态（立即写 Redis）。"""
        if self._writer:
            await self._writer.update_state(data)

    async def finish(self, data: dict) -> None:
        """写入最终结果。"""
        if self._writer:
            await self._writer.finish(data)

    # ======== 兼容旧 API（方法调用）========

    def type_method(self) -> str:
        """[已废弃] 用 task.type 代替。"""
        return self._typename

    def payload_method(self) -> bytes:
        """[已废弃] 用 task.payload 代替。"""
        return self._payload

    def headers_method(self) -> dict[str, str]:
        """[已废弃] 用 task.headers 代替。"""
        return dict(self._headers)

    def options_method(self) -> list[Option]:
        """[已废弃] 用 task.options 代替。"""
        return list(self._opts)

    # ======== 内部方法（Server 使用）========

    def _setup_context(self, task_id: str, queue: str, timeout: int, deadline: int, start_ns: int):
        """Server 内部调用：注入运行时上下文。"""
        self._id = task_id
        self._queue = queue
        self._timeout = timeout
        self._deadline = deadline
        self._start_ns = start_ns

    def _cancel(self) -> None:
        """取消任务。"""
        self._cancelled = True

    def __repr__(self) -> str:
        return f"Task(type={self._typename!r}, payload={len(self._payload)} bytes)"

    def __str__(self) -> str:
        return f"Task[{self._typename}]"


def new_task(typename: str, payload: bytes = b"", *opts: Option, headers: Optional[dict[str, str]] = None) -> Task:
    """创建新任务。"""
    return Task(typename, payload, *opts, headers=headers)
