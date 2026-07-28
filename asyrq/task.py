# task.py — Task 和 TaskInfo 模块
# 定义任务的核心数据结构，1:1 对应 Go asynq 的 Task 和 TaskInfo 类型

from __future__ import annotations
from typing import Any
import json as _json
import uuid as _uuid
from dataclasses import dataclass, field

from .internal.base import TaskState, DEFAULT_MAX_RETRY, DEFAULT_QUEUE_NAME, DEFAULT_TIMEOUT
from .options import Option, validate_options, apply_options

@dataclass
class TaskInfo:
    """任务的完整元数据信息。

    1:1 对应 Go asynq 的 TaskInfo 结构体。
    提供任务的不可变视图，包含所有状态和处理历史。
    """
    # 任务唯一标识符（UUID）
    id: str = ""
    # 任务所在的队列名
    queue: str = DEFAULT_QUEUE_NAME
    # 任务类型名称，用于路由到对应的 Handler
    type: str = ""
    # 任务负载数据（二进制）
    payload: bytes = field(default=b"", repr=False)
    # 任务当前状态
    state: TaskState = TaskState.PENDING
    # 最大重试次数
    max_retry: int = DEFAULT_MAX_RETRY
    # 已重试次数
    retried: int = 0
    # 最近一次失败的错误消息
    last_err: str = ""
    # 最近一次失败的时间（纳秒时间戳），0 表示从未失败
    last_failed_at: int = 0
    # 任务处理超时（秒），0 表示无超时
    timeout: int = DEFAULT_TIMEOUT
    # 任务截止时间（纳秒时间戳），0 表示无截止
    deadline: int = 0
    # 任务聚合组名，空字符串表示不属于任何组
    group: str = ""
    # 下次处理时间（纳秒时间戳），用于 scheduled/retry 状态
    next_process_at: int = 0
    # 是否为孤儿任务（原 worker 已死，由 recoverer 捡起）
    is_orphaned: bool = False
    # 完成后保留时长（秒），0 表示不保留
    retention: int = 0
    # 完成时间（纳秒时间戳），0 表示未完成
    completed_at: int = 0
    # 任务执行结果（二进制），仅在 completed 状态时有效
    result: bytes = field(default=b"", repr=False)
    # 任务附加请求头
    headers: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """将 TaskInfo 转为字典，用于序列化输出。"""
        return {
            "id": self.id,
            "queue": self.queue,
            "type": self.type,
            "payload": self.payload,
            "state": str(self.state),
            "max_retry": self.max_retry,
            "retried": self.retried,
            "last_err": self.last_err,
            "last_failed_at": self.last_failed_at,
            "timeout": self.timeout,
            "deadline": self.deadline,
            "group": self.group,
            "next_process_at": self.next_process_at,
            "is_orphaned": self.is_orphaned,
            "retention": self.retention,
            "completed_at": self.completed_at,
            "result": self.result,
            "headers": self.headers,
        }

class Task:
    """任务对象，表示一个将要被处理的工作单元。

    1:1 对应 Go asynq 的 Task 结构体。
    任务由类型名（用于路由）和负载数据（业务数据）组成。

    Usage:
        task = Task("email:send", json.dumps({"to": "a@b.com"}).encode())
        task = Task("image:resize", payload, Queue("critical"), MaxRetry(3))
    """

    def __init__(
        self,
        typename: str,
        payload: bytes = b"",
        *opts: Option,
        headers: Optional[dict[str, str] ]= None,
    ):
        """创建一个新任务实例。

        Args:
            typename: 任务类型名称，用于路由到对应的 Handler（如 "email:send"）
            payload: 任务的业务负载数据（二进制），默认空
            *opts: 任务配置选项（MaxRetry, Queue, Timeout 等）
            headers: 附加的请求头键值对

        Raises:
            ValueError: typename 为空时抛出
        """
        # 验证任务类型名不能为空
        if not typename:
            raise ValueError("任务类型名不能为空")

        # 任务类型标识符，用于 ServeMux 路由匹配
        self._typename = typename
        # 任务负载数据
        self._payload = payload
        # 任务配置选项列表
        self._opts: List[Option] = list(opts)
        # 任务附加请求头
        self._headers: Dict[str, str] = dict(headers) if headers else {}
        # 结果写入器引用（在处理时由 Server 设置）
        self._writer: Optional["ResultWriter"] = None

    @property
    def typename(self) -> str:
        """返回任务类型名称。"""
        return self._typename

    def type(self) -> str:
        """返回任务类型名称（兼容方法）。1:1 对应 Go asynq 的 Task.Type() 方法。"""
        return self._typename

    def payload(self) -> bytes:
        """返回任务负载数据。1:1 对应 Go asynq 的 Task.Payload() 方法。"""
        return self._payload

    def result_writer(self) -> Optional["ResultWriter"]:
        """返回关联的结果写入器。1:1 对应 Go asynq 的 Task.ResultWriter() 方法。

        仅在任务由 Server 处理后可用，Client 端创建时为 None。
        """
        return self._writer

    def set_result_writer(self, writer: "ResultWriter") -> None:
        """设置结果写入器（由 Server 内部调用）。"""
        self._writer = writer

    def options(self) -> List[Option]:
        """返回任务的所有配置选项。"""
        return list(self._opts)

    def headers(self) -> Dict[str, str]:
        """返回任务的附加请求头。"""
        return dict(self._headers)

    def __repr__(self) -> str:
        """返回任务的字符串表示。"""
        return f"Task(type={self._typename!r}, payload={len(self._payload)} bytes)"

    def __str__(self) -> str:
        """返回任务的可读描述。"""
        return f"Task[{self._typename}]"

def new_task(typename: str, payload: bytes = b"", *opts: Option, headers: Optional[dict[str, str] ]= None) -> Task:
    """创建新任务的工厂函数。

    1:1 对应 Go asynq 的 NewTask 函数。

    Args:
        typename: 任务类型名称
        payload: 负载数据
        *opts: 任务配置选项
        headers: 附加请求头

    Returns:
        Task: 新的任务实例
    """
    return Task(typename, payload, *opts, headers=headers)
