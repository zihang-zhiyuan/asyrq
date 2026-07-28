# internal/base.py — 基础模块
# 定义 TaskMessage、TaskState、Broker 接口和所有 Redis Key 常量
# 1:1 对应 Go asynq 的 internal/base 包

from __future__ import annotations
from typing import Any
import json as _json
import uuid as _uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from enum import IntEnum


# ============================================================================
# 包级常量 — 1:1 对应 Go asynq 的顶层常量
# ============================================================================

# 默认队列名称，当用户未指定队列时使用
DEFAULT_QUEUE_NAME: str = "default"

# 默认最大重试次数，与 Go asynq 保持一致
DEFAULT_MAX_RETRY: int = 25

# 默认任务超时时间（秒），0 表示无超时
DEFAULT_TIMEOUT: int = 0

# 默认任务保留时间（秒），任务完成后在 Redis 中保留的时长
DEFAULT_RETENTION: int = 0

# 默认关闭超时时间（秒），服务器优雅关闭的最大等待时间
DEFAULT_SHUTDOWN_TIMEOUT: int = 8

# 服务器心跳间隔（秒），定期向 Redis 写入服务器状态
SERVER_HEARTBEAT_INTERVAL: int = 5

# 租约时长（秒），任务从 active 到被认为超时的时长
LEASE_DURATION: int = 30

# 任务组聚合的最长延迟（秒）
DEFAULT_GROUP_MAX_DELAY: int = 30

# 任务组聚合的宽限期（秒）
DEFAULT_GROUP_GRACE_PERIOD: int = 60

# 任务组聚合的最大任务数
DEFAULT_GROUP_MAX_SIZE: int = 20

# 队列暂停状态的 TTL（秒），用于暂停检测
QUEUE_PAUSED_TTL: int = 10

# 任务唯一键的锁定时长（秒），防止唯一任务的竞态条件
UNIQUE_LOCK_TTL: int = 60

# Asynq 协议版本号
ASYNQ_VERSION: str = "0.1.0"

# ============================================================================
# Redis Key 生成函数 — 1:1 对应 Go asynq 的内部 key 构建逻辑
# ============================================================================
# Go asynq 使用 asynq:{queue}:t:{task_id} 格式（带 hash tag {queue} 用于 Redis Cluster）
# 确保同一队列的所有 key 都路由到同一个 Redis Cluster slot
# 我们同样使用 {qname} 来保持兼容性

def pending_key(qname: str) -> str:
    """生成待处理队列的 Redis Key。asynq:{qname}:pending"""
    return f"asynq:{{{qname}}}:pending"

def active_key(qname: str) -> str:
    """生成活动任务列表的 Redis Key。asynq:{qname}:active"""
    return f"asynq:{{{qname}}}:active"

def scheduled_key(qname: str) -> str:
    """生成定时任务 ZSet 的 Redis Key。asynq:{qname}:scheduled"""
    return f"asynq:{{{qname}}}:scheduled"

def retry_key(qname: str) -> str:
    """生成重试任务 ZSet 的 Redis Key。asynq:{qname}:retry"""
    return f"asynq:{{{qname}}}:retry"

def archived_key(qname: str) -> str:
    """生成已归档任务 ZSet 的 Redis Key。asynq:{qname}:archived"""
    return f"asynq:{{{qname}}}:archived"

def completed_key(qname: str) -> str:
    """生成已完成任务 ZSet 的 Redis Key。asynq:{qname}:completed"""
    return f"asynq:{{{qname}}}:completed"

def task_key(qname: str, task_id: str) -> str:
    """生成任务数据的 Hash Key。asynq:{qname}:t:{task_id}"""
    return f"asynq:{{{qname}}}:t:{task_id}"

def paused_key(qname: str) -> str:
    """生成队列暂停状态的 Hash Key。asynq:{qname}:paused"""
    return f"asynq:{{{qname}}}:paused"

def lease_key(qname: str) -> str:
    """生成任务租约信息的 Hash Key。asynq:{qname}:lease"""
    return f"asynq:{{{qname}}}:lease"

def group_key(qname: str, gname: str) -> str:
    """生成任务组聚合的 Set Key。asynq:{qname}:aggregation:{gname}"""
    return f"asynq:{{{qname}}}:aggregation:{gname}"

def all_groups_key(qname: str) -> str:
    """生成所有任务组的 Hash Key。asynq:{qname}:groups"""
    return f"asynq:{{{qname}}}:groups"

def all_queues_key() -> str:
    """生成全局所有队列名的 Set Key。asynq:queues"""
    return "asynq:queues"

def servers_key() -> str:
    """生成全局服务器心跳的 ZSet Key。asynq:servers"""
    return "asynq:servers"

def workers_key(hostname: str, pid: int, server_id: str) -> str:
    """生成工作协程状态的 ZSet Key。asynq:workers:{hostname}:{pid}:{server_id}"""
    return f"asynq:workers:{{{hostname}:{pid}:{server_id}}}"

def scheduler_key(entry_id: str) -> str:
    """生成调度器条目的 Key。asynq:schedulers:{entry_id}"""
    return f"asynq:schedulers:{{{entry_id}}}"

def scheduler_history_key(entry_id: str) -> str:
    """生成调度器执行历史的 Key。asynq:scheduler_history:{entry_id}"""
    return f"asynq:scheduler_history:{entry_id}"

def unique_key(qname: str, task_type: str, payload: bytes) -> str:
    """生成任务唯一键，用于去重。asynq:{qname}:unique:{task_type}:{payload_hash}"""
    import hashlib
    # 使用 SHA256 计算负载哈希（与 Go asynq 保持一致）
    payload_hash = hashlib.sha256(payload).hexdigest()  # 计算负载的 SHA256 哈希
    return f"asynq:{{{qname}}}:unique:{task_type}:{payload_hash}"

# ============================================================================
# 常量别名 — 供 rdb.py 等模块使用的便捷引用
# ============================================================================

# 全局队列集合 key 常量
ALL_QUEUES_KEY: str = all_queues_key()      # "asynq:queues"

# 全局服务器 ZSet key 常量
SERVERS_KEY: str = servers_key()            # "asynq:servers"

# ============================================================================
# TaskState 枚举 — 任务可能处于的状态
# ============================================================================

class TaskState(IntEnum):
    """任务状态枚举，1:1 对应 Go asynq 的 TaskState。"""
    ACTIVE = 1        # 正在被某个 worker 处理中
    PENDING = 2       # 在待处理队列中等待被获取
    SCHEDULED = 3     # 等待到达预定执行时间
    RETRY = 4         # 处理失败，等待重试
    ARCHIVED = 5      # 重试次数耗尽，已归档
    COMPLETED = 6     # 已成功处理完成
    AGGREGATING = 7   # 正在任务组中聚合

    def __str__(self) -> str:
        """返回任务状态的可读字符串表示。"""
        names = {
            1: "active",
            2: "pending",
            3: "scheduled",
            4: "retry",
            5: "archived",
            6: "completed",
            7: "aggregating",
        }
        return names.get(self.value, "unknown")

# ============================================================================
# TaskMessage — 在 Redis 中存储的任务消息
# ============================================================================

@dataclass
class TaskMessage:
    """任务消息体，序列化后存储在 Redis Hash 的 'msg' 字段中。

    1:1 对应 Go asynq 的 TaskMessage protobuf 结构体。
    字段名使用蛇形命名（Python 惯例），序列化时转为 JSON。
    """
    type: str = ""                      # 任务类型标识符，用于路由到正确的 Handler
    payload: bytes = b""                # 任务的负载数据（二进制）
    id: str = ""                        # 任务唯一标识符（UUID）
    queue: str = DEFAULT_QUEUE_NAME     # 目标队列名称
    retry: int = DEFAULT_MAX_RETRY      # 最大重试次数
    retried: int = 0                    # 已重试次数
    error_msg: str = ""                 # 最近一次失败的错误消息
    timeout: int = 0                    # 任务处理超时时间（秒），0 表示无超时
    deadline: int = 0                   # 任务处理截止时间（纳秒时间戳），0 表示无截止
    unique_key: str = ""                # 任务唯一键，用于去重
    last_failed_at: int = 0             # 最近一次失败的时间（纳秒时间戳）
    retention: int = 0                  # 完成后保留时长（秒）
    completed_at: int = 0               # 完成时间（纳秒时间戳）
    group_key: str = ""                 # 任务组键，任务属于的聚合组名
    headers: Dict[str, str] = field(default_factory=dict)  # 附加请求头（键值对）

    def to_json(self) -> str:
        """将 TaskMessage 序列化为 JSON 字符串。

        Returns:
            str: JSON 格式的消息字符串
        """
        data = asdict(self)  # 将 dataclass 转为字典
        # 将 bytes 类型的 payload 转为 base64 编码字符串
        import base64
        data["payload"] = base64.b64encode(self.payload).decode("ascii")
        return _json.dumps(data, ensure_ascii=False)  # 序列化为 JSON

    @classmethod
    def from_json(cls, json_str: str) -> "TaskMessage":
        """从 JSON 字符串反序列化为 TaskMessage。

        Args:
            json_str: JSON 格式的消息字符串

        Returns:
            TaskMessage: 反序列化后的消息对象
        """
        import base64
        data = _json.loads(json_str)  # 解析 JSON
        # 将 base64 编码的 payload 还原为 bytes
        data["payload"] = base64.b64decode(data["payload"])
        return cls(**data)  # 使用解析后的数据创建 TaskMessage 实例

    def to_dict(self) -> Dict[str, Any]:
        """将 TaskMessage 转为普通字典（用于 Redis Hash 存储）。

        payload 字段转为 base64 编码字符串，headers 转为 JSON 字符串。

        Returns:
            dict[str, Any]: 所有字段都是字符串或数字的字典
        """
        import base64
        return {
            "type": self.type,
            "payload": base64.b64encode(self.payload).decode("ascii"),
            "id": self.id,
            "queue": self.queue,
            "retry": str(self.retry),
            "retried": str(self.retried),
            "error_msg": self.error_msg,
            "timeout": str(self.timeout),
            "deadline": str(self.deadline),
            "unique_key": self.unique_key,
            "last_failed_at": str(self.last_failed_at),
            "retention": str(self.retention),
            "completed_at": str(self.completed_at),
            "group_key": self.group_key,
            "headers": _json.dumps(self.headers, ensure_ascii=False),
        }

# ============================================================================
# ServerInfo — 服务器状态信息
# ============================================================================

@dataclass
class ServerInfo:
    """服务器信息，用于心跳写入和服务器发现。

    1:1 对应 Go asynq 的 ServerInfo 结构体。
    """
    host: str = ""            # 服务器主机名
    pid: int = 0              # 进程 ID
    server_id: str = ""       # 服务器唯一 ID
    concurrency: int = 0      # 最大并发处理数
    queues: Dict[str, int] = field(default_factory=dict)  # 队列名→优先级映射
    strict_priority: bool = False  # 是否启用严格优先级模式
    status: str = ""          # 服务器状态（"active", "quiet", "stopped"）
    start_time: int = 0       # 服务器启动时间（纳秒时间戳）
    active_worker_count: int = 0  # 当前活跃 worker 数量

# ============================================================================
# Broker 接口 — Redis 操作的抽象层
# ============================================================================

class Broker(ABC):
    """Broker 抽象接口，定义所有 Redis 操作的契约。

    1:1 对应 Go asynq 的 internal/base.Broker 接口。
    使用抽象基类确保所有 Redis 操作都有统一的接口，便于测试和替换。
    """

    # --- 连接管理 ---

    @abstractmethod
    async def ping(self) -> bool:
        """检查 Redis 连接是否正常。"""
        ...

    @abstractmethod
    async def close(self) -> None:
        """关闭 Redis 连接。"""
        ...

    # --- 任务入队操作 ---

    @abstractmethod
    async def enqueue(self, msg: TaskMessage) -> str:
        """将任务消息入队到 pending 队列。

        Args:
            msg: 要入队的任务消息

        Returns:
            str: 任务的唯一 ID
        """
        ...

    @abstractmethod
    async def enqueue_unique(self, msg: TaskMessage, ttl: int) -> str:
        """唯一地入队任务消息（如果已存在则跳过）。

        Args:
            msg: 要入队的任务消息
            ttl: 唯一键的 TTL（秒）

        Returns:
            str: 任务的唯一 ID（如果是新任务），或空字符串（如果任务已存在）
        """
        ...

    @abstractmethod
    async def schedule(self, msg: TaskMessage, process_at: int) -> str:
        """将任务调度到未来某个时间执行。

        Args:
            msg: 要调度的任务消息
            process_at: 计划执行时间（纳秒时间戳）

        Returns:
            str: 任务的唯一 ID
        """
        ...

    @abstractmethod
    async def schedule_unique(self, msg: TaskMessage, process_at: int, ttl: int) -> str:
        """唯一地调度定时任务。

        Args:
            msg: 要调度的任务消息
            process_at: 计划执行时间（纳秒时间戳）
            ttl: 唯一键的 TTL（秒）

        Returns:
            str: 任务的唯一 ID（如果是新任务），或空字符串（如果任务已存在）
        """
        ...

    # --- 任务出队操作 ---

    @abstractmethod
    async def dequeue(self, queues: List[str], strict_priority: bool) -> Optional[TaskMessage]:
        """从队列中取出一个待处理任务。

        Args:
            queues: 按优先级排序的队列名列表
            strict_priority: 是否使用严格优先级模式

        Returns:
            Optional[TaskMessage]: 出队的任务消息，无任务时返回 None
        """
        ...

    # --- 任务生命周期操作 ---

    @abstractmethod
    async def done(self, msg: TaskMessage) -> None:
        """标记任务为已完成。

        Args:
            msg: 已完成的任务消息
        """
        ...

    @abstractmethod
    async def mark_as_complete(self, msg: TaskMessage) -> None:
        """将任务移入已完成集合。

        Args:
            msg: 要标记为完成的任务消息
        """
        ...

    @abstractmethod
    async def retry(self, msg: TaskMessage, process_at: int, error_msg: str, is_failure: bool) -> None:
        """将任务标记为重试状态。

        Args:
            msg: 要重试的任务消息
            process_at: 下次重试时间（纳秒时间戳）
            error_msg: 错误消息
            is_failure: 是否为失败（False 表示重试次数未耗尽）
        """
        ...

    @abstractmethod
    async def archive(self, msg: TaskMessage, error_msg: str) -> None:
        """将任务归档（重试次数耗尽后）。

        Args:
            msg: 要归档的任务消息
            error_msg: 错误消息
        """
        ...

    @abstractmethod
    async def forward_if_ready(self, queues: List[str]) -> None:
        """检查 scheduled 和 retry 集合，将到期任务移到 pending 队列。"""
        ...

    # --- 任务查询操作 ---

    @abstractmethod
    async def get_task_info(self, qname: str, task_id: str) -> Optional[dict[str, Any]]:
        """获取指定任务的详细信息。

        Args:
            qname: 队列名
            task_id: 任务 ID

        Returns:
            Optional[dict[str, Any]]: 任务信息字典，不存在时返回 None
        """
        ...

    @abstractmethod
    async def list_tasks(self, qname: str, state: TaskState, page_size: int = 100, page_num: int = 1) -> List[dict[str, Any]]:
        """列出指定状态的 tasks。"""
        ...

    # --- 队列管理操作 ---

    @abstractmethod
    async def list_queues(self) -> List[str]:
        """列出所有队列名称。"""
        ...

    @abstractmethod
    async def pause_queue(self, qname: str) -> None:
        """暂停指定队列。"""
        ...

    @abstractmethod
    async def unpause_queue(self, qname: str) -> None:
        """恢复指定队列。"""
        ...

    @abstractmethod
    async def delete_queue(self, qname: str, force: bool = False) -> None:
        """删除队列及其所有任务。"""
        ...

    @abstractmethod
    async def current_queue_stats(self, qname: str) -> Dict[str, int]:
        """获取队列统计信息。"""
        ...

    # --- 服务器管理操作 ---

    @abstractmethod
    async def write_server_state(self, info: ServerInfo, workers: List[dict[str, Any]], ttl: int) -> None:
        """写入服务器状态和 worker 信息到 Redis（心跳）。

        Args:
            info: 服务器信息
            workers: worker 信息列表
            ttl: 心跳数据的 TTL（秒）
        """
        ...

    @abstractmethod
    async def clear_server_state(self, host: str, pid: int, server_id: str) -> None:
        """清除服务器状态。

        Args:
            host: 主机名
            pid: 进程 ID
            server_id: 服务器唯一 ID
        """
        ...

    @abstractmethod
    async def list_servers(self) -> List[ServerInfo]:
        """列出所有活跃的服务器。"""
        ...

    # --- 任务组聚合操作 ---

    @abstractmethod
    async def add_to_group(self, msg: TaskMessage, gname: str) -> str:
        """将任务添加到聚合组。

        Args:
            msg: 任务消息
            gname: 组名

        Returns:
            str: 任务 ID
        """
        ...

    @abstractmethod
    async def add_to_group_unique(self, msg: TaskMessage, gname: str, ttl: int) -> str:
        """唯一地将任务添加到聚合组。

        Args:
            msg: 任务消息
            gname: 组名
            ttl: 唯一键的 TTL

        Returns:
            str: 任务 ID（如果是新任务），或空字符串
        """
        ...

    @abstractmethod
    async def list_groups(self, qname: str) -> List[str]:
        """列出队列中的所有聚合组。"""
        ...

    @abstractmethod
    async def aggregation_check(self, qname: str, gname: str, max_delay: int, max_size: int, grace_period: int) -> Optional[str]:
        """检查聚合组是否应该被触发。

        Returns:
            Optional[str]: 要触发的任务 ID，不需要触发时返回 None
        """
        ...

    # --- 租约管理 ---

    @abstractmethod
    async def list_lease_expired(self, queues: List[str]) -> List[TaskMessage]:
        """列出租约过期的活动任务（故障恢复用）。"""
        ...

    @abstractmethod
    async def extend_lease(self, qname: str, task_ids: List[str], lease_seconds: int) -> None:
        """延长任务的活动租约。

        Args:
            qname: 队列名
            task_ids: 需要延长租约的任务 ID 列表
            lease_seconds: 租约延长秒数
        """
        ...

    # --- 调度器操作 ---

    @abstractmethod
    async def write_scheduler_entries(self, entries: List[dict[str, Any]], ttl: int) -> None:
        """写入调度器条目到 Redis。"""
        ...

    @abstractmethod
    async def list_scheduler_entries(self) -> List[dict[str, Any]]:
        """列出所有调度器条目。"""
        ...

    @abstractmethod
    async def record_scheduler_enqueue_event(self, entry_id: str, task_id: str) -> None:
        """记录调度器的入队事件。"""
        ...
