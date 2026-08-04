# asyrq 公开 API 导出
# 1:1 对应 Go asynq 包的所有公开类型和函数

# ---- 版本 ----
__version__ = "0.1.3"

# ---- 核心类型 ----
from .task import Task, TaskInfo, new_task
from .client import Client
from .server import Server, ServeMux, Config

# ---- 处理接口 ----
from .handler import Handler, HandlerFunc, Context

# ---- 中间件 ----
from .middleware import MiddlewareFunc

# ---- 选项 ----
from .options import (
    Option,
    MaxRetry,
    Queue,
    TaskID,
    Timeout,
    Deadline,
    Unique,
    ProcessAt,
    ProcessIn,
    Retention,
    Group,
    ResultKey,
)

# ---- 连接配置 ----
from .connection import (
    RedisClientOpt,
    RedisFailoverClientOpt,
    RedisClusterClientOpt,
    parse_redis_uri,
)

# ---- 结果写入 ----
from .result_writer import ResultWriter

# ---- 错误类型 ----
from .errors import (
    SkipRetry,
    EnqueueError,
    DequeueError,
    TaskNotFoundError,
    QueueNotFoundError,
)

# ---- 调度器 ----
from .scheduler import Scheduler, SchedulerOpts

# ---- 内部常量 ----
from .internal.base import (
    TaskState,
    TaskMessage,
    DEFAULT_QUEUE_NAME,
    DEFAULT_MAX_RETRY,
    DEFAULT_TIMEOUT,
    DEFAULT_SHUTDOWN_TIMEOUT,
    ASYNQ_VERSION,
)

# ---- 日志 ----
from .internal.log import Logger, LogLevel, DefaultLogger


# ============================================================================
# 公开 API 摘要
# ============================================================================
__all__ = [
    # 版本
    "__version__",

    # 核心
    "Task", "TaskInfo", "new_task",
    "Client", "Server", "ServeMux", "Config",

    # 处理
    "Handler", "HandlerFunc", "Context",

    # 中间件
    "MiddlewareFunc",

    # 选项
    "Option", "MaxRetry", "Queue", "TaskID",
    "Timeout", "Deadline", "Unique",
    "ProcessAt", "ProcessIn", "Retention", "Group",
    "ResultKey",

    # 连接
    "RedisClientOpt", "RedisFailoverClientOpt", "RedisClusterClientOpt",
    "parse_redis_uri",

    # 结果
    "ResultWriter",

    # 错误
    "SkipRetry", "EnqueueError", "DequeueError",
    "TaskNotFoundError", "QueueNotFoundError",

    # 调度器
    "Scheduler", "SchedulerOpts",

    # 内部
    "TaskState", "TaskMessage",
    "DEFAULT_QUEUE_NAME", "DEFAULT_MAX_RETRY",
    "DEFAULT_TIMEOUT", "DEFAULT_SHUTDOWN_TIMEOUT",
    "ASYNQ_VERSION",

    # 日志
    "Logger", "LogLevel", "DefaultLogger",
]
