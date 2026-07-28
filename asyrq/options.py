# options.py — 任务配置选项模块
# 定义所有任务级别的配置选项，1:1 对应 Go asynq 的 Option 接口及具体实现

from __future__ import annotations
from dataclasses import dataclass

from .internal import timeutil as _timeutil
from .internal.base import DEFAULT_MAX_RETRY, DEFAULT_QUEUE_NAME, DEFAULT_TIMEOUT


class Option:
    """任务配置选项基类。

    1:1 对应 Go asynq 的 Option 接口。
    所有任务级别的配置都通过 Option 子类传递给 Client.Enqueue。
    """

    def type(self) -> str:
        """返回选项类型名称。"""
        return self.__class__.__name__

    def value(self):
        """返回选项的值。"""
        return None


class MaxRetry(Option):
    """设置任务的最大重试次数。

    任务处理失败后会自动重试，达到最大次数后归档。
    默认最大重试次数为 25。

    1:1 对应 Go asynq 的 MaxRetry 选项。
    """
    def __init__(self, n: int):
        """设置最大重试次数。

        Args:
            n: 最大重试次数，负数会被限制为 0（不重试）
        """
        self._n = max(0, n)  # 确保重试次数不为负数

    def max_retry_count(self) -> int:
        """返回最大重试次数。"""
        return self._n


class Queue(Option):
    """设置任务的队列名称。

    可以将任务路由到不同的队列，实现优先级分离。
    默认队列名为 "default"。

    1:1 对应 Go asynq 的 Queue 选项。
    """
    def __init__(self, name: str):
        """设置队列名称。

        Args:
            name: 队列名称（大小写不敏感，会被转为小写）
        """
        self._name = name.lower()  # 队列名统一为小写

    def queue_name(self) -> str:
        """返回队列名称。"""
        return self._name


class TaskID(Option):
    """设置自定义任务 ID。

    可用于幂等地创建任务（具有相同 ID 的任务不会重复创建）。

    1:1 对应 Go asynq 的 TaskID 选项。
    """
    def __init__(self, task_id: str):
        """设置自定义任务 ID。

        Args:
            task_id: 自定义的任务唯一标识符
        """
        self._task_id = task_id

    def task_id(self) -> str:
        """返回自定义任务 ID。"""
        return self._task_id


class Timeout(Option):
    """设置任务的处理超时时间。

    当任务处理时间超过此值时，任务会被取消并重新入队。
    默认超时时间为 30 分钟（1800 秒）。

    1:1 对应 Go asynq 的 Timeout 选项。
    """
    def __init__(self, timeout_secs: int):
        """设置超时时间。

        Args:
            timeout_secs: 超时秒数，0 表示无超时
        """
        self._timeout = max(0, timeout_secs)  # 确保非负

    def timeout_secs(self) -> int:
        """返回超时秒数。"""
        return self._timeout


class Deadline(Option):
    """设置任务的绝对截止时间。

    超时后任务会被取消，与 Timeout 不同的是此选项使用绝对时间。

    1:1 对应 Go asynq 的 Deadline 选项。
    """
    def __init__(self, deadline_nsec: int):
        """设置截止时间。

        Args:
            deadline_nsec: 截止的纳秒级 Unix 时间戳
        """
        self._deadline = deadline_nsec

    def deadline_nsec(self) -> int:
        """返回截止时间戳。"""
        return self._deadline


class Unique(Option):
    """设置任务唯一性锁。

    具有相同类型+负载+队列的任务在 TTL 内只会创建一次。
    返回 ErrDuplicateTask 表示任务已存在且被跳过。

    1:1 对应 Go asynq 的 Unique 选项。
    """
    def __init__(self, ttl_secs: int):
        """设置唯一性 TTL。

        Args:
            ttl_secs: 唯一锁的 TTL（秒），最小值 1 秒
        """
        self._ttl = max(1, ttl_secs)  # 最小 TTL 为 1 秒

    def ttl_secs(self) -> int:
        """返回唯一锁 TTL 秒数。"""
        return self._ttl


class ProcessAt(Option):
    """设置任务在指定绝对时间执行。

    1:1 对应 Go asynq 的 ProcessAt 选项。
    """
    def __init__(self, process_at_nsec: int):
        """设置执行时间。

        Args:
            process_at_nsec: 计划执行时间的纳秒级 Unix 时间戳
        """
        self._process_at = process_at_nsec

    def process_at_nsec(self) -> int:
        """返回计划执行时间戳。"""
        return self._process_at


class ProcessIn(Option):
    """设置任务在指定相对时间后执行。

    1:1 对应 Go asynq 的 ProcessIn 选项。
    """
    def __init__(self, delay_secs: int):
        """设置延迟执行时间。

        Args:
            delay_secs: 延迟秒数
        """
        # 计算绝对执行时间 = 当前时间 + 延迟时间
        self._process_at = _timeutil.now() + delay_secs * 1_000_000_000

    def process_at_nsec(self) -> int:
        """返回计划执行时间戳。"""
        return self._process_at


class Retention(Option):
    """设置任务完成后的保留时间。

    任务完成后在 Redis 中保留指定时长，可用于结果查询。

    1:1 对应 Go asynq 的 Retention 选项。
    """
    def __init__(self, retention_secs: int):
        """设置保留时间。

        Args:
            retention_secs: 保留秒数，0 表示不保留（完成后立即删除）
        """
        self._retention = max(0, retention_secs)  # 确保非负

    def retention_secs(self) -> int:
        """返回保留秒数。"""
        return self._retention


class Group(Option):
    """设置任务聚合组名。

    相同组的任务会被聚合为一个批量任务处理，适用于需要攒批的场景。

    1:1 对应 Go asynq 的 Group 选项。
    """
    def __init__(self, name: str):
        """设置聚合组名。

        Args:
            name: 聚合组名称
        """
        self._name = name

    def group_name(self) -> str:
        """返回聚合组名。"""
        return self._name


# ============================================================================
# 选项解析工具函数
# ============================================================================

def validate_options(opts: List[Option]) -> None:
    """验证选项列表的有效性，检查冲突和不兼容的选项。

    Args:
        opts: 要验证的选项列表

    Raises:
        ValueError: 当存在冲突的选项时
    """
    has_process_at = False  # 标记是否有 ProcessAt
    has_process_in = False  # 标记是否有 ProcessIn
    for opt in opts:
        if isinstance(opt, ProcessAt):
            if has_process_at or has_process_in:
                raise ValueError("ProcessAt 和 ProcessIn 不能同时使用")
            has_process_at = True
        elif isinstance(opt, ProcessIn):
            if has_process_at or has_process_in:
                raise ValueError("ProcessAt 和 ProcessIn 不能同时使用")
            has_process_in = True


def apply_options(opts: List[Option]) -> dict:
    """解析选项列表并提取配置参数。

    遍历所有选项，提取队列名、重试次数、超时等配置，
    返回一个统一的配置字典。

    Args:
        opts: 任务选项列表

    Returns:
        dict: 包含 queue, retry, timeout, deadline, unique_ttl, process_at, retention, group, task_id 的字典
    """
    # 初始化默认配置
    config = {
        "queue": DEFAULT_QUEUE_NAME,         # 默认队列
        "retry": DEFAULT_MAX_RETRY,          # 默认最大重试次数
        "timeout": DEFAULT_TIMEOUT,          # 默认超时（0 = 无）
        "deadline": 0,                       # 默认无截止时间
        "unique_ttl": 0,                     # 默认不启用唯一性
        "process_at": 0,                     # 默认立即执行
        "retention": 0,                      # 默认不保留
        "group": "",                         # 默认不分组
        "task_id": "",                       # 默认自动生成 ID
    }

    # 遍历所有选项，提取配置值
    for opt in opts:
        if isinstance(opt, Queue):
            config["queue"] = opt.queue_name()  # 队列名
        elif isinstance(opt, MaxRetry):
            config["retry"] = opt.max_retry_count()  # 重试次数
        elif isinstance(opt, Timeout):
            config["timeout"] = opt.timeout_secs()  # 超时秒数
        elif isinstance(opt, Deadline):
            config["deadline"] = opt.deadline_nsec()  # 截止时间戳
        elif isinstance(opt, Unique):
            config["unique_ttl"] = opt.ttl_secs()  # 唯一性 TTL
        elif isinstance(opt, ProcessAt):
            config["process_at"] = opt.process_at_nsec()  # 计划执行时间
        elif isinstance(opt, ProcessIn):
            config["process_at"] = opt.process_at_nsec()  # 延迟执行时间
        elif isinstance(opt, Retention):
            config["retention"] = opt.retention_secs()  # 保留时间
        elif isinstance(opt, Group):
            config["group"] = opt.group_name()  # 聚合组名
        elif isinstance(opt, TaskID):
            config["task_id"] = opt.task_id()  # 自定义任务 ID

    return config  # 返回解析后的配置字典
