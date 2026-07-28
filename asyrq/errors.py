# errors.py — 错误类型定义
# 定义 asyrq 中使用的所有错误类型，1:1 对应 Go asynq 的 errors 包

from __future__ import annotations
class SkipRetry(Exception):
    """跳过重试的包装错误。在 Handler 中返回此错误表示任务不应重试。

    用法:
        return SkipRetry("参数无效")
        或
        raise SkipRetry("无效输入") from original_error
    """
    pass


class EnqueueError(Exception):
    """任务入队失败时抛出的错误。"""
    pass


class DequeueError(Exception):
    """任务出队失败时抛出的错误。"""
    pass


class TaskNotFoundError(Exception):
    """在 Redis 中未找到指定任务时抛出的错误。"""
    pass


class QueueNotFoundError(Exception):
    """在 Redis 中未找到指定队列时抛出的错误。"""
    pass
