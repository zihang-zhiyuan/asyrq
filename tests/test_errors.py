# tests/test_errors.py — 错误类型的单元测试

import pytest
from asyrq.errors import (
    SkipRetry,
    EnqueueError,
    DequeueError,
    TaskNotFoundError,
    QueueNotFoundError,
)


class TestErrors:
    """错误类型的单元测试。"""

    def test_skip_retry_is_exception(self):
        """测试 SkipRetry 是 Exception 的子类。"""
        err = SkipRetry("不应该重试")
        assert isinstance(err, Exception)
        assert str(err) == "不应该重试"

    def test_enqueue_error(self):
        """测试 EnqueueError。"""
        err = EnqueueError("入队失败")
        assert isinstance(err, Exception)
        assert str(err) == "入队失败"

    def test_task_not_found_error(self):
        """测试 TaskNotFoundError。"""
        err = TaskNotFoundError("任务未找到")
        assert isinstance(err, Exception)

    def test_queue_not_found_error(self):
        """测试 QueueNotFoundError。"""
        err = QueueNotFoundError("队列未找到")
        assert isinstance(err, Exception)
