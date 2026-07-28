# tests/test_task.py — Task 和 Option 的单元测试

import pytest
from asyrq import (
    Task, new_task, TaskInfo, TaskState,
    MaxRetry, Queue, Timeout, Unique, ProcessIn, Retention, Group,
)


class TestTask:
    """Task 类型的单元测试。"""

    def test_create_task_basic(self):
        """测试创建基本任务。"""
        task = Task("email:send", b'{"to": "test@test.com"}')
        assert task.type() == "email:send"
        assert task.payload() == b'{"to": "test@test.com"}'
        assert task.result_writer() is None

    def test_create_task_with_empty_typename_raises(self):
        """测试空类型名应该抛出 ValueError。"""
        with pytest.raises(ValueError, match="任务类型名不能为空"):
            Task("")

    def test_new_task_factory(self):
        """测试 new_task 工厂函数。"""
        task = new_task("report:generate", b"data")
        assert task.type() == "report:generate"
        assert task.payload() == b"data"

    def test_task_with_headers(self):
        """测试带请求头的任务。"""
        task = Task("test", b"data", headers={"x-request-id": "123"})
        assert task.headers() == {"x-request-id": "123"}

    def test_task_repr(self):
        """测试 __repr__ 格式。"""
        task = Task("email:send", b"hello")
        assert "email:send" in repr(task)
        assert "5" in repr(task)  # len(b"hello") = 5


class TestOptions:
    """Option 类的单元测试。"""

    def test_max_retry(self):
        """测试 MaxRetry 选项。"""
        opt = MaxRetry(3)
        assert opt.max_retry_count() == 3

    def test_max_retry_negative_clamped(self):
        """测试 MaxRetry 负数被限制为 0。"""
        opt = MaxRetry(-5)
        assert opt.max_retry_count() == 0

    def test_queue(self):
        """测试 Queue 选项。"""
        opt = Queue("Critical")
        assert opt.queue_name() == "critical"  # 转为小写

    def test_timeout(self):
        """测试 Timeout 选项。"""
        opt = Timeout(30)
        assert opt.timeout_secs() == 30

    def test_unique(self):
        """测试 Unique 选项。"""
        opt = Unique(60)
        assert opt.ttl_secs() == 60

    def test_unique_min_1_second(self):
        """测试 Unique TTL 最小值。"""
        opt = Unique(0)
        assert opt.ttl_secs() == 1

    def test_process_in(self):
        """测试 ProcessIn 选项。"""
        import time
        opt = ProcessIn(10)
        # 验证 process_at 是未来时间
        now_nsec = int(time.time() * 1_000_000_000)
        assert opt.process_at_nsec() >= now_nsec

    def test_retention(self):
        """测试 Retention 选项。"""
        opt = Retention(3600)
        assert opt.retention_secs() == 3600

    def test_group(self):
        """测试 Group 选项。"""
        opt = Group("batch-1")
        assert opt.group_name() == "batch-1"


class TestTaskInfo:
    """TaskInfo 的单元测试。"""

    def test_task_info_defaults(self):
        """测试 TaskInfo 默认值。"""
        info = TaskInfo()
        assert info.id == ""
        assert info.state == TaskState.PENDING
        assert info.max_retry == 25

    def test_task_info_to_dict(self):
        """测试 TaskInfo 序列化。"""
        info = TaskInfo(id="abc123", type="test", state=TaskState.ACTIVE)
        d = info.to_dict()
        assert d["id"] == "abc123"
        assert d["state"] == "active"


class TestTaskState:
    """TaskState 枚举的单元测试。"""

    def test_task_state_strings(self):
        """测试 TaskState 字符串表示。"""
        assert str(TaskState.ACTIVE) == "active"
        assert str(TaskState.PENDING) == "pending"
        assert str(TaskState.SCHEDULED) == "scheduled"
        assert str(TaskState.RETRY) == "retry"
        assert str(TaskState.ARCHIVED) == "archived"
        assert str(TaskState.COMPLETED) == "completed"
        assert str(TaskState.AGGREGATING) == "aggregating"
