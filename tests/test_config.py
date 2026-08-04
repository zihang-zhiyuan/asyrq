# tests/test_config.py — Config 和内部模块的单元测试

import pytest
from asyrq.server import Config, default_retry_delay, default_is_failure
from asyrq import DEFAULT_QUEUE_NAME, DEFAULT_MAX_RETRY, TaskState
from asyrq.internal.base import TaskMessage
from asyrq.internal.timeutil import now, now_seconds, nsec_to_sec
from asyrq.internal.log import LogLevel, DefaultLogger


class TestConfig:
    """Config 的单元测试。"""

    def test_default_config(self):
        """测试默认配置。"""
        config = Config()
        assert config.concurrency > 0
        assert config.strict_priority is False
        assert config.shutdown_timeout == 8

    def test_custom_config(self):
        """测试自定义配置。"""
        config = Config(
            concurrency=5,
            strict_priority=True,
            group_max_size=50,
        )
        assert config.concurrency == 5
        assert config.strict_priority is True
        assert config.group_max_size == 50


class TestRetryDelay:
    """重试延迟函数的单元测试。"""

    def test_default_retry_delay_grows(self):
        """测试默认重试延迟随重试次数增加。"""
        from asyrq.task import Task
        task = Task("test")
        delay_0 = default_retry_delay(0, Exception("e"), task)
        delay_5 = default_retry_delay(5, Exception("e"), task)
        # 第5次重试的延迟应当远大于第0次
        assert delay_5 > delay_0

    def test_default_is_failure(self):
        """测试默认失败判定。"""
        assert default_is_failure(Exception()) is True
        assert default_is_failure(ValueError()) is True


class TestTaskMessage:
    """TaskMessage 序列化的单元测试。"""

    def test_task_message_serialization(self):
        """测试 TaskMessage JSON 序列化/反序列化。"""
        msg = TaskMessage(
            type="test:task",
            payload=b"hello world",
            id="abc123",
            queue="default",
            retry=25,
        )
        json_str = msg.to_json()
        assert "test:task" in json_str
        assert "abc123" in json_str

        # 反序列化
        msg2 = TaskMessage.from_json(json_str)
        assert msg2.type == "test:task"
        assert msg2.payload == b"hello world"
        assert msg2.id == "abc123"

    def test_task_message_to_dict(self):
        """测试 TaskMessage 转 dict。"""
        msg = TaskMessage(type="test", id="1", payload=b"data")
        d = msg.to_dict()
        assert d["type"] == "test"
        assert d["id"] == "1"
        # payload 应该是 base64 编码的
        import base64
        decoded = base64.b64decode(d["payload"])
        assert decoded == b"data"


class TestTimeUtil:
    """时间工具的单元测试。"""

    def test_now_returns_positive(self):
        """测试 now() 返回正数。"""
        nsec = now()
        assert nsec > 0
        assert nsec > 1_000_000_000_000_000_000  # > 2020 年的时间戳

    def test_now_seconds(self):
        """测试 now_seconds()。"""
        sec = now_seconds()
        assert sec > 0

    def test_nsec_to_sec(self):
        """测试 nsec_to_sec 转换。"""
        assert nsec_to_sec(1_000_000_000) == 1
        assert nsec_to_sec(2_500_000_000) == 2


class TestLogging:
    """日志模块的单元测试。"""

    def test_default_logger(self):
        """测试 DefaultLogger 创建。"""
        logger = DefaultLogger(LogLevel.DEBUG)
        assert logger is not None
        # 不应抛出异常
        logger.debug("test")
        logger.info("test")
        logger.warn("test")
        logger.error("test")

    def test_log_level_enum(self):
        """测试 LogLevel 枚举。"""
        assert LogLevel.DEBUG < LogLevel.INFO
        assert LogLevel.WARN < LogLevel.ERROR
