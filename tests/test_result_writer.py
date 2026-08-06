# tests/test_result_writer.py — ResultWriter key 格式测试
import pytest

from asyrq.result_writer import ResultWriter


class _FakeClient:
    def __init__(self):
        self.writes = []

    async def set(self, key, value, ex=None):
        self.writes.append((key, ex))


class _FakeBroker:
    def __init__(self):
        self._client = _FakeClient()


async def test_update_state_uses_asyrq_prefix():
    """中间状态 key 固定为 asyrq:{type}:state:{task_id}。"""
    broker = _FakeBroker()
    writer = ResultWriter("task-1", broker, "default", typename="email:send")
    await writer.update_state({"step": "doing"})

    assert broker._client.writes == [
        ("asyrq:email:send:state:task-1", 3600),
    ]


async def test_finish_default_key_uses_asyrq_prefix():
    """未配置 ResultKey 时，最终结果 key 为 asyrq:{type}:result:{task_id}。"""
    broker = _FakeBroker()
    writer = ResultWriter("task-1", broker, "default", typename="email:send")
    await writer.finish({"status": "ok"}, retention=600)

    assert broker._client.writes == [
        ("asyrq:email:send:result:task-1", 600),
    ]


async def test_finish_custom_result_key():
    """配置 ResultKey 时，最终结果 key 为 {result_key}:{task_id}。"""
    broker = _FakeBroker()
    writer = ResultWriter(
        "task-1", broker, "default",
        typename="email:send", result_key="my:out",
    )
    await writer.finish({"status": "ok"})

    assert broker._client.writes == [
        ("my:out:task-1", None),
    ]
