# tests/test_integration.py — 集成测试（需要 Redis）

import pytest
import asyncio
import json

from asyrq import (
    Task, Client, Server, ServeMux, Config,
    RedisClientOpt, Queue, MaxRetry, TaskState,
)

# 集成测试的标记，需要本地 Redis 运行
pytestmark = pytest.mark.integration


def get_redis_opt():
    """获取 Redis 连接配置（使用默认本地配置）。"""
    return RedisClientOpt(addr="127.0.0.1:6379", db=15)  # 使用 db15 避免冲突


async def _is_redis_available() -> bool:
    """检查 Redis 是否可用。"""
    try:
        import redis.asyncio as redis
        r = redis.Redis(host="127.0.0.1", port=6379, db=15)
        await r.ping()
        await r.aclose()
        return True
    except Exception:
        return False


class TestClientIntegration:
    """Client 集成测试（需要 Redis）。"""

    @pytest.mark.asyncio
    async def test_ping(self):
        """测试 Redis 连接。"""
        if not await _is_redis_available():
            pytest.skip("Redis 不可用")
        client = Client(get_redis_opt())
        ok = await client.ping()
        await client.close()
        assert ok is True

    @pytest.mark.asyncio
    async def test_enqueue_task(self):
        """测试入队任务。"""
        if not await _is_redis_available():
            pytest.skip("Redis 不可用")
        client = Client(get_redis_opt())
        task = Task("test:hello", json.dumps({"msg": "world"}).encode())
        info = await client.enqueue(task)
        await client.close()

        assert info.id != ""
        assert info.type == "test:hello"
        assert info.state == TaskState.PENDING
        assert info.queue == "default"

    @pytest.mark.asyncio
    async def test_enqueue_with_queue_option(self):
        """测试入队到指定队列。"""
        if not await _is_redis_available():
            pytest.skip("Redis 不可用")
        client = Client(get_redis_opt())
        task = Task("test:important")
        info = await client.enqueue(task, Queue("critical"))
        await client.close()

        assert info.queue == "critical"

    @pytest.mark.asyncio
    async def test_enqueue_with_options(self):
        """测试带多种选项的入队。"""
        if not await _is_redis_available():
            pytest.skip("Redis 不可用")
        client = Client(get_redis_opt())
        task = Task("test:config", b"data")
        info = await client.enqueue(task, MaxRetry(5), Queue("low"))
        await client.close()

        assert info.max_retry == 5
        assert info.queue == "low"


class TestServerIntegration:
    """Server 集成测试（需要 Redis）。"""

    @pytest.mark.asyncio
    async def test_server_process_task(self):
        """测试 Server 处理任务。"""
        if not await _is_redis_available():
            pytest.skip("Redis 不可用")

        # 清理测试数据
        redis_opt = get_redis_opt()
        client = Client(redis_opt)

        # 收集处理结果
        processed = []

        async def my_handler(ctx, task):
            data = json.loads(task.payload.decode())
            processed.append(data)

        # 创建 Server
        mux = ServeMux()
        mux.handle_func("integration:test", my_handler)

        server = Server(redis_opt, Config(concurrency=1))
        await server.start(mux)

        # 入队一个任务
        task = Task("integration:test", json.dumps({"key": "value"}).encode())
        info = await client.enqueue(task)

        # 等待处理完成
        await asyncio.sleep(2)

        # 关闭
        await client.close()
        await server.shutdown()

        # 验证
        assert len(processed) >= 1
        assert processed[0] == {"key": "value"}
