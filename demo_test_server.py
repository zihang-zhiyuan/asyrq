# demo_test_server.py - 消费者示例
# 运行: python demo_test_server.py
from __future__ import annotations

import asyncio, json, logging
import os
from asyrq import Server, Task

logger = logging.getLogger("demo.server")

# Redis 连接可用环境变量覆盖，便于本地手动测试:
#   ASYRQ_REDIS_ADDR / ASYRQ_REDIS_PASSWORD / ASYRQ_REDIS_DB
REDIS_ADDR = os.getenv("ASYRQ_REDIS_ADDR", "127.0.0.1:6380")
REDIS_PASSWORD = os.getenv("ASYRQ_REDIS_PASSWORD", "fastapiadmin_redis")
REDIS_DB = int(os.getenv("ASYRQ_REDIS_DB", "0"))

app = Server(
    redis_addr=REDIS_ADDR,
    redis_password=REDIS_PASSWORD,
    redis_db=REDIS_DB,
    name="辽宁数据采集",
    code="pmos_liaoning",
    concurrency=10,
)


@app.task("rqdj")
async def handle_regular(task: Task) -> None:
    data = json.loads(task.payload)
    name = data.get("name", "?")
    logger.info("▶ [rqdj] %-12s started  (id=%s)", name, task.id[:8])
    await task.update_state({"step": "started", "name": name})
    await asyncio.sleep(1)
    await task.finish({"name": name, "status": "ok"})
    logger.info("✓ [rqdj] %-12s done", name)


@app.task("delayed")
async def handle_delayed(task: Task) -> None:
    data = json.loads(task.payload)
    name = data.get("name", "?")
    logger.info("▶ [delayed] %-12s started  (id=%s)", name, task.id[:8])
    await task.update_state({"step": "delayed", "name": name})
    await asyncio.sleep(1)
    await task.finish({"name": name, "status": "ok"})
    logger.info("✓ [delayed] %-12s done", name)


async def main():
    logger.info("═══ 消费者启动 ═══")
    logger.info("  服务名     : 辽宁数据采集")
    logger.info("  code       : pmos_liaoning")
    logger.info("  concurrency: 10")
    logger.info("  routes     : pmos_liaoning:rqdj, pmos_liaoning:delayed")
    logger.info("  Ctrl+C 停止")
    logger.info("═══════════════════")
    await app.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)-14s] %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(main())
