# demo_scheduler.py — 定时调度独立进程
# 运行: python demo_scheduler.py
# 配合 demo_test_server.py 使用
from __future__ import annotations

import asyncio, json, logging
import os
from asyrq import Scheduler, Task, Queue, RedisClientOpt

logger = logging.getLogger("demo.scheduler")
# Redis 连接可用环境变量覆盖，便于本地手动测试:
#   ASYRQ_REDIS_ADDR / ASYRQ_REDIS_PASSWORD / ASYRQ_REDIS_DB
REDIS = RedisClientOpt(
    addr=os.getenv("ASYRQ_REDIS_ADDR", "127.0.0.1:6380"),
    password=os.getenv("ASYRQ_REDIS_PASSWORD", "fastapiadmin_redis"),
    db=int(os.getenv("ASYRQ_REDIS_DB", "0")),
)


async def main():
    scheduler = Scheduler(REDIS)

    # @every 格式：每 10 秒
    await scheduler.register(
        "@every 10s",
        Task("pmos_liaoning:rqdj", json.dumps({"name": "每10秒任务", "from": "scheduler"}).encode()),
    )
    logger.info("✓ @every 10s — pmos_liaoning:rqdj")

    # Cron 格式：每分钟
    await scheduler.register(
        "* * * * *",
        Task("pmos_liaoning:delayed", json.dumps({"name": "每分钟任务", "from": "scheduler"}).encode()),
    )
    logger.info("✓ * * * * *  — pmos_liaoning:delayed")

    # 带队列的定时任务
    await scheduler.register(
        "@every 30s",
        Task("pmos_liaoning:rqdj", json.dumps({"name": "30秒高优", "from": "scheduler"}).encode()),
        Queue("critical"),
    )
    logger.info("✓ @every 30s — queue=critical")

    logger.info("定时调度已启动，Ctrl+C 停止\n")
    await scheduler.run()  # 阻塞运行


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)-16s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    asyncio.run(main())
