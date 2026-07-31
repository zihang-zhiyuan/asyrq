# demo_scheduler.py — 定时调度独立进程
# 运行: python demo_scheduler.py
# 配合 demo_test_server.py 使用
from __future__ import annotations

import asyncio, json, logging, os
from asyrq import Scheduler, Task, RedisClientOpt, Queue

logger = logging.getLogger("demo.scheduler")
REDIS = RedisClientOpt(
    addr=os.environ.get("ASYRQ_REDIS_ADDR", "127.0.0.1:6379"),
    password=os.environ.get("ASYRQ_REDIS_PASSWORD", ""),
    db=int(os.environ.get("ASYRQ_REDIS_DB", "0")),
)


async def main():
    scheduler = Scheduler(REDIS)

    # ---- @every 格式：每 10 秒 ----
    await scheduler.register(
        "@every 10s",
        Task("pmos_liaoning:regular", json.dumps({"name": "每10秒任务", "from": "scheduler"}).encode()),
    )
    logger.info("✓ @every 10s — pmos_liaoning:regular")

    # ---- Cron 格式：每分钟 ----
    await scheduler.register(
        "* * * * *",
        Task("pmos_liaoning:delayed", json.dumps({"name": "每分钟任务", "from": "scheduler"}).encode()),
    )
    logger.info("✓ * * * * *  — pmos_liaoning:delayed")

    # ---- 带队列的定时任务 ----
    await scheduler.register(
        "@every 30s",
        Task("pmos_liaoning:regular", json.dumps({"name": "30秒高优", "from": "scheduler"}).encode()),
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
