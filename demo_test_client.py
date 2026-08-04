# demo_test_client.py - 生产者示例
# 运行: python demo_test_client.py
from __future__ import annotations

import asyncio, json, logging
import os
from asyrq import Client

logger = logging.getLogger("demo.client")

# Redis 连接可用环境变量覆盖，便于本地手动测试:
#   ASYRQ_REDIS_ADDR / ASYRQ_REDIS_PASSWORD / ASYRQ_REDIS_DB
REDIS_ADDR = os.getenv("ASYRQ_REDIS_ADDR", "127.0.0.1:6380")
REDIS_PASSWORD = os.getenv("ASYRQ_REDIS_PASSWORD", "fastapiadmin_redis")
REDIS_DB = int(os.getenv("ASYRQ_REDIS_DB", "0"))


async def main():
    client = Client(
        server="pmos_liaoning",
        redis_addr=REDIS_ADDR,
        redis_password=REDIS_PASSWORD,
        redis_db=REDIS_DB,
    )

    # 1. 最简 — 指定 server + route + payload
    info = await client.enqueue(
        route="rqdj",
        task=json.dumps({"name": "最简任务"}).encode(),
    )
    logger.info("1.最简     id=%s  route=pmos_liaoning:rqdj", info.id[:12])

    # 2. 指定队列
    info = await client.enqueue(
        route="rqdj",
        task=json.dumps({"name": "高优任务"}).encode(),
        queue="critical",
    )
    logger.info("2.队列     id=%s  queue=critical", info.id[:12])

    # 3. 自定义重试
    info = await client.enqueue(
        route="rqdj",
        task=json.dumps({"name": "少重试"}).encode(),
        max_retry=3,
    )
    logger.info("3.重试     id=%s  max_retry=3", info.id[:12])

    # 4. 处理超时
    info = await client.enqueue(
        route="rqdj",
        task=json.dumps({"name": "限时任务"}).encode(),
        timeout=60,
    )
    logger.info("4.超时     id=%s  timeout=60s", info.id[:12])

    # 5. 延迟执行
    info = await client.enqueue(
        route="delayed",
        task=json.dumps({"name": "延迟10秒"}).encode(),
        process_in=10,
    )
    logger.info("5.延迟     id=%s  10秒后执行", info.id[:12])

    # 6. 去重
    info = await client.enqueue(
        route="rqdj",
        task=json.dumps({"name": "去重任务"}).encode(),
        unique=300,
    )
    logger.info("6.去重     id=%s  300秒内唯一", info.id[:12])

    # 7. 自定义任务 ID
    info = await client.enqueue(
        route="rqdj",
        task=json.dumps({"name": "自定义ID"}).encode(),
        task_id="my-custom-task-001",
    )
    logger.info("7.自定义ID id=%s", info.id)

    # 8. 自定义结果 key
    info = await client.enqueue(
        route="rqdj",
        task=json.dumps({"name": "自定义结果"}).encode(),
        result_key="my:custom:output",
    )
    logger.info("8.结果key id=%s  → my:custom:output:%s", info.id[:12], info.id[:12])

    # 9. 全参数组合
    info = await client.enqueue(
        route="rqdj",
        task=json.dumps({"name": "全参数"}).encode(),
        queue="critical",
        max_retry=3,
        timeout=120,
        process_in=5,
        unique=600,
        result_key="full:output",
    )
    logger.info("9.全参数  id=%s  queue=critical retry=3 timeout=120s delay=5s", info.id[:12])

    await client.close()
    logger.info("全部 9 个示例已入队")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)-14s] %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(main())
