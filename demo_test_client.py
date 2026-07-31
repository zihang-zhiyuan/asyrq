# demo_test_client.py — 生产者完整示例（配合 demo_test_server.py）
# 运行: python demo_test_client.py
from __future__ import annotations

import asyncio, json, logging, os
from asyrq import (
    Client, Task, RedisClientOpt,
    ProcessIn,          # 延迟 N 秒执行
    Queue,              # 指定队列
    MaxRetry,           # 最大重试次数
    Timeout,            # 处理超时秒数
    Unique,             # 去重（TTL 秒）
    Retention,          # 完成后保留秒数
    Group,              # 聚合组名
    TaskID,             # 自定义任务 ID
    ResultKey,          # 自定义结果 Redis key
)

logger = logging.getLogger("asyrq.client")
REDIS = RedisClientOpt(
    addr=os.environ.get("ASYRQ_REDIS_ADDR", "127.0.0.1:6379"),
    password=os.environ.get("ASYRQ_REDIS_PASSWORD", ""),
    db=int(os.environ.get("ASYRQ_REDIS_DB", "0")),
)
TASK_TYPE = "pmos_liaoning:regular"       # 普通任务类型（对应 server handler）
TASK_DELAYED = "pmos_liaoning:delayed"    # 延迟任务类型（对应 server handler）


async def main():
    client = Client(REDIS)

    # ====================================================================
    # 1. 最简 — 只传 type + payload
    # ====================================================================
    info = await client.enqueue(
        Task(TASK_TYPE, json.dumps({"name": "最简任务"}).encode()),
    )
    logger.info("1.最简     id=%s  队列=default  重试=25次", info.id[:12])

    # ====================================================================
    # 2. 指定队列 — Queue("队列名")
    # ====================================================================
    info = await client.enqueue(
        Task(TASK_TYPE, json.dumps({"name": "高优任务"}).encode()),
        Queue("critical"),
    )
    logger.info("2.队列     id=%s  queue=critical", info.id[:12])

    # ====================================================================
    # 3. 自定义重试 — MaxRetry(次数)
    # ====================================================================
    info = await client.enqueue(
        Task(TASK_TYPE, json.dumps({"name": "少重试"}).encode()),
        MaxRetry(3),
    )
    logger.info("3.重试     id=%s  max_retry=3", info.id[:12])

    # ====================================================================
    # 4. 处理超时 — Timeout(秒)
    # ====================================================================
    info = await client.enqueue(
        Task(TASK_TYPE, json.dumps({"name": "限时任务"}).encode()),
        Timeout(60),
    )
    logger.info("4.超时     id=%s  timeout=60s", info.id[:12])

    # ====================================================================
    # 5. 延迟执行 — ProcessIn(秒)
    # ====================================================================
    info = await client.enqueue(
        Task(TASK_DELAYED, json.dumps({"name": "延迟10秒"}).encode()),
        ProcessIn(10),
    )
    logger.info("5.延迟     id=%s  10秒后执行", info.id[:12])

    # ====================================================================
    # 6. 去重 — Unique(TTL秒)
    # ====================================================================
    info = await client.enqueue(
        Task(TASK_TYPE, json.dumps({"name": "去重任务"}).encode()),
        Unique(300),
    )
    logger.info("6.去重     id=%s  300秒内唯一", info.id[:12])

    # ====================================================================
    # 7. 结果保留 — Retention(秒)
    # ====================================================================
    info = await client.enqueue(
        Task(TASK_TYPE, json.dumps({"name": "保留结果"}).encode()),
        Retention(3600),
    )
    logger.info("7.保留     id=%s  保留1小时", info.id[:12])

    # ====================================================================
    # 8. 聚合组 — Group("组名")
    # ====================================================================
    info = await client.enqueue(
        Task(TASK_TYPE, json.dumps({"name": "批量汇总"}).encode()),
        Group("hourly-batch"),
    )
    logger.info("8.聚合     id=%s  group=hourly-batch", info.id[:12])

    # ====================================================================
    # 9. 自定义任务 ID — TaskID("自定义id")
    # ====================================================================
    info = await client.enqueue(
        Task(TASK_TYPE, json.dumps({"name": "自定义ID"}).encode()),
        TaskID("my-custom-task-001"),
    )
    logger.info("9.自定义ID id=%s", info.id)

    # ====================================================================
    # 10. 自定义结果 key — ResultKey("prefix")
    # ====================================================================
    info = await client.enqueue(
        Task(TASK_TYPE, json.dumps({"name": "自定义结果"}).encode()),
        ResultKey("my:custom:output"),
    )
    logger.info("10.结果key id=%s  → my:custom:output:%s", info.id[:12], info.id[:12])

    # ====================================================================
    # 11. 全参数组合
    # ====================================================================
    info = await client.enqueue(
        Task(
            TASK_TYPE,
            json.dumps({"name": "全参数"}).encode(),
            headers={"trace-id": "abc123"},
        ),
        Queue("critical"),
        MaxRetry(3),
        Timeout(120),
        ProcessIn(5),
        Unique(600),
        Retention(7200),
        ResultKey("full:output"),
    )
    logger.info("11.全参数  id=%s  queue=critical retry=3 timeout=120s delay=5s", info.id[:12])

    # ====================================================================
    # 12. 定时调度 — 见 demo_scheduler.py（需独立进程运行）
    # ====================================================================
    logger.info("12.定时    见 demo_scheduler.py — 独立进程启动定时入队")

    await client.close()
    logger.info("全部 12 个示例已演示")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)-14s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    asyncio.run(main())
