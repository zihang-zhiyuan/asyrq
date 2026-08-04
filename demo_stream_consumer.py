# demo_stream_consumer.py — Redis Stream 消费者示例（Consumer Group 模式）
# 运行: python demo_stream_consumer.py [消费者名称]
#   - 单个消费者: python demo_stream_consumer.py
#   - 多消费者: 开多个终端，分别运行 python demo_stream_consumer.py worker-1 / worker-2
from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

import redis.asyncio as redis

logger = logging.getLogger("demo.stream.consumer")

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
STREAM_KEY = "demo:orders"                # Stream 键名
GROUP_NAME = "order-processors"           # 消费者组名称
# Redis 连接可用环境变量覆盖，便于本地手动测试:
#   ASYRQ_REDIS_URL / ASYRQ_REDIS_PASSWORD
REDIS_URL = os.getenv("ASYRQ_REDIS_URL", "redis://127.0.0.1:6380")
REDIS_PASSWORD = os.getenv("ASYRQ_REDIS_PASSWORD", "fastapiadmin_redis")
BLOCK_MS = 5000                           # XREADGROUP 阻塞等待时间（毫秒）
BATCH_SIZE = 3                            # 每次批量拉取的消息数


async def ensure_consumer_group(r: redis.Redis) -> None:
    """创建消费者组（如不存在）。

    XGROUP CREATE 需要 Stream 已存在才能创建组。
    这里先尝试创建，如果组已存在则忽略错误。
    使用 MKSTREAM 让 Redis 在 Stream 不存在时自动创建空的 Stream。
    """
    try:
        await r.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
        logger.info("消费者组 '%s' 已创建（Stream: %s）", GROUP_NAME, STREAM_KEY)
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info("消费者组 '%s' 已存在，跳过创建", GROUP_NAME)
        else:
            raise


async def process_message(msg_id: bytes, fields: dict[bytes, bytes]) -> dict:
    """模拟处理一条消息，返回处理结果。

    Args:
        msg_id: Redis Stream 消息 ID（如 b"1234567890123-0"）
        fields: 消息字段字典，key/value 均为 bytes

    Returns:
        处理结果 dict
    """
    # 将 bytes 字段解码为 str
    order_id = fields.get(b"order_id", b"?").decode()
    order_type = fields.get(b"type", b"?").decode()
    user = fields.get(b"user", b"?").decode()
    amount = fields.get(b"amount", b"?").decode()

    logger.info(
        "处理消息  id=%s  order_id=%s  type=%-6s  user=%-8s  ¥%s",
        msg_id.decode(), order_id[:8], order_type, user, amount,
    )

    # 模拟业务处理耗时
    if order_type == "refund":
        await asyncio.sleep(1.0)   # 退款处理较慢
    else:
        await asyncio.sleep(0.2)

    return {"order_id": order_id, "status": "completed", "processed_at": datetime.now(timezone.utc).isoformat()}


async def handle_pending(r: redis.Redis, consumer_name: str) -> int:
    """处理该消费者未确认的 pending 消息（断线重连后恢复）。

    当消费者崩溃后重启时，之前已读取但未 ACK 的消息会留在 PEL（Pending Entries List）中。
    这里先认领回来处理，避免消息丢失。

    Returns:
        处理的 pending 消息数量
    """
    reclaimed = 0
    while True:
        # XPENDING 查看该消费者的 pending 消息
        pending = await r.xpending_range(
            STREAM_KEY, GROUP_NAME, min="-", max="+", count=BATCH_SIZE,
            consumername=consumer_name,
        )
        if not pending:
            break

        for entry in pending:
            msg_id = entry["message_id"]
            # 重新读取消息内容
            result = await r.xrange(STREAM_KEY, min=msg_id, max=msg_id, count=1)
            if result:
                _, fields = result[0]
                await process_message(msg_id, fields)
                await r.xack(STREAM_KEY, GROUP_NAME, msg_id)  # ACK 确认
                reclaimed += 1

    if reclaimed:
        logger.info("恢复 %d 条 pending 消息", reclaimed)
    else:
        logger.info("无 pending 消息需要恢复")
    return reclaimed


async def consume_loop(consumer_name: str) -> None:
    """消费者主循环：从 Stream 读取消息 → 处理 → 确认。

    使用 XREADGROUP 从消费者组读取消息，每条消息只会被组内一个消费者获取。
    处理完成后调用 XACK 确认，消息才会从 PEL 中移除。
    """
    r = redis.Redis.from_url(REDIS_URL, password=REDIS_PASSWORD)
    processed = 0

    try:
        await ensure_consumer_group(r)
        await handle_pending(r, consumer_name)

        logger.info("消费者 '%s' 开始监听 Stream: %s", consumer_name, STREAM_KEY)

        while True:
            # XREADGROUP group consumer streams key ">" count block
            # ">" 表示只读取从未被其他消费者读取过的新消息
            messages = await r.xreadgroup(
                GROUP_NAME, consumer_name,
                streams={STREAM_KEY: ">"},
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )

            if messages is None:
                # 阻塞超时，没有新消息
                continue

            for stream_name, entries in messages:
                for msg_id, fields in entries:
                    try:
                        result = await process_message(msg_id, fields)
                        # XACK stream group id — 确认消息处理完成
                        ack_count = await r.xack(STREAM_KEY, GROUP_NAME, msg_id)
                        if ack_count:
                            processed += 1
                            logger.info(
                                "✓ ACK  id=%s  order_id=%s  (累计: %d)",
                                msg_id.decode(), result["order_id"][:8], processed,
                            )
                        else:
                            logger.warning(
                                "XACK 返回 0，消息可能已被处理: id=%s", msg_id.decode()
                            )
                    except Exception:
                        logger.exception("消息处理失败  id=%s，进入 pending 等待重试", msg_id.decode())

            # 获取 Stream 信息展示概览
            info = await r.xinfo_stream(STREAM_KEY)
            first = info.get("first-entry") or (b"?-0",)
            last = info.get("last-entry") or (b"?-0",)
            logger.info(
                "Stream 概览 | length=%d | first-entry=%s | last-entry=%s",
                info["length"], first[0].decode(), last[0].decode(),
            )

    except asyncio.CancelledError:
        logger.info("消费者 '%s' 被取消，已处理 %d 条消息", consumer_name, processed)
    finally:
        await r.aclose()


async def main() -> None:
    consumer_name = sys.argv[1] if len(sys.argv) > 1 else "worker-1"

    logger.info("=" * 60)
    logger.info(
        "Redis Stream 消费者 | group=%s | consumer=%s | stream=%s",
        GROUP_NAME, consumer_name, STREAM_KEY,
    )
    logger.info("=" * 60)

    await consume_loop(consumer_name)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)-24s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    asyncio.run(main())
