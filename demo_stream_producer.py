# demo_stream_producer.py — Redis Stream 生产者示例
# 运行: python demo_stream_producer.py [消息数量]
from __future__ import annotations

import asyncio
import logging
import sys
import uuid
from datetime import datetime, timezone

import redis.asyncio as redis

logger = logging.getLogger("demo.stream.producer")

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
STREAM_KEY = "demo:orders"                # Stream 键名
REDIS_URL = "redis://127.0.0.1:6380"      # Redis 地址
REDIS_PASSWORD = "fastapiadmin_redis"     # Redis 密码

# 模拟的订单类型
ORDERS = [
    {"type": "create",  "user": "Alice",   "amount": 199.00},
    {"type": "create",  "user": "Bob",     "amount": 89.50},
    {"type": "cancel",  "user": "Alice",   "amount": 199.00},
    {"type": "create",  "user": "Charlie", "amount": 350.00},
    {"type": "refund",  "user": "Bob",     "amount": 89.50},
    {"type": "create",  "user": "Diana",   "amount": 120.00},
    {"type": "update",  "user": "Charlie", "amount": 350.00},
    {"type": "create",  "user": "Eve",     "amount": 75.00},
]


async def produce_messages(count: int) -> None:
    """向 Redis Stream 中写入消息。

    XADD 命令将消息追加到 Stream 末尾，并自动生成消息 ID。
    每条消息是一个 key-value 字典，value 必须为 bytes。
    """
    r = redis.Redis.from_url(REDIS_URL, password=REDIS_PASSWORD)

    try:
        for i in range(count):
            order = ORDERS[i % len(ORDERS)]
            # 每条消息附带唯一 ID 和时间戳
            msg = {
                "order_id": str(uuid.uuid4()),
                "type": order["type"],
                "user": order["user"],
                "amount": str(order["amount"]),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            # XADD stream_key {"field": "value", ...}
            # maxlen=1000 限制 Stream 最大长度，超过时自动裁剪旧消息
            msg_id = await r.xadd(STREAM_KEY, msg, maxlen=1000)
            logger.info(
                "[%d/%d] XADD %s  type=%-6s  user=%-8s  id=%s",
                i + 1, count, STREAM_KEY, msg["type"], msg["user"], msg_id,
            )
            await asyncio.sleep(0.3)  # 模拟生产间隔

    finally:
        await r.aclose()

    logger.info("生产完成：共写入 %d 条消息到 Stream '%s'", count, STREAM_KEY)


async def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    logger.info("=" * 60)
    logger.info("Redis Stream 生产者 | key=%s | 消息数=%d", STREAM_KEY, count)
    logger.info("=" * 60)
    await produce_messages(count)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)-24s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    asyncio.run(main())
