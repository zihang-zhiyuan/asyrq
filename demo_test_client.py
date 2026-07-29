# test_client.py — 生产者示例
# 运行: python test_client.py
from __future__ import annotations

import asyncio, json, time
from asyrq import Client, Task, RedisClientOpt, ProcessIn, TaskKey, ResultKey, Queue

REDIS = RedisClientOpt(addr="127.0.0.1:6380", password="fastapiadmin_redis", db=0)


async def main():
    client = Client(REDIS)

    # ---- 1. 普通任务，结果写到指定 key ----
    await client.enqueue(
        Task("task:regular", json.dumps({"name": "发送邮件"}).encode()),
        ResultKey("result:email:send"),
    )
    print("✓ 入队: 普通任务 → 结果写入 result:email:send")

    # ---- 2. 任务存到指定 Hash key ----
    await client.enqueue(
        Task("task:regular", json.dumps({"name": "生成报表"}).encode()),
        TaskKey("my:task:report:daily"),
        ResultKey("result:report:daily"),
    )
    print("✓ 入队: 存储到 my:task:report:daily → 结果写入 result:report:daily")

    # ---- 3. 延迟 5 秒 ----
    await client.enqueue(
        Task("task:delayed", json.dumps({"name": "5秒提醒"}).encode()),
        ProcessIn(5),
        ResultKey("result:reminder:5s"),
    )
    print("✓ 入队: 5秒后执行 → 结果写入 result:reminder:5s")

    # ---- 4. 指定高优队列 ----
    await client.enqueue(
        Task("task:regular", json.dumps({"name": "紧急任务"}).encode()),
        Queue("critical"),
        ResultKey("result:urgent"),
    )
    print("✓ 入队: 高优队列 critical → 结果写入 result:urgent")

    await client.close()
    print("\n已完成，启动 test_server.py 消费")


if __name__ == "__main__":
    asyncio.run(main())
