# test_server.py — 消费者示例
# 运行: python test_server.py
# 配合 test_client.py 使用，也监听自定义 TaskKey 的任务
from __future__ import annotations

import asyncio, json, time
from asyrq import (
    Server, ServeMux, Config, RedisClientOpt, Context, Task,
)

REDIS = RedisClientOpt(
    addr="127.0.0.1:6380",
    password="fastapiadmin_redis",
    db=0,
)

# ---- 按任务类型限流 ----
_regular_sem = asyncio.Semaphore(2)   # task:regular 最多 2 并发
_delayed_sem = asyncio.Semaphore(1)   # task:delayed 最多 1 并发


async def handle_regular_task(ctx: Context, task: Task) -> None:
    """处理普通任务"""
    async with _regular_sem:
        data = json.loads(task.payload().decode())
        name = data.get("name", "?")
        print(f"  [普通] {name} @ {time.strftime('%H:%M:%S')}")
        await asyncio.sleep(1)
        print(f"  [普通] {name} ✓")

        # 把结果写入 ResultWriter → 自动同步到 ResultKey 指定的 key
        writer = task.result_writer()
        await writer.write(json.dumps({"name": name, "status": "ok"}).encode())


async def handle_delayed_task(ctx: Context, task: Task) -> None:
    """处理延迟任务"""
    async with _delayed_sem:
        data = json.loads(task.payload().decode())
        name = data.get("name", "?")
        print(f"  [延迟] {name} @ {time.strftime('%H:%M:%S')}")
        await asyncio.sleep(1)
        print(f"  [延迟] {name} ✓")

        writer = task.result_writer()
        await writer.write(json.dumps({"name": name, "status": "ok"}).encode())


async def main():
    mux = ServeMux()
    mux.handle_func("task:regular", handle_regular_task)
    mux.handle_func("task:delayed", handle_delayed_task)

    server = Server(REDIS, Config(
        name="辽宁示例消费者",
        code="pmos_liaoning",
        concurrency=5,
        queues={"critical": 6, "default": 3},
    ))

    print("消费者已启动，监听标准队列 + 自定义 TaskKey 任务")
    print("Ctrl+C 停止\n")
    await server.run(mux)  # 阻塞运行


if __name__ == "__main__":
    asyncio.run(main())
