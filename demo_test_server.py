# demo_test_server.py - 消费者示例（loguru 日志）
# 运行: python demo_test_server.py
from __future__ import annotations

import asyncio, json, os, time
from asyrq import Server, Config, ServeMux, RedisClientOpt, Context, Task, LogLevel
from loguru import logger

# ---- 让 asyrq 内部日志也走 loguru ----
class LoguruAdapter:
    """把 asyrq 的 Logger 接口适配到 loguru。"""
    def debug(self, msg, *a, **kw): logger.debug(msg, *a, **kw)
    def info(self, msg, *a, **kw): logger.info(msg, *a, **kw)
    def warn(self, msg, *a, **kw): logger.warning(msg, *a, **kw)
    def error(self, msg, *a, **kw): logger.error(msg, *a, **kw)
    def fatal(self, msg, *a, **kw): logger.critical(msg, *a, **kw)

# ---- 子路由：pmos_liaoning:* 二级分发 ----
liaoning = ServeMux()


@liaoning.task("pmos_liaoning:regular")
async def handle_regular(ctx: Context, task: Task) -> None:
    t0 = time.time()
    data = json.loads(task.payload().decode())
    name = data.get("name", "?")

    logger.info("▶ [REGULAR] {} started", name)
    await task.update_state({"step": "started", "name": name})
    await asyncio.sleep(1)
    await task.finish({"name": name, "status": "ok"})
    logger.info("✓ [REGULAR] {} done ({:.1f}s)", name, time.time() - t0)


@liaoning.task("pmos_liaoning:delayed")
async def handle_delayed(ctx: Context, task: Task) -> None:
    t0 = time.time()
    data = json.loads(task.payload().decode())
    name = data.get("name", "?")

    logger.info("▶ [DELAYED] {} started", name)
    await task.update_state({"step": "delayed", "name": name})
    await asyncio.sleep(1)
    await task.finish({"name": name, "status": "ok"})
    logger.info("✓ [DELAYED] {} done ({:.1f}s)", name, time.time() - t0)


# ---- 主 Server ----
app = Server(
    RedisClientOpt(
        addr=os.environ.get("ASYRQ_REDIS_ADDR", "127.0.0.1:6380"),
        password=os.environ.get("ASYRQ_REDIS_PASSWORD", "fastapiadmin_redis"),
        db=int(os.environ.get("ASYRQ_REDIS_DB", "0")),
    ),
    Config(
        name="辽宁消费者",
        code="pmos_liaoning",
        concurrency=10,
        queues={"critical": 6, "default": 3},
        logger=LoguruAdapter(),   # ← 传入 loguru 适配器
        log_dir=None,             # loguru 自己管文件，不让 asyrq 写
    ),
)
app._mux.handle("pmos_liaoning:", liaoning)


async def main():
    logger.info("═══ 消费者启动 ═══")
    logger.info("  code       : pmos_liaoning")
    logger.info("  concurrency: 10")
    logger.info("  queues     : critical(6), default(3)")
    logger.info("  routes     : pmos_liaoning:regular, pmos_liaoning:delayed")
    logger.info("  Ctrl+C 停止")
    logger.info("═══════════════════")
    await app.run()


if __name__ == "__main__":
    # loguru 默认输出到 stderr，格式已经很好看
    asyncio.run(main())
