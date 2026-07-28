"""
asyrq 交互式教程
==================
跟着每步跑，5 分钟从零到会用。

环境: Redis 127.0.0.1:6380, 密码 fastapiadmin_redis
"""

import asyncio, json

# ============================================================
# 第 1 步：创建 Redis 连接
# ============================================================
async def step1():
    print("\n" + "=" * 50)
    print("第1步：创建 Redis 连接")
    print("=" * 50)

    from asyrq import RedisClientOpt, Client

    # 这就是你连接 Redis 的方式
    client = Client(RedisClientOpt(
        addr="127.0.0.1:6380",       # Redis 地址
        password="fastapiadmin_redis", # 密码
        db=0,                          # 用哪个 DB（0-15）
    ))

    ok = await client.ping()
    print(f"  Redis 连接: {'✓ 正常' if ok else '✗ 失败'}")

    await client.close()
    return ok


# ============================================================
# 第 2 步：创建并入队任务
# ============================================================
async def step2():
    print("\n" + "=" * 50)
    print("第2步：创建任务并入队")
    print("=" * 50)

    from asyrq import RedisClientOpt, Client, Task
    from asyrq import Queue, MaxRetry, Unique, ProcessIn

    client = Client(RedisClientOpt(
        addr="127.0.0.1:6380", password="fastapiadmin_redis", db=0
    ))

    # ---- 2.1 创建一个任务 ----
    # Task(任务类型名, 负载数据)
    task = Task(
        "email:send",                          # 类型名，用于路由
        json.dumps({"to": "hi@example.com", "body": "你好"}).encode()  # 负载
    )
    print(f"  创建任务: {task}")

    # ---- 2.2 最简单的入队 ----
    info = await client.enqueue(task)
    print(f"  入队: id={info.id[:12]}... 队列={info.queue} 状态={info.state}")

    # ---- 2.3 带选项入队 ----
    info = await client.enqueue(
        Task("report:daily", b"report-data"),
        Queue("critical"),   # 高优队列
        MaxRetry(3),          # 最多重试3次
    )
    print(f"  带选项入队: 队列={info.queue} 最多重试={info.max_retry}次")

    # ---- 2.4 延迟入队（30秒后执行）----
    info = await client.enqueue(
        Task("reminder:send", b"remind-me"),
        ProcessIn(30),
    )
    print(f"  延迟入队: {info.state}（30秒后执行）")

    # ---- 2.5 去重入队 ----
    task_uniq = Task("cache:warm", b"daily")
    info1 = await client.enqueue(task_uniq, Unique(60))   # 60秒内不重复
    print(f"  唯一入队-首次: id={info1.id[:12]}...")
    try:
        info2 = await client.enqueue(task_uniq, Unique(60))
    except Exception:
        print(f"  唯一入队-重复: 被拒绝（任务已存在）")

    await client.close()


# ============================================================
# 第 3 步：定义处理器 + 启动 Server
# ============================================================
async def step3():
    print("\n" + "=" * 50)
    print("第3步：定义处理器，启动 Server 消费任务")
    print("=" * 50)

    from asyrq import (
        RedisClientOpt, Server, ServeMux, Config,
        Client, Task, Context, Handler,
    )

    # ---- 3.1 定义处理器 ----
    # 处理器就是一个异步函数，签名为: async def handler(ctx, task)

    async def handle_email_send(ctx: Context, task: Task):
        """发送邮件的处理器"""
        data = json.loads(task.payload().decode())
        print(f"    → 正在发邮件给 {data['to']}...")
        await asyncio.sleep(0.1)    # 模拟发送
        print(f"    → 邮件发送成功！")

    async def handle_report_daily(ctx: Context, task: Task):
        """日报处理器"""
        print(f"    → 正在生成日报...")
        await asyncio.sleep(0.1)
        print(f"    → 日报生成完毕！")

    # ---- 3.2 注册到路由器 ----
    mux = ServeMux()
    mux.handle_func("email:send", handle_email_send)
    mux.handle_func("report:daily", handle_report_daily)

    # ---- 3.3 启动 Server ----
    config = Config(concurrency=3)   # 同时处理3个任务
    server = Server(RedisClientOpt(
        addr="127.0.0.1:6380", password="fastapiadmin_redis", db=0
    ), config)

    await server.start(mux)   # 非阻塞启动
    print(f"  Server 已启动（并发={config.concurrency}）")

    # ---- 3.4 入队几个任务让 Server 处理 ----
    client = Client(RedisClientOpt(
        addr="127.0.0.1:6380", password="fastapiadmin_redis", db=0
    ))

    await client.enqueue(
        Task("email:send", json.dumps({"to": "alice@x.com", "body": "Hi"}).encode())
    )
    await client.enqueue(
        Task("email:send", json.dumps({"to": "bob@x.com", "body": "Hello"}).encode())
    )
    await client.enqueue(Task("report:daily", b"report"))

    print("  已入队3个任务，等待处理...")
    await asyncio.sleep(2)   # 等2秒让 worker 处理

    # ---- 3.5 关闭 ----
    await client.close()
    await server.shutdown()
    print("  Server 已关闭")


# ============================================================
# 第 4 步：中间件 — 给所有处理器加日志/计时
# ============================================================
async def step4():
    print("\n" + "=" * 50)
    print("第4步：使用中间件")
    print("=" * 50)

    from asyrq import (
        RedisClientOpt, Server, ServeMux, Config,
        Client, Task, Context, Handler, MiddlewareFunc,
    )
    import time

    # ---- 4.1 写一个计时的中间件 ----
    def timing_middleware() -> MiddlewareFunc:
        """统计每个任务耗时的中间件"""
        def middleware(handler: Handler) -> Handler:
            class TimingHandler(Handler):
                async def process_task(self, ctx: Context, task: Task):
                    start = time.time()
                    await handler.process_task(ctx, task)     # 执行实际处理器
                    elapsed = time.time() - start
                    print(f"      ⏱ {task.type()} 耗时 {elapsed:.3f}s")
            return TimingHandler()
        return middleware

    def log_middleware() -> MiddlewareFunc:
        def middleware(handler: Handler) -> Handler:
            class LogHandler(Handler):
                async def process_task(self, ctx: Context, task: Task):
                    print(f"      → 开始: {task.type()}")
                    await handler.process_task(ctx, task)
                    print(f"      → 完成: {task.type()}")
            return LogHandler()
        return middleware

    # ---- 4.2 注册中间件（洋葱模型，先注册的先执行外层）----
    mux = ServeMux()
    mux.use(log_middleware(), timing_middleware())   # log 外层，timing 内层

    async def my_handler(ctx, task):
        await asyncio.sleep(0.2)
        print(f"         🔨 核心逻辑执行中...")

    mux.handle_func("demo:task", my_handler)

    server = Server(RedisClientOpt(
        addr="127.0.0.1:6380", password="fastapiadmin_redis", db=0
    ), Config(concurrency=1))
    await server.start(mux)

    client = Client(RedisClientOpt(
        addr="127.0.0.1:6380", password="fastapiadmin_redis", db=0
    ))
    await client.enqueue(Task("demo:task", b"test"))

    await asyncio.sleep(2)
    await client.close()
    await server.shutdown()


# ============================================================
# 第 5 步：定时任务
# ============================================================
async def step5():
    print("\n" + "=" * 50)
    print("第5步：定时任务（Cron）")
    print("=" * 50)

    from asyrq import (
        RedisClientOpt, Server, ServeMux, Config,
        Client, Task, Scheduler,
    )

    # ---- 5.1 注册定时任务 ----
    scheduler = Scheduler(RedisClientOpt(
        addr="127.0.0.1:6380", password="fastapiadmin_redis", db=0
    ))

    # 每10秒执行一次
    task_a = Task("health:check", b"ping")
    await scheduler.register("@every 10s", task_a)

    # Cron表达式：每分钟第0秒执行
    task_b = Task("report:minute", b"stats")
    await scheduler.register("0 * * * *", task_b)

    print(f"  已注册2个定时任务:")
    print(f"    1. health:check — 每10秒")
    print(f"    2. report:minute — 每分钟")

    # ---- 5.2 配套 Server 消费任务 ----
    count = 0
    async def handle_health(ctx, task):
        nonlocal count
        count += 1
        print(f"    ✓ 健康检查 #{count}")

    mux = ServeMux()
    mux.handle_func("health:check", handle_health)
    mux.handle_func("report:minute", lambda ctx, t: print(f"    ✓ 每分钟报告"))

    server = Server(RedisClientOpt(
        addr="127.0.0.1:6380", password="fastapiadmin_redis", db=0
    ), Config(concurrency=1))
    await server.start(mux)

    await scheduler.start()
    print("  运行15秒观察定时触发...")
    await asyncio.sleep(15)

    await scheduler.shutdown()
    await server.shutdown()


# ============================================================
# 第 6 步：错误处理和重试
# ============================================================
async def step6():
    print("\n" + "=" * 50)
    print("第6步：错误处理 & 重试")
    print("=" * 50)

    from asyrq import (
        RedisClientOpt, Server, ServeMux, Config,
        Client, Task, MaxRetry, SkipRetry,
    )

    async def flaky_handler(ctx, task):
        """模拟不稳定的处理器 — 前2次失败，第3次成功"""
        attempt = int(task.payload().decode())
        if attempt < 3:
            print(f"    第{attempt}次尝试 → 失败，将重试")
            raise Exception(f"模拟失败 #{attempt}")
        print(f"    第{attempt}次尝试 → ✓ 成功！")

    async def bad_data_handler(ctx, task):
        """遇到坏数据 — 不应重试，直接归档"""
        print(f"    数据格式错误 — 跳过重试")
        raise SkipRetry("数据无效，不重试")

    mux = ServeMux()
    mux.handle_func("flaky:task", flaky_handler)
    mux.handle_func("bad:data", bad_data_handler)

    server = Server(RedisClientOpt(
        addr="127.0.0.1:6380", password="fastapiadmin_redis", db=0
    ), Config(concurrency=1))
    await server.start(mux)

    client = Client(RedisClientOpt(
        addr="127.0.0.1:6380", password="fastapiadmin_redis", db=0
    ))

    # 不稳定的任务 — 给足够重试次数
    await client.enqueue(Task("flaky:task", b"1"), MaxRetry(5))
    print("  入队 flaky:task（最多重试5次）")

    # 坏数据任务 — 跳过的范例
    await client.enqueue(Task("bad:data", b"bad"), MaxRetry(3))
    print("  入队 bad:data（坏数据，不重试）")

    await asyncio.sleep(3)
    await client.close()
    await server.shutdown()


# ============================================================
# 第 7 步：完整项目结构
# ============================================================
async def step7():
    print("\n" + "=" * 50)
    print("第7步：生产级项目结构")
    print("=" * 50)
    print("""
  项目文件布局:
  ├── producer.py      # 生产者：往队列里扔任务
  ├── consumer.py      # 消费者：从队列取任务处理
  └── handlers.py      # 处理器：具体的业务逻辑

  ---- producer.py ----
  from asyrq import RedisClientOpt, Client, Task
  client = Client(RedisClientOpt(addr="...", password="..."))
  await client.enqueue(Task("order:create", payload))

  ---- handlers.py ----
  async def handle_order_create(ctx, task):
      data = json.loads(task.payload())
      await create_order(data)

  ---- consumer.py ----
  from asyrq import RedisClientOpt, Server, ServeMux, Config
  from handlers import handle_order_create
  mux = ServeMux()
  mux.handle_func("order:create", handle_order_create)
  server = Server(RedisClientOpt(...), Config(concurrency=10))
  await server.run(mux)   # 阻塞运行，Ctrl+C 停止
""")


# ============================================================
# 主菜单
# ============================================================
async def main():
    print("""
╔══════════════════════════════════════════════════╗
║         asyrq 交互式教程                       ║
║    Redis 分布式任务队列 — 5分钟从零到会用           ║
╚══════════════════════════════════════════════════╝
选择要运行的步骤:
  1 — Redis 连接
  2 — 创建 & 入队任务（即时/延迟/去重）
  3 — 定义处理器 & 启动 Server
  4 — 中间件（日志/计时）
  5 — 定时任务（Cron）
  6 — 错误处理 & 重试
  7 — 生产级项目结构
  0 — 全部运行一遍
""")

    choice = input("输入选择 [0-7]: ").strip() or "0"

    steps = {
        "1": step1,
        "2": step2,
        "3": step3,
        "4": step4,
        "5": step5,
        "6": step6,
        "7": step7,
    }

    if choice == "0":
        for s in ["1", "2", "3", "4", "5", "6", "7"]:
            await steps[s]()
    elif choice in steps:
        await steps[choice]()
    else:
        print("无效选择")

    print("\n教程结束！更多用法见 使用说明文档.md")


if __name__ == "__main__":
    asyncio.run(main())
