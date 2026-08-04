"""
asyrq 完整功能测试用例
=========================
运行方式: python test_all.py
要求: 本地 Redis 运行（默认 127.0.0.1:6379 DB 15 用于测试）
可通过环境变量配置:
  ASYRQ_REDIS_ADDR      — Redis 地址（默认 127.0.0.1:6379）
  ASYRQ_REDIS_PASSWORD  — Redis 密码（默认空）
  ASYRQ_REDIS_DB        — Redis DB（默认 15）

测试覆盖:
 1. 任务创建与选项
 2. 任务入队（即时/延迟/唯一）
 3. 服务器处理任务
 4. 任务路由器
 5. 中间件链
 6. 重试与归档
 7. 队列权重
 8. 定时调度器
 9. 错误处理
10. 连接配置
"""
import asyncio, json, sys, time, os
# ============================================================
# 测试工具
# ============================================================
passed = 0
failed = 0
errors = []

def check(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  [✓] {name}")
    else:
        failed += 1
        msg = f"  [✗] {name} — {detail}" if detail else f"  [✗] {name}"
        print(msg)
        errors.append(name)


async def is_redis_available():
    try:
        import redis.asyncio as rds
        addr = os.environ.get("ASYRQ_REDIS_ADDR", "127.0.0.1:6379")
        host, _, port = addr.partition(":")
        pwd = os.environ.get("ASYRQ_REDIS_PASSWORD", "")
        db = int(os.environ.get("ASYRQ_REDIS_DB", "15"))
        r = rds.Redis(host=host, port=int(port or "6379"), password=pwd or None, db=db)
        await r.ping()
        await r.aclose()
        return True
    except Exception:
        return False


def get_redis_opt():
    from asyrq.connection import RedisClientOpt
    addr = os.environ.get("ASYRQ_REDIS_ADDR", "127.0.0.1:6379")
    pwd = os.environ.get("ASYRQ_REDIS_PASSWORD", "")
    db = int(os.environ.get("ASYRQ_REDIS_DB", "15"))
    return RedisClientOpt(addr=addr, password=pwd, db=db)


def get_raw_redis():
    import redis.asyncio as rds
    addr = os.environ.get("ASYRQ_REDIS_ADDR", "127.0.0.1:6379")
    host, _, port = addr.partition(":")
    pwd = os.environ.get("ASYRQ_REDIS_PASSWORD", "")
    db = int(os.environ.get("ASYRQ_REDIS_DB", "15"))
    return rds.Redis(host=host, port=int(port or "6379"), password=pwd or None, db=db)


# ============================================================
# 测试 1: 任务创建与选项
# ============================================================
async def test_task_and_options():
    print("\n[1] 任务创建与选项")
    from asyrq import (
        Task, TaskState, MaxRetry, Queue, Timeout, Unique,
        ProcessIn, ProcessAt, Retention, Group, TaskID
    )

    # 1.1 基本任务创建
    task = Task("email:send", b'{"to":"a@b.com"}')
    check("创建基本任务", task.type == "email:send")
    check("任务负载", task.payload == b'{"to":"a@b.com"}')
    check("result_writer 为空", task.result_writer() is None)

    # 1.2 空类型名应报错
    try:
        Task("")
        check("空类型名报错", False, "应该抛出 ValueError")
    except ValueError:
        check("空类型名报错", True)

    # 1.3 带请求头的任务
    task = Task("test", b"data", headers={"x-id": "123"})
    check("任务请求头", task.headers == {"x-id": "123"})

    # 1.4 new_task 工厂函数
    from asyrq import new_task
    task = new_task("report:gen", b"payload")
    check("new_task 工厂", task.type == "report:gen")

    # 1.5 选项
    check("MaxRetry(5)", MaxRetry(5).max_retry_count() == 5)
    check("MaxRetry(-1)→0", MaxRetry(-1).max_retry_count() == 0)
    check("Queue 小写", Queue("Critical").queue_name() == "critical")
    check("Timeout(30)", Timeout(30).timeout_secs() == 30)
    check("Unique(60)", Unique(60).ttl_secs() == 60)
    check("Unique(0)→1", Unique(0).ttl_secs() == 1)
    check("Retention(3600)", Retention(3600).retention_secs() == 3600)
    check("Group", Group("batch").group_name() == "batch")
    check("TaskID", TaskID("custom-id").task_id() == "custom-id")

    # 1.6 ProcessIn 计算未来时间
    import time as _time
    opt = ProcessIn(10)
    now = int(_time.time() * 1_000_000_000)
    check("ProcessIn 未来时间", opt.process_at_nsec() >= now)

    # 1.7 TaskState 字符串
    check("TaskState 字符串", str(TaskState.ACTIVE) == "active")
    check("TaskState 字符串", str(TaskState.PENDING) == "pending")
    check("TaskState 字符串", str(TaskState.ARCHIVED) == "archived")


# ============================================================
# 测试 2: 连接配置
# ============================================================
async def test_connection():
    print("\n[2] 连接配置")
    from asyrq.connection import RedisClientOpt, parse_redis_uri

    opt = RedisClientOpt()
    check("默认地址", opt.addr == "127.0.0.1:6379")
    check("默认 DB", opt.db == 0)
    check("默认 pool_size", opt.pool_size == 10)

    opt2 = parse_redis_uri("redis://:mypwd@host:6380/3")
    check("URI 解析密码", opt2.password == "mypwd")
    check("URI 解析地址", opt2.addr == "host:6380")
    check("URI 解析 DB", opt2.db == 3)

    opt3 = parse_redis_uri("rediss://localhost:6380")
    check("rediss TLS", opt3.tls_config is not None)


# ============================================================
# 测试 3: 错误类型
# ============================================================
async def test_errors():
    print("\n[3] 错误类型")
    from asyrq.errors import (
        SkipRetry, EnqueueError, DequeueError,
        TaskNotFoundError, QueueNotFoundError
    )

    check("SkipRetry 是 Exception", isinstance(SkipRetry("跳过"), Exception))
    check("EnqueueError", isinstance(EnqueueError("入队失败"), Exception))
    check("TaskNotFoundError", isinstance(TaskNotFoundError("无"), Exception))
    check("QueueNotFoundError", isinstance(QueueNotFoundError("无"), Exception))


# ============================================================
# 测试 4: Handler 和 ServeMux
# ============================================================
async def test_handler_and_mux():
    print("\n[4] Handler 和 ServeMux")
    from asyrq import Handler, Context, ServeMux, Task

    # 4.1 Handler 接口
    class TestHandler(Handler):
        async def process_task(self, ctx, task):
            pass
    handler = TestHandler()
    await handler.process_task(Context(), Task("test"))
    check("Handler 接口", True)

    # 4.2 精确匹配
    mux = ServeMux()
    results = []
    async def h_a(ctx, task): results.append("a")
    async def h_b(ctx, task): results.append("b")
    mux.handle_func("task:a", h_a)
    mux.handle_func("task:b", h_b)
    await mux.process_task(Context(), Task("task:a"))
    check("精确匹配", results == ["a"])

    # 4.3 前缀匹配
    mux2 = ServeMux()
    r2 = []
    async def h_default(ctx, task): r2.append("default")
    async def h_specific(ctx, task): r2.append("specific")
    mux2.handle_func("task:", h_default)
    mux2.handle_func("task:specific", h_specific)
    await mux2.process_task(Context(), Task("task:specific"))
    check("精确优先于前缀", r2 == ["specific"])
    await mux2.process_task(Context(), Task("task:other"))
    check("前缀匹配回退", r2 == ["specific", "default"])

    # 4.4 无匹配报错
    mux3 = ServeMux()
    try:
        await mux3.process_task(Context(), Task("unknown"))
        check("无匹配报错", False, "应该抛出 ValueError")
    except ValueError:
        check("无匹配报错", True)

    # 4.5 HandlerFunc 适配
    called = False
    async def my_func(ctx, task):
        nonlocal called; called = True
    from asyrq.handler import wrap_handler_func
    h = wrap_handler_func(my_func)
    await h.process_task(Context(), Task("x"))
    check("HandlerFunc 适配", called)


# ============================================================
# 测试 5: 中间件
# ============================================================
async def test_middleware():
    print("\n[5] 中间件")
    from asyrq import ServeMux, Handler, Context, Task

    mux = ServeMux()
    order = []

    def make_mw(name):
        def middleware(handler):
            class W(Handler):
                async def process_task(self, ctx, task):
                    order.append(f"{name}_before")
                    await handler.process_task(ctx, task)
                    order.append(f"{name}_after")
            return W()
        return middleware

    async def real_handler(ctx, task): order.append("handler")
    mux.handle_func("test", real_handler)
    mux.use(make_mw("A"), make_mw("B"), make_mw("C"))
    await mux.process_task(Context(), Task("test"))
    check("洋葱模型",
          order == ["A_before","B_before","C_before","handler","C_after","B_after","A_after"])


# ============================================================
# 测试 6: Client 入队（需要 Redis）
# ============================================================
async def test_client_enqueue():
    print("\n[6] Client 入队操作 [需要 Redis]")
    if not await is_redis_available():
        print("  ⚠ Redis 不可用 — 跳过")
        return

    from asyrq import Client, Task, Queue, MaxRetry, Unique, ProcessIn
    from asyrq.connection import RedisClientOpt
    from asyrq.errors import EnqueueError

    # 先清空测试 DB，避免上次运行的残留数据
    r = get_raw_redis()
    await r.flushdb()
    await r.aclose()

    opt = get_redis_opt()

    async with Client(opt) as client:
        # 6.1 ping
        ok = await client.ping()
        check("Client.ping()", ok)

        # 6.2 基本入队
        task = Task("test:basic", b"hello")
        info = await client.enqueue(task)
        check("基本入队", info.id != "" and info.type == "test:basic")
        check("状态=PENDING", str(info.state) == "pending")
        check("队列=default", info.queue == "default")

        # 6.3 指定队列入队
        info = await client.enqueue(Task("test:q"), Queue("critical"))
        check("指定队列", info.queue == "critical")

        # 6.4 带选项入队
        info = await client.enqueue(Task("test:opts"), MaxRetry(5), Queue("low"))
        check("重试次数", info.max_retry == 5)
        check("低优队列", info.queue == "low")

        # 6.5 唯一入队（去重）
        unique_task = Task("test:unique", b"unique-payload")
        info1 = await client.enqueue(unique_task, Unique(60))
        check("唯一入队-首次", info1.id != "")
        # 重复入队应抛出 EnqueueError
        dup_caught = False
        try:
            await client.enqueue(unique_task, Unique(60))
        except EnqueueError:
            dup_caught = True
        check("唯一入队-重复报错", dup_caught)

        # 6.6 延迟入队
        info = await client.enqueue(Task("test:delay"), ProcessIn(3600))
        check("延迟入队", str(info.state) == "scheduled")
        check("延迟队列", info.next_process_at > 0)

        # 6.7 空任务类型报错
        try:
            Task("")
            check("空类型报错", False, "应该报错")
        except ValueError:
            check("空类型报错", True)


# ============================================================
# 测试 7: Server 处理任务（需要 Redis）
# ============================================================
async def test_server_process():
    print("\n[7] Server 处理任务 [需要 Redis]")
    if not await is_redis_available():
        print("  ⚠ Redis 不可用 — 跳过")
        return

    # 清理残留数据
    r = get_raw_redis(); await r.flushdb(); await r.aclose()

    from asyrq import Client, Task, Server, ServeMux, Config, Queue
    from asyrq.connection import RedisClientOpt

    opt = get_redis_opt()
    result_holder = {}

    async def my_handler(ctx, task):
        result_holder[task.type] = task.payload.decode()

    mux = ServeMux()
    mux.handle_func("test:server", my_handler)

    server = Server(opt, Config(concurrency=1))
    await server.start(mux)

    async with Client(opt) as client:
        # 入队 3 个任务
        for i in range(3):
            await client.enqueue(
                Task("test:server", f"消息{i}".encode())
            )

    # 等待处理完成
    await asyncio.sleep(2)
    await server.shutdown()

    check("Server 至少处理1个", len(result_holder) >= 1)
    check("Server 处理结果正确", result_holder.get("test:server") in ["消息0", "消息1", "消息2"])


# ============================================================
# 测试 8: 队列权重
# ============================================================
async def test_queue_weights():
    print("\n[8] 队列优先级 [需要 Redis]")
    if not await is_redis_available():
        print("  ⚠ Redis 不可用 — 跳过")
        return

    from asyrq import Client, Task, Server, ServeMux, Config, Queue
    from asyrq.connection import RedisClientOpt

    opt = get_redis_opt()
    processed_queues = []

    async def handler(ctx, task):
        # 从 task key 中无法直接获取 queue，这里只验证能处理
        processed_queues.append("ok")

    mux = ServeMux()
    mux.handle_func("test:weight", handler)

    config = Config(
        concurrency=1,
        queues={"critical": 6, "default": 3, "low": 1},
    )
    server = Server(opt, config)
    await server.start(mux)

    async with Client(opt) as client:
        await client.enqueue(Task("test:weight", b"1"), Queue("critical"))
        await client.enqueue(Task("test:weight", b"2"), Queue("default"))
        await client.enqueue(Task("test:weight", b"3"), Queue("low"))

    await asyncio.sleep(2)
    await server.shutdown()

    check("多队列处理", len(processed_queues) >= 1)


# ============================================================
# 测试 9: 重试与 SkipRetry
# ============================================================
async def test_retry_and_skip():
    print("\n[9] 重试与 SkipRetry [需要 Redis]")
    if not await is_redis_available():
        print("  ⚠ Redis 不可用 — 跳过")
        return

    from asyrq import Client, Task, Server, ServeMux, Config, MaxRetry
    from asyrq.connection import RedisClientOpt
    from asyrq.errors import SkipRetry
    from asyrq.internal.rdb import RDB
    import redis.asyncio as rds

    opt = get_redis_opt()

    # 9.1 SkipRetry 直接归档
    skip_called = False
    async def skip_handler(ctx, task):
        nonlocal skip_called; skip_called = True
        raise SkipRetry("测试跳过")

    mux = ServeMux()
    mux.handle_func("test:skip", skip_handler)
    server = Server(opt, Config(concurrency=1))
    await server.start(mux)

    async with Client(opt) as client:
        await client.enqueue(Task("test:skip", b"x"), MaxRetry(3))

    await asyncio.sleep(2)
    await server.shutdown()
    check("SkipRetry 被调用", skip_called)

    # 验证任务已归档
    r = get_raw_redis()
    broker = RDB(r)
    stats = await broker.current_queue_stats("test", "skip", "default")
    check("归档计数≥1", stats["archived"] >= 1)
    await r.aclose()


# ============================================================
# 测试 10: 任务消息序列化
# ============================================================
async def test_message_serialization():
    print("\n[10] 任务消息序列化")
    from asyrq.internal.base import TaskMessage
    import base64

    msg = TaskMessage(
        type="email:send",
        payload=b"hello world",
        id="abc-123",
        queue="default",
        retry=25,
    )

    json_str = msg.to_json()
    check("JSON 包含类型", "email:send" in json_str)
    check("JSON 包含 ID", "abc-123" in json_str)

    msg2 = TaskMessage.from_json(json_str)
    check("反序列化类型", msg2.type == "email:send")
    check("反序列化负载", msg2.payload == b"hello world")
    check("反序列化 ID", msg2.id == "abc-123")

    d = msg.to_dict()
    decoded = base64.b64decode(d["payload"])
    check("to_dict 负载", decoded == b"hello world")


# ============================================================
# 测试 11: 时间工具
# ============================================================
async def test_time_utils():
    print("\n[11] 时间工具")
    from asyrq.internal.timeutil import now, now_seconds, nsec_to_sec

    nsec = now()
    check("now() > 1e18(>2020年)", nsec > 1_000_000_000_000_000_000)
    check("now_seconds() > 0", now_seconds() > 0)
    check("纳秒→秒", nsec_to_sec(1_000_000_000) == 1)
    check("纳秒→秒", nsec_to_sec(2_500_000_000) == 2)


# ============================================================
# 测试 12: 日志
# ============================================================
async def test_logging():
    print("\n[12] 日志系统")
    from asyrq.internal.log import DefaultLogger, LogLevel

    logger = DefaultLogger(LogLevel.DEBUG)
    logger.debug("debug msg")
    logger.info("info msg")
    logger.warn("warn msg")
    logger.error("error msg")
    check("日志器不报错", True)
    check("日志级别", LogLevel.DEBUG < LogLevel.INFO < LogLevel.WARN)


# ============================================================
# 测试 13: 调度器（需要 Redis）
# ============================================================
async def test_scheduler():
    print("\n[13] 调度器 [需要 Redis]")
    if not await is_redis_available():
        print("  ⚠ Redis 不可用 — 跳过")
        return

    from asyrq import Scheduler, Task, SchedulerOpts
    from asyrq.connection import RedisClientOpt

    opt = get_redis_opt()
    scheduler = Scheduler(opt)

    # 注册一个 @every 1s 的任务
    entry_id = await scheduler.register("@every 30s", Task("sched:test", b"x"))
    check("注册调度任务", entry_id != "")

    # 启动 2 秒后关闭
    await scheduler.start()
    await asyncio.sleep(1)  # 等 1 秒让主循环运行
    await scheduler.shutdown()
    check("调度器启动关闭", True)

    # 取消注册
    scheduler2 = Scheduler(opt)
    eid = await scheduler2.register("@every 1h", Task("sched:cancel"))
    check("再注册", eid != "")
    await scheduler2.unregister(eid)
    check("取消注册", eid not in scheduler2._entries)
    await scheduler2.shutdown()


# ============================================================
# 测试 14: ResultWriter
# ============================================================
async def test_result_writer():
    print("\n[14] ResultWriter")
    from asyrq.result_writer import ResultWriter

    writer = ResultWriter("task-123", None, "default")
    check("task_id", writer.task_id() == "task-123")
    written = await writer.write(b"result-data")
    check("write 返回字节数", written == 11)
    check("get_data", writer.get_data() == b"result-data")


# ============================================================
# 测试 15: Config
# ============================================================
async def test_config():
    print("\n[15] 配置")
    from asyrq import Config, DEFAULT_QUEUE_NAME, DEFAULT_MAX_RETRY

    c = Config()
    check("默认并发>0", c.concurrency > 0)
    check("默认队列含default", DEFAULT_QUEUE_NAME in c.queues)
    check("默认队列权重=1", c.queues[DEFAULT_QUEUE_NAME] == 1)
    check("默认非严格优先级", not c.strict_priority)
    check("默认关闭超时=8", c.shutdown_timeout == 8)
    check("默认最大重试=25", DEFAULT_MAX_RETRY == 25)

    c2 = Config(concurrency=5, strict_priority=True, group_max_size=50)
    check("自定义并发", c2.concurrency == 5)
    check("自定义严格优先级", c2.strict_priority)
    check("自定义聚合大小", c2.group_max_size == 50)


# ============================================================
# 测试 16: 重试延迟算法
# ============================================================
async def test_retry_delay():
    print("\n[16] 重试延迟算法")
    from asyrq.server import default_retry_delay, default_is_failure
    from asyrq import Task

    task = Task("test")
    d0 = default_retry_delay(0, Exception("e"), task)
    d3 = default_retry_delay(3, Exception("e"), task)
    d5 = default_retry_delay(5, Exception("e"), task)
    check("延迟递增 0<3", d0 < d3)
    check("延迟递增 3<5", d3 < d5)
    check("第5次>600s", d5 > 600)
    check("is_failure", default_is_failure(Exception()) is True)


# ============================================================
# 测试 17: Redis Key 生成
# ============================================================
async def test_redis_keys():
    print("\n[17] Redis Key 生成")
    from asyrq.internal.base import (
        pending_key, active_key, scheduled_key, retry_key, archived_key,
        completed_key, task_key, paused_key, lease_key, group_key,
        all_groups_key, all_queues_key, servers_key, workers_key,
        unique_key, ALL_QUEUES_KEY, SERVERS_KEY,
    )

    check("pending key", pending_key("t", "r", "q") == "asyrq:tasks:t:r:q:pending")
    check("active key", active_key("t", "r", "q") == "asyrq:tasks:t:r:q:active")
    check("scheduled key", scheduled_key("t", "r", "q") == "asyrq:tasks:t:r:q:scheduled")
    check("retry key", retry_key("t", "r", "q") == "asyrq:tasks:t:r:q:retry")
    check("archived key", archived_key("t", "r", "q") == "asyrq:tasks:t:r:q:archived")
    check("completed key", completed_key("t", "r", "q") == "asyrq:tasks:t:r:q:completed")
    check("task key", task_key("t", "r", "q", "id1") == "asyrq:tasks:t:r:q:id1")
    check("paused key", paused_key("t", "r", "q") == "asyrq:tasks:t:r:q:paused")
    check("lease key", lease_key("t", "r", "q") == "asyrq:tasks:t:r:q:lease")
    check("group key", group_key("t", "r", "q", "g") == "asyrq:tasks:t:r:q:aggregation:g")
    check("all groups", all_groups_key("t", "r", "q") == "asyrq:tasks:t:r:q:groups")
    check("all queues", all_queues_key() == "asyrq:queues")
    check("servers", servers_key() == "asyrq:servers")
    check("workers", workers_key("h", 1, "s") == "asyrq:workers:h:1:s")

    import hashlib
    h = hashlib.sha256(b"test").hexdigest()
    check("unique key", unique_key("t", "r", "q", b"test") == f"asyrq:tasks:t:r:q:unique:{h}")

    check("ALL_QUEUES_KEY 常量", ALL_QUEUES_KEY == "asyrq:queues")
    check("SERVERS_KEY 常量", SERVERS_KEY == "asyrq:servers")


# ============================================================
# 测试 18: 入队全部选项组合
# ============================================================
async def test_all_options():
    print("\n[18] 全部选项组合 [需要 Redis]")
    if not await is_redis_available():
        print("  ⚠ Redis 不可用 — 跳过")
        return

    from asyrq import (
        Client, Task, Queue, MaxRetry, Timeout, Unique,
        ProcessIn, Retention, Group, TaskID,
    )
    from asyrq.connection import RedisClientOpt

    opt = get_redis_opt()

    async with Client(opt) as client:
        # 组合：自定义 ID + 指定队列 + 重试 + 超时 + 保留
        info = await client.enqueue(
            Task("test:all"),
            TaskID("my-custom-id"),
            Queue("critical"),
            MaxRetry(3),
            Timeout(120),
            Retention(600),
        )
        check("自定义ID", info.id == "my-custom-id")
        check("队列=critical", info.queue == "critical")
        check("重试=3", info.max_retry == 3)
        check("超时=120", info.timeout == 120)
        check("保留=600", info.retention == 600)

        # 组合：唯一 + 延迟 + 聚合组
        info = await client.enqueue(
            Task("test:combo2", b"data"),
            Unique(300),
            ProcessIn(1800),
            Group("batch-daily"),
        )
        check("唯一延迟聚合", str(info.state) == "scheduled" and info.group == "batch-daily")


# ============================================================
# 测试 19: 优雅关闭
# ============================================================
async def test_graceful_shutdown():
    print("\n[19] 优雅关闭 [需要 Redis]")
    if not await is_redis_available():
        print("  ⚠ Redis 不可用 — 跳过")
        return

    from asyrq import Server, ServeMux, Config, Task, Client
    from asyrq.connection import RedisClientOpt

    opt = get_redis_opt()

    async def slow_handler(ctx, task):
        await asyncio.sleep(5)  # 模拟长任务

    mux = ServeMux()
    mux.handle_func("test:slow", slow_handler)

    server = Server(opt, Config(concurrency=1, shutdown_timeout=3))

    # 启动 → 入队长任务 → 立即关闭
    await server.start(mux)

    async with Client(opt) as client:
        await client.enqueue(Task("test:slow", b"x"))

    await asyncio.sleep(0.5)    # 等任务被取出
    start = time.time()
    await server.shutdown()
    elapsed = time.time() - start

    # 关闭应该在 shutdown_timeout 附近
    check("优雅关闭完成", True)
    check(f"关闭耗时<{server._config.shutdown_timeout + 2}s", elapsed < server._config.shutdown_timeout + 2)


# ============================================================
# 测试 20: Version 和 __all__
# ============================================================
async def test_version():
    print("\n[20] 版本和导出")
    import asyrq

    check("版本号存在", asyrq.__version__ == "0.1.2")
    check("ASYNQ_VERSION", asyrq.ASYNQ_VERSION == "0.1.0")

    expected = [
        "Task", "TaskInfo", "new_task", "Client", "Server", "ServeMux", "Config",
        "Handler", "HandlerFunc", "Context", "MiddlewareFunc",
        "MaxRetry", "Queue", "TaskID", "Timeout", "Deadline", "Unique",
        "ProcessAt", "ProcessIn", "Retention", "Group",
        "RedisClientOpt", "RedisFailoverClientOpt", "RedisClusterClientOpt",
        "parse_redis_uri", "ResultWriter",
        "SkipRetry", "EnqueueError", "DequeueError",
        "TaskNotFoundError", "QueueNotFoundError",
        "Scheduler", "SchedulerOpts",
        "TaskState", "TaskMessage", "Logger", "LogLevel", "DefaultLogger",
    ]
    for name in expected:
        check(f"导出 {name}", hasattr(asyrq, name))


# ============================================================
# 主函数
# ============================================================
async def main():
    print("=" * 60)
    print("  asyrq 完整功能测试")
    print("=" * 60)
    print(f"  Python: {sys.version}")
    print(f"  时间:   {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 检查 Redis
    redis_ok = await is_redis_available()
    if redis_ok:
        print("  Redis:  连接正常 (DB 15)")
    else:
        print("  Redis:  未连接 — 跳过需要 Redis 的测试")

    # 运行所有测试
    await test_task_and_options()
    await test_connection()
    await test_errors()
    await test_handler_and_mux()
    await test_middleware()
    await test_client_enqueue()
    await test_server_process()
    await test_queue_weights()
    await test_retry_and_skip()
    await test_message_serialization()
    await test_time_utils()
    await test_logging()
    await test_scheduler()
    await test_result_writer()
    await test_config()
    await test_retry_delay()
    await test_redis_keys()
    await test_all_options()
    await test_graceful_shutdown()
    await test_version()

    # 输出汇总
    print("\n" + "=" * 60)
    total = passed + failed
    print(f"  结果: {passed}/{total} 通过", end="")
    if failed > 0:
        print(f"  ({failed} 失败)")
        print(f"  失败项: {', '.join(errors)}")
    else:
        print("  ✓ 全部通过!")
    print("=" * 60)

    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
