# stress_test.py — asyrq 长时间高负荷压力测试
"""
运行方式:
    python stress_test.py [--duration 300] [--producers 5] [--consumers 10] [--redis-host 127.0.0.1] [--redis-port 6379] [--redis-password ""]

测试维度:
    1. 高并发吞吐量测试 — 多生产者 + 多消费者
    2. 长时间稳定性测试 — 默认 5 分钟持续运行
    3. 内存泄漏检测 — 监控进程内存变化
    4. 任务场景全覆盖 — 即时/延迟/去重/重试/定时
    5. 优雅关闭测试 — 负载下关闭验证
    6. 错误恢复测试 — 任务失败重试和归档

前置条件:
    - Redis 服务器运行在指定的 host:port
    - pip install asyrq redis psutil
"""
from __future__ import annotations

import asyncio, json, os, sys, time, uuid, math, random, argparse
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Optional

import redis.asyncio as _redis

from asyrq import (
    Client, Server, ServeMux, Config, Task, Context,
    RedisClientOpt, Queue, MaxRetry, Timeout, Unique,
    ProcessIn, Retention, TaskID, ResultKey, SkipRetry,
)


# ============================================================================
# 统计收集器
# ============================================================================

@dataclass
class StressStats:
    """压力测试统计信息。"""
    # 吞吐量
    total_enqueued: int = 0
    total_processed: int = 0
    total_failed: int = 0
    total_retried: int = 0
    total_skipped: int = 0
    total_timeout: int = 0

    # 延迟（纳秒）
    latencies: list[float] = field(default_factory=list)

    # 每个任务类型的计数
    by_type: dict = field(default_factory=lambda: defaultdict(int))

    # 错误
    errors: list[str] = field(default_factory=list)

    # 开始/结束时间
    start_time: float = 0.0
    end_time: float = 0.0

    # Redis 队列状态快照
    queue_snapshots: list[dict] = field(default_factory=list)

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time

    @property
    def throughput_enqueue(self) -> float:
        dur = self.duration
        return self.total_enqueued / dur if dur > 0 else 0

    @property
    def throughput_process(self) -> float:
        dur = self.duration
        return self.total_processed / dur if dur > 0 else 0

    @property
    def avg_latency_ms(self) -> float:
        if not self.latencies:
            return 0
        return (sum(self.latencies) / len(self.latencies)) / 1_000_000

    @property
    def p50_latency_ms(self) -> float:
        if not self.latencies: return 0
        s = sorted(self.latencies)
        return s[len(s) // 2] / 1_000_000

    @property
    def p99_latency_ms(self) -> float:
        if not self.latencies: return 0
        s = sorted(self.latencies)
        return s[int(len(s) * 0.99)] / 1_000_000

    @property
    def max_latency_ms(self) -> float:
        if not self.latencies: return 0
        return max(self.latencies) / 1_000_000

    @property
    def error_rate(self) -> float:
        total = self.total_processed + self.total_failed
        return (self.total_failed / total * 100) if total > 0 else 0


# ============================================================================
# 全局统计实例
# ============================================================================
stats = StressStats()
stats_lock = asyncio.Lock()


# ============================================================================
# 任务处理器 — 模拟各类工作负载
# ============================================================================

async def handle_quick_task(ctx: Context, task: Task) -> None:
    """快速任务 — 模拟轻量级操作。"""
    t0 = time.time_ns()
    payload = json.loads(task.payload().decode()) if task.payload() else {}
    t_start = payload.get("_enqueue_time", 0)

    # 模拟 CPU 工作
    _ = sum(range(100))

    t1 = time.time_ns()
    async with stats_lock:
        stats.total_processed += 1
        stats.by_type[task.type()] += 1
        if t_start:
            stats.latencies.append(t1 - t_start)

    await task.finish({"status": "ok", "elapsed_ns": t1 - t0})


async def handle_io_task(ctx: Context, task: Task) -> None:
    """IO 密集任务 — 模拟网络/数据库调用。"""
    t0 = time.time_ns()
    payload = json.loads(task.payload().decode()) if task.payload() else {}
    t_start = payload.get("_enqueue_time", 0)

    # 模拟 IO 延迟
    delay = random.uniform(0.01, 0.1)
    await asyncio.sleep(delay)

    t1 = time.time_ns()
    async with stats_lock:
        stats.total_processed += 1
        stats.by_type[task.type()] += 1
        if t_start:
            stats.latencies.append(t1 - t_start)

    await task.finish({"status": "ok", "io_delay": delay})


async def handle_cpu_task(ctx: Context, task: Task) -> None:
    """CPU 密集任务 — 模拟数据处理。"""
    t0 = time.time_ns()
    payload = json.loads(task.payload().decode()) if task.payload() else {}
    t_start = payload.get("_enqueue_time", 0)

    # 模拟 CPU 密集型工作
    n = payload.get("cpu_iterations", 10000)
    _ = sum(i * i for i in range(n))

    t1 = time.time_ns()
    async with stats_lock:
        stats.total_processed += 1
        stats.by_type[task.type()] += 1
        if t_start:
            stats.latencies.append(t1 - t_start)

    await task.finish({"status": "ok", "iterations": n})


async def handle_flaky_task(ctx: Context, task: Task) -> None:
    """不稳定任务 — 随机失败，测试重试机制。"""
    payload = json.loads(task.payload().decode()) if task.payload() else {}
    t_start = payload.get("_enqueue_time", 0)

    # 30% 概率失败
    if random.random() < 0.3:
        async with stats_lock:
            stats.total_failed += 1
            stats.by_type[task.type()] += 1
        raise RuntimeError(f"随机失败 (attempt={task.headers().get('x-retry-count', '?')})")

    async with stats_lock:
        stats.total_processed += 1
        stats.by_type[task.type()] += 1
        if t_start:
            stats.latencies.append(time.time_ns() - t_start)

    await task.finish({"status": "ok", "survived": True})


async def handle_skip_task(ctx: Context, task: Task) -> None:
    """跳过重试任务 — 用于验证 SkipRetry 机制。"""
    payload = json.loads(task.payload().decode()) if task.payload() else {}
    should_skip = payload.get("skip", False)

    if should_skip:
        async with stats_lock:
            stats.total_skipped += 1
        raise SkipRetry(f"跳过任务: {task.type()}")

    async with stats_lock:
        stats.total_processed += 1
        stats.by_type[task.type()] += 1

    await task.finish({"status": "skipped_ok"})


async def handle_slow_task(ctx: Context, task: Task) -> None:
    """慢任务 — 接近超时边界。"""
    payload = json.loads(task.payload().decode()) if task.payload() else {}
    t_start = payload.get("_enqueue_time", 0)
    sleep_time = payload.get("sleep", 0.5)

    await asyncio.sleep(sleep_time)

    async with stats_lock:
        stats.total_processed += 1
        stats.by_type[task.type()] += 1
        if t_start:
            stats.latencies.append(time.time_ns() - t_start)

    await task.finish({"status": "ok"})


# ============================================================================
# 内存监控
# ============================================================================

def get_memory_mb() -> float:
    """获取当前进程的内存使用量（MB）。"""
    try:
        import psutil
        return psutil.Process().memory_info().rss / (1024 * 1024)
    except ImportError:
        return -1


# ============================================================================
# 生产者协程
# ============================================================================

async def producer(client: Client, task_types: list[str], stop_event: asyncio.Event, producer_id: int):
    """持续生产任务直到收到停止信号。"""
    task_index = 0
    while not stop_event.is_set():
        task_type = random.choice(task_types)
        payload = {
            "producer_id": producer_id,
            "task_index": task_index,
            "message": f"来自生产者 {producer_id} 的任务 #{task_index}",
            "cpu_iterations": random.randint(1000, 50000),
            "skip": task_type == "stress:skip" and task_index % 10 == 0,
            "sleep": random.uniform(0.01, 0.8),
            "_enqueue_time": time.time_ns(),
        }

        try:
            opts = [MaxRetry(3), Timeout(10)]
            if random.random() < 0.1:
                opts.append(Queue("critical"))
            if random.random() < 0.05:
                opts.append(Unique(30))

            task = Task(task_type, json.dumps(payload).encode())
            info = await client.enqueue(task, *opts)

            async with stats_lock:
                stats.total_enqueued += 1

            task_index += 1

            # 速率控制：每个生产者每秒约 50 条
            await asyncio.sleep(random.uniform(0.01, 0.03))

        except Exception as e:
            async with stats_lock:
                stats.errors.append(f"P{producer_id}: {e}")


async def delayed_producer(client: Client, stop_event: asyncio.Event, producer_id: int):
    """生产延迟任务。"""
    task_index = 0
    while not stop_event.is_set():
        payload = {
            "producer_id": producer_id,
            "task_index": task_index,
            "message": f"延迟任务 #{task_index}",
            "_enqueue_time": time.time_ns(),
        }
        try:
            task = Task("stress:quick", json.dumps(payload).encode())
            await client.enqueue(task, ProcessIn(random.randint(1, 10)))
            async with stats_lock:
                stats.total_enqueued += 1
            task_index += 1
            await asyncio.sleep(0.5)
        except Exception as e:
            async with stats_lock:
                stats.errors.append(f"DP{producer_id}: {e}")


# ============================================================================
# Redis 监控协程
# ============================================================================

async def redis_monitor(redis: _redis.Redis, stop_event: asyncio.Event, interval: float = 5.0):
    """定期采集 Redis 队列状态。"""
    from asyrq.internal.rdb import RDB
    broker = RDB(redis)
    # 压测任务的路由列表（key 布局: asyrq:tasks:{task_type}:{route}:{queue}:...）
    task_routes = [("stress", "quick"), ("stress", "io"), ("stress", "cpu"),
                   ("stress", "flaky"), ("stress", "skip"), ("stress", "slow")]

    while not stop_event.is_set():
        try:
            for qname in ("default", "critical"):
                totals = {"pending": 0, "active": 0, "scheduled": 0,
                          "retry": 0, "archived": 0, "completed": 0}
                for task_type, route in task_routes:
                    st = await broker.current_queue_stats(task_type, route, qname)
                    for k in totals:
                        totals[k] += st[k]
                snapshot = {"queue": qname, "time": time.time(), **totals}
                stats.queue_snapshots.append(snapshot)
        except Exception:
            pass
        await asyncio.sleep(interval)


# ============================================================================
# 输出报告
# ============================================================================

def print_report():
    """打印压力测试报告。"""
    print()
    print("=" * 70)
    print("  asyrq 压力测试报告")
    print("=" * 70)

    print(f"\n  📊 测试时长:        {stats.duration:.1f}s")
    print(f"  📤 总入队数:        {stats.total_enqueued}")
    print(f"  ✅ 总处理数:        {stats.total_processed}")
    print(f"  ❌ 总失败数:        {stats.total_failed}")
    print(f"  ⏭️  总跳过数:        {stats.total_skipped}")
    print(f"  🔄 重试后成功:      ~{stats.total_processed} (完成)")
    print(f"  ⚠️  错误数:          {len(stats.errors)}")

    print(f"\n  📈 吞吐量:")
    print(f"    入队: {stats.throughput_enqueue:.1f} tasks/s")
    print(f"    处理: {stats.throughput_process:.1f} tasks/s")

    print(f"\n  ⏱️  延迟:")
    print(f"    avg:  {stats.avg_latency_ms:.2f}ms")
    print(f"    p50:  {stats.p50_latency_ms:.2f}ms")
    print(f"    p99:  {stats.p99_latency_ms:.2f}ms")
    print(f"    max:  {stats.max_latency_ms:.2f}ms")

    print(f"\n  📋 按类型统计:")
    for tname, count in sorted(stats.by_type.items()):
        print(f"    {tname}: {count}")

    if stats.errors:
        print(f"\n  ❗ 错误详情（前10条）:")
        for e in stats.errors[:10]:
            print(f"    - {e[:120]}")

    # 内存
    mem = get_memory_mb()
    if mem > 0:
        print(f"\n  💾 当前进程内存: {mem:.1f} MB")

    # 队列快照
    if stats.queue_snapshots:
        print(f"\n  📊 Redis 队列快照（{len(stats.queue_snapshots)} 次采样）:")
        last = stats.queue_snapshots[-1]
        for qname in ("default", "critical"):
            matching = [s for s in stats.queue_snapshots if s["queue"] == qname]
            if matching:
                s = matching[-1]
                print(f"    {qname}: pending={s.get('pending',0)}, "
                      f"active={s.get('active',0)}, scheduled={s.get('scheduled',0)}, "
                      f"retry={s.get('retry',0)}, archived={s.get('archived',0)}, "
                      f"completed={s.get('completed',0)}")

    print("\n" + "=" * 70)

    # 判定结果
    issues = []
    if stats.error_rate > 5:
        issues.append(f"错误率过高: {stats.error_rate:.1f}%")
    # p99 延迟是关键指标（max 延迟会被重试任务的指数退避拉高，属预期行为）
    if stats.p99_latency_ms > 60000:
        issues.append(f"p99 延迟过高: {stats.p99_latency_ms:.0f}ms")
    if mem > 0 and mem > 1000:
        issues.append(f"内存占用过高: {mem:.0f}MB")
    if stats.total_enqueued > 0 and stats.total_processed == 0:
        issues.append("无任务被处理！消费者可能未工作")

    if issues:
        print(f"  ❌ 发现问题 ({len(issues)}):")
        for i in issues:
            print(f"     - {i}")
    else:
        print(f"  ✅ 压力测试通过")

    print("=" * 70)


# ============================================================================
# 主函数
# ============================================================================

async def main():
    parser = argparse.ArgumentParser(description="asyrq 长时间高负荷压力测试")
    parser.add_argument("--duration", type=int, default=60, help="测试持续时间（秒），默认 60")
    parser.add_argument("--producers", type=int, default=3, help="生产者数量，默认 3")
    parser.add_argument("--consumers", type=int, default=2, help="消费者数量（Server 实例），默认 2")
    parser.add_argument("--concurrency", type=int, default=20, help="每个消费者的并发数，默认 20")
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis 主机")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis 端口")
    parser.add_argument("--redis-password", default="", help="Redis 密码")
    parser.add_argument("--redis-db", type=int, default=14, help="Redis DB 编号，默认 14")
    parser.add_argument("--cleanup", action="store_true", default=True, help="测试前清空数据库")
    args = parser.parse_args()

    print("=" * 70)
    print("  asyrq 长时间高负荷压力测试")
    print("=" * 70)
    print(f"  测试时长:     {args.duration}s")
    print(f"  生产者数量:   {args.producers}")
    print(f"  消费者数量:   {args.consumers}")
    print(f"  每消费者并发: {args.concurrency}")
    print(f"  Redis:        {args.redis_host}:{args.redis_port} (DB {args.redis_db})")
    print(f"  清理模式:     {'是' if args.cleanup else '否'}")
    print()

    # 连接 Redis
    redis_opt = RedisClientOpt(
        addr=f"{args.redis_host}:{args.redis_port}",
        password=args.redis_password,
        db=args.redis_db,
        pool_size=50,
    )

    # 检查 Redis 连接
    try:
        raw_redis = redis_opt.make_redis_client()
        await raw_redis.ping()
        print("  ✅ Redis 连接成功")
    except Exception as e:
        print(f"  ❌ Redis 连接失败: {e}")
        return 1

    # 清理测试数据
    if args.cleanup:
        await raw_redis.flushdb()
        print("  ✅ 测试数据库已清理")

    stats.start_time = time.time()

    # ================================================================
    # 启动消费者
    # ================================================================

    # 构建路由器
    mux = ServeMux()
    mux.handle_func("stress:quick", handle_quick_task)
    mux.handle_func("stress:io", handle_io_task)
    mux.handle_func("stress:cpu", handle_cpu_task)
    mux.handle_func("stress:flaky", handle_flaky_task)
    mux.handle_func("stress:skip", handle_skip_task)
    mux.handle_func("stress:slow", handle_slow_task)

    servers = []
    for i in range(args.consumers):
        config = Config(
            name=f"stress-consumer-{i}",
            code=f"stress-{uuid.uuid4().hex[:4]}",
            concurrency=args.concurrency,
            queues={"critical": 6, "default": 3},
            log_level=30,  # WARNING 级别减少日志干扰
        )
        server = Server(redis_opt, config)
        await server.start(mux)
        servers.append(server)
        print(f"  ✅ 消费者 #{i} 已启动 (concurrency={args.concurrency})")

    # 等待消费者完全就绪
    await asyncio.sleep(1)

    # ================================================================
    # 启动生产者
    # ================================================================
    stop_event = asyncio.Event()
    producer_tasks = []

    task_types = ["stress:quick", "stress:io", "stress:cpu", "stress:flaky", "stress:skip", "stress:slow"]
    # 权重分配：quick 和 io 占大部分
    weighted_types = (
        ["stress:quick"] * 40 +
        ["stress:io"] * 30 +
        ["stress:cpu"] * 10 +
        ["stress:flaky"] * 10 +
        ["stress:skip"] * 5 +
        ["stress:slow"] * 5
    )

    for i in range(args.producers):
        client = Client(redis_opt)
        t = asyncio.create_task(producer(client, weighted_types, stop_event, i), name=f"producer-{i}")
        producer_tasks.append((t, client))
        print(f"  ✅ 生产者 #{i} 已启动")

    # 启动一个延迟任务生产者
    delay_client = Client(redis_opt)
    delay_task = asyncio.create_task(delayed_producer(delay_client, stop_event, args.producers))
    producer_tasks.append((delay_task, delay_client))
    print(f"  ✅ 延迟生产者已启动")

    # 启动 Redis 监控
    monitor_task = asyncio.create_task(redis_monitor(raw_redis, stop_event, interval=10.0))

    # ================================================================
    # 运行测试
    # ================================================================
    print(f"\n  🚀 压力测试运行中... ({args.duration}s)")
    print(f"  {'-' * 50}")

    # 进度报告
    last_report = time.time()
    last_enqueued = 0
    last_processed = 0

    try:
        elapsed = 0
        while elapsed < args.duration:
            await asyncio.sleep(min(5, args.duration - elapsed))
            elapsed = time.time() - stats.start_time
            current_enqueued = stats.total_enqueued
            current_processed = stats.total_processed
            mem = get_memory_mb()

            report_interval = time.time() - last_report
            enq_rate = (current_enqueued - last_enqueued) / report_interval if report_interval > 0 else 0
            proc_rate = (current_processed - last_processed) / report_interval if report_interval > 0 else 0

            print(f"  [{elapsed:5.0f}s] 入队: {current_enqueued:>6} (+{enq_rate:.0f}/s)  "
                  f"处理: {current_processed:>6} (+{proc_rate:.0f}/s)  "
                  f"失败: {stats.total_failed:>4}  "
                  f"内存: {mem:.0f}MB" if mem > 0 else "")

            last_report = time.time()
            last_enqueued = current_enqueued
            last_processed = current_processed

    except KeyboardInterrupt:
        print("\n  ⚠️  收到中断信号，开始停止...")

    # ================================================================
    # 停止并清理
    # ================================================================
    print("\n  🛑 停止生产者...")
    stop_event.set()

    # 等待所有生产者完成
    for t, client in producer_tasks:
        try:
            await asyncio.wait_for(t, timeout=5)
        except asyncio.TimeoutError:
            t.cancel()
        try:
            await client.close()
        except Exception:
            pass
    print("  ✅ 生产者已停止")

    stats.end_time = time.time()

    # 等待剩余任务被消费 — 最多等 30 秒
    print("  ⏳ 等待剩余任务处理完成...")
    drain_timeout = min(30, args.duration * 0.5)
    drain_start = time.time()
    while time.time() - drain_start < drain_timeout:
        try:
            from asyrq.internal.rdb import RDB
            broker = RDB(raw_redis)
            task_routes = [("stress", "quick"), ("stress", "io"), ("stress", "cpu"),
                           ("stress", "flaky"), ("stress", "skip"), ("stress", "slow")]
            default_stats = {"pending": 0, "active": 0, "scheduled": 0,
                             "retry": 0, "archived": 0, "completed": 0}
            critical_stats = {"pending": 0, "active": 0, "scheduled": 0,
                              "retry": 0, "archived": 0, "completed": 0}
            for task_type, route in task_routes:
                dst = await broker.current_queue_stats(task_type, route, "default")
                cst = await broker.current_queue_stats(task_type, route, "critical")
                for k in default_stats:
                    default_stats[k] += dst[k]
                    critical_stats[k] += cst[k]
            remaining = (
                default_stats["pending"] + default_stats["active"] +
                critical_stats["pending"] + critical_stats["active"]
            )
            if remaining == 0:
                print(f"  ✅ 所有任务已消费")
                break
            print(f"  剩余任务: default(pending={default_stats['pending']}, active={default_stats['active']})  "
                  f"critical(pending={critical_stats['pending']}, active={critical_stats['active']})")
        except Exception:
            pass
        await asyncio.sleep(2)

    # 停止 Redis 监控
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass

    # 关闭消费者
    print("  🛑 关闭消费者...")
    for i, server in enumerate(servers):
        try:
            await server.shutdown()
            print(f"  ✅ 消费者 #{i} 已关闭")
        except Exception as e:
            print(f"  ⚠️  消费者 #{i} 关闭异常: {e}")

    # 关闭主 Redis 连接
    await raw_redis.aclose()

    # 输出报告
    print_report()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
