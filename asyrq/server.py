# server.py - 任务服务器
from __future__ import annotations

import asyncio as _asyncio, os as _os, signal as _signal, socket as _socket, uuid as _uuid
import math as _math, random as _random, json as _json, inspect as _inspect
from dataclasses import dataclass, field
from typing import Optional, Callable, Awaitable, Any

import redis.asyncio as _redis

from .task import Task, TaskInfo
from .handler import Handler, HandlerFunc, Context, wrap_handler_func
from .middleware import MiddlewareFunc, apply_middlewares
from .result_writer import ResultWriter
from .options import Option, apply_options
from .internal.base import (
    TaskMessage, TaskState, ServerInfo, DEFAULT_QUEUE_NAME, DEFAULT_MAX_RETRY, DEFAULT_TIMEOUT,
    DEFAULT_SHUTDOWN_TIMEOUT, SERVER_HEARTBEAT_INTERVAL, LEASE_DURATION,
    DEFAULT_GROUP_MAX_DELAY, DEFAULT_GROUP_GRACE_PERIOD, DEFAULT_GROUP_MAX_SIZE,
    pending_key, active_key, scheduled_key, retry_key, archived_key, completed_key,
    task_key, lease_key, group_key, all_groups_key, unique_key, processed_key, failed_key,
    all_queues_key, servers_key, workers_key, server_info_key, scheduler_key,
    REDIS_PREFIX, ALL_QUEUES_KEY, SERVERS_KEY, split_typename,
)
from .internal.rdb import RDB
from .internal import timeutil as _timeutil
from .internal.log import Logger, DefaultLogger, LogLevel
from .errors import SkipRetry

ErrorHandler = Callable[["Context", Task, Exception], Awaitable[None]]
RetryDelayFunc = Callable[[int, Exception, Task], int]
IsFailureFunc = Callable[[Exception], bool]

@dataclass
class Config:
    concurrency: int = field(default_factory=lambda: (_os.cpu_count() or 4))
    name: str = ""
    code: str = ""
    queues: dict = field(default_factory=lambda: {DEFAULT_QUEUE_NAME: 1})
    strict_priority: bool = False
    error_handler: Optional[ErrorHandler] = None
    logger: Optional[Logger] = None
    log_level: LogLevel = LogLevel.INFO
    log_dir: str = "logs"
    shutdown_timeout: int = DEFAULT_SHUTDOWN_TIMEOUT
    health_check_func: Optional[Callable[[Optional[Exception]], Awaitable[None]]] = None
    health_check_interval: int = 15
    delayed_task_check_interval: int = 1
    task_check_interval: float = 0.1
    retry_delay_func: Optional[RetryDelayFunc] = None
    is_failure_func: Optional[IsFailureFunc] = None
    group_max_delay: int = DEFAULT_GROUP_MAX_DELAY
    group_grace_period: int = DEFAULT_GROUP_GRACE_PERIOD
    group_max_size: int = DEFAULT_GROUP_MAX_SIZE
    group_aggregator: Optional[Callable[[str, list[Task]], Task]] = None
    janitor_interval: int = 8
    janitor_batch_size: int = 100

def default_retry_delay(retry_count: int, error: Exception, task: Task) -> int:
    n = retry_count
    base = int(_math.pow(float(n), 4))
    jitter = _random.randint(0, 30) * (n + 1)
    return base + 15 + jitter

def default_is_failure(error: Exception) -> bool: return True

# ============================================================================
# ServeMux
# ============================================================================

class ServeMux(Handler):
    def __init__(self):
        self._handlers: dict[str, Handler] = {}
        self._prefix_handlers: dict[str, Handler] = {}
        self._middlewares: list[MiddlewareFunc] = []

    def handle(self, pattern: str, handler: Handler) -> None:
        if pattern.endswith(":"): self._prefix_handlers[pattern] = handler
        else: self._handlers[pattern] = handler

    def handle_func(self, pattern: str, func: HandlerFunc) -> None:
        self.handle(pattern, wrap_handler_func(func))

    def task(self, pattern: str):
        def decorator(obj):
            if isinstance(obj, type): self.handle(pattern, obj())
            else: self.handle_func(pattern, obj)
            return obj
        return decorator

    def use(self, *middlewares: MiddlewareFunc) -> None:
        self._middlewares.extend(middlewares)

    async def process_task(self, *args) -> None:
        if len(args) >= 2: ctx, task = args[0], args[1]
        else: task = args[0]; ctx = None
        handler = self._find_handler(task.typename)
        if self._middlewares: handler = apply_middlewares(handler, self._middlewares)
        inner = handler
        if hasattr(inner, '_func'): sig = _inspect.signature(inner._func)
        else: sig = _inspect.signature(inner.process_task)
        params = list(sig.parameters.values())
        num_params = len([p for p in params if p.name != "self"])
        if num_params >= 2:
            if ctx is None: ctx = Context(); ctx.task_id = task.id; ctx.timeout = task.timeout; ctx._start_ns = task._start_ns
            await handler.process_task(ctx, task)
        else: await handler.process_task(task)

    def _find_handler(self, typename: str) -> Handler:
        if typename in self._handlers: return self._handlers[typename]
        best_match, best_len = None, 0
        for prefix, handler in self._prefix_handlers.items():
            if typename.startswith(prefix) and len(prefix) > best_len:
                best_match, best_len = handler, len(prefix)
        if best_match: return best_match
        if "" in self._prefix_handlers: return self._prefix_handlers[""]
        raise ValueError(f"未找到任务 '{typename}' 的处理器")

    def routes(self) -> list[str]:
        patterns = list(self._handlers.keys()) + list(self._prefix_handlers.keys())
        for h in self._handlers.values():
            if isinstance(h, ServeMux): patterns.extend(h.routes())
        for h in self._prefix_handlers.values():
            if isinstance(h, ServeMux): patterns.extend(h.routes())
        return sorted(set(patterns))

# ============================================================================
# Server
# ============================================================================

class Server:
    def __init__(
        self,
        redis_conn: Any = None,
        redis_addr: str = "127.0.0.1:6379",
        redis_password: str = "",
        redis_db: int = 0,
        name: str = "",
        code: str = "",
        concurrency: int = 10,
        redis_client: Optional[_redis.Redis] = None,
        config: Optional[Config] = None,
        **kwargs,
    ):
        self._config = config or Config(
            name=name, code=code, concurrency=concurrency,
            **{k: v for k, v in kwargs.items() if hasattr(Config, k)},
        )
        self._logger = self._config.logger or DefaultLogger(level=self._config.log_level, log_dir=self._config.log_dir, name="server")
        self._host = _socket.gethostname()
        self._pid = _os.getpid()
        self._server_id = self._config.code or (f"{self._config.name}:{_uuid.uuid4().hex[:8]}" if self._config.name else _uuid.uuid4().hex)
        self._code = self._config.code
        # 队列按权重降序排列（严格优先级模式下高权重队列优先消费）
        self._queues = sorted(
            self._config.queues.keys(),
            key=lambda q: self._config.queues[q],
            reverse=True,
        )

        if redis_client:
            self._redis = redis_client; self._owns_connection = False; self._redis_created = True
        elif redis_conn is not None:
            if not hasattr(redis_conn, "make_redis_client"):
                raise TypeError("redis_conn 必须是 RedisClientOpt / RedisFailoverClientOpt / RedisClusterClientOpt")
            self._redis_opts = redis_conn
            self._owns_connection = True; self._redis_created = False
        else:
            from .connection import RedisClientOpt
            self._redis_opts = RedisClientOpt(addr=redis_addr, password=redis_password, db=redis_db)
            self._owns_connection = True; self._redis_created = False

        self._broker: Optional[RDB] = None
        self._mux = ServeMux()
        self._handler: Handler | ServeMux | None = None
        self._state: str = "new"
        self._shutdown_event: Optional[_asyncio.Event] = None
        self._quiet_event: Optional[_asyncio.Event] = None
        self._bg_tasks: list[_asyncio.Task] = []
        self._worker_tasks: set[_asyncio.Task] = set()
        self._active_task_ids: dict[str, str] = {}
        self._active_workers: dict[str, dict[str, Any]] = {}
        self._task_types: list[str] = []  # Service 知道的 task type，由 @app.task 注册填充

    async def run(self, handler: Handler | ServeMux | None = None) -> None:
        await self.start(handler)
        await self._wait_for_signal()
        await self.shutdown()

    async def start(self, handler: Handler | ServeMux | None = None) -> None:
        if self._state != "new": raise RuntimeError(f"服务器已启动，当前状态: {self._state}")
        self._state = "active"
        self._handler = handler or self._mux
        loop = _asyncio.get_running_loop()
        self._shutdown_event = _asyncio.Event()
        self._quiet_event = _asyncio.Event()
        if not self._redis_created:
            self._redis = self._redis_opts.make_redis_client(); self._redis_created = True
        if self._broker is None:
            self._broker = RDB(self._redis, self._logger)

        self._bg_tasks.extend([
            loop.create_task(self._heartbeat_loop(), name="heartbeat"),
            loop.create_task(self._forwarder_loop(), name="forwarder"),
            loop.create_task(self._recoverer_loop(), name="recoverer"),
            loop.create_task(self._janitor_loop(), name="janitor"),
            loop.create_task(self._healthcheck_loop(), name="healthcheck"),
        ])
        if self._config.group_aggregator:
            self._bg_tasks.append(loop.create_task(self._aggregator_loop(), name="aggregator"))
        self._bg_tasks.append(loop.create_task(self._processor(), name="processor"))

        routes_info = ", ".join(self._routes()) or "(无)"
        self._logger.info(
            "━━━ Server 启动 ━━━ name=%s code=%s concurrency=%s routes=[%s]",
            self._config.name, self._config.code, self._config.concurrency, routes_info,
        )

    def task(self, pattern: str):
        def decorator(obj):
            full_route = f"{self._code}:{pattern}" if self._code else pattern
            if isinstance(obj, type): self._mux.handle(full_route, obj())
            else: self._mux.handle_func(full_route, obj)
            return obj
        return decorator

    def use(self, *mws): self._mux.use(*mws)

    def _routes(self) -> list[str]:
        """返回当前生效 handler 注册的路由（外部传入的 mux 也计入）。"""
        handler = self._handler if isinstance(self._handler, ServeMux) else self._mux
        return handler.routes()

    def _task_routes(self) -> list[str]:
        """返回可消费的路由列表。

        精确路由（如 "pmos_liaoning:rqdj"）可映射到 key 前缀参与出队；
        前缀路由（如 "email:"）和 catch-all（""）无法枚举 key，跳过。
        """
        return [r for r in self._routes() if r and not r.endswith(":")]

    async def shutdown(self) -> None:
        if self._state in ("closed", "new"): return
        self._logger.info("开始优雅关闭...")
        self._state = "stopped"
        self._shutdown_event.set()
        # 取消后台循环任务（不取消在途 worker，让其优雅完成）
        for t in self._bg_tasks:
            if not t.done(): t.cancel()
        if self._bg_tasks: await _asyncio.gather(*self._bg_tasks, return_exceptions=True)
        # 等待在途任务完成（最多 shutdown_timeout 秒），超时后强制取消
        if self._worker_tasks:
            done, pending = await _asyncio.wait(
                self._worker_tasks, timeout=self._config.shutdown_timeout
            )
            for t in pending: t.cancel()
            if pending: await _asyncio.gather(*pending, return_exceptions=True)
        try: await self._broker.clear_server_state(self._host, self._pid, self._server_id)
        except Exception: pass
        if self._owns_connection:
            try: await self._broker.close()
            except Exception: pass
        self._state = "closed"
        self._logger.info("服务器已关闭")

    async def _processor(self) -> None:
        semaphore = _asyncio.Semaphore(self._config.concurrency)
        while not self._shutdown_event.is_set():
            if self._state == "quiet": await _asyncio.sleep(0.1); continue
            if self._state == "stopped": break
            try:
                # 按「路由 × 队列」消费，权重模式传给 broker 做加权随机
                msg = await self._broker.dequeue(
                    self._task_routes(), self._queues, self._config.strict_priority,
                    weights=self._config.queues,
                )
                if msg is None:
                    await _asyncio.sleep(self._config.task_check_interval); continue
                await semaphore.acquire()
                worker_task = _asyncio.create_task(self._worker(msg, semaphore), name=f"worker-{msg.id[:8]}")
                self._worker_tasks.add(worker_task)
                worker_task.add_done_callback(self._worker_tasks.discard)
            except _asyncio.CancelledError: break
            except Exception as e: self._logger.error(f"处理器错误: {e}"); await _asyncio.sleep(1)
        self._logger.info("处理器已停止，等待 worker 完成...")

    async def _worker(self, msg: TaskMessage, semaphore: _asyncio.Semaphore) -> None:
        task_id = msg.id; tid = task_id[:12]; t0 = _timeutil.now()
        try:
            if not msg.type:
                # 防御：空类型名的历史/脏数据直接归档，避免 worker 崩溃
                await self._broker.archive(msg, "任务类型名为空")
                return
            task = Task(typename=msg.type, payload=msg.payload, headers=msg.headers)
            writer = ResultWriter(task_id, self._broker, msg.queue, typename=msg.type)
            task._set_writer(writer)
            task._setup_context(task_id, msg.queue, msg.timeout, msg.deadline, t0)
            task_type, route = split_typename(msg.type)
            self._active_task_ids[task_id] = (msg.queue, task_type, route)

            inner = self._handler
            if hasattr(inner, '_func'): sig = _inspect.signature(inner._func)
            else: sig = _inspect.signature(inner.process_task)
            params = list(sig.parameters.values())
            num_params = len([p for p in params if p.name != "self"])

            try:
                if num_params >= 2:
                    ctx = Context(); ctx.task_id = task_id; ctx.timeout = msg.timeout; ctx.deadline = msg.deadline; ctx._start_ns = t0
                    if msg.timeout > 0: await _asyncio.wait_for(self._handler.process_task(ctx, task), timeout=msg.timeout)
                    else: await self._handler.process_task(ctx, task)
                else:
                    if msg.timeout > 0: await _asyncio.wait_for(self._handler.process_task(task), timeout=msg.timeout)
                    else: await self._handler.process_task(task)

                await self._broker.done(msg)
                elapsed = (_timeutil.now() - t0) / 1e9
                self._logger.info("✓ [%s] %s 完成 耗时 %.2fs", tid, msg.type, elapsed)

            except _asyncio.TimeoutError:
                await self._handle_task_error(msg, task, TimeoutError("超时"))
            except SkipRetry:
                self._logger.warn("[%s] 跳过重试", tid)
                await self._broker.archive(msg, "skip retry")
            except Exception as e:
                self._logger.error("[%s] 失败: %s", tid, e)
                await self._handle_task_error(msg, task, e)
        finally:
            self._active_task_ids.pop(task_id, None); semaphore.release()

    async def _handle_task_error(self, msg: TaskMessage, task: Task, error: Exception) -> None:
        error_msg = str(error)
        if self._config.error_handler:
            try:
                ctx = Context(); ctx.task_id = msg.id; ctx.timeout = msg.timeout; ctx._start_ns = _timeutil.now()
                await self._config.error_handler(ctx, task, error)
            except Exception: pass
        if isinstance(error, SkipRetry):
            await self._broker.archive(msg, error_msg); return
        if msg.retried >= msg.retry:
            await self._broker.archive(msg, error_msg); return
        delay_func = self._config.retry_delay_func or default_retry_delay
        delay_secs = delay_func(msg.retried, error, task)
        process_at = _timeutil.now() + delay_secs * 1_000_000_000
        is_failure_func = self._config.is_failure_func or default_is_failure
        await self._broker.retry(msg, process_at, error_msg, is_failure_func(error))

    async def _heartbeat_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                worker_list = [{"task_id": tid, "queue": meta[0]} for tid, meta in self._active_task_ids.items()]
                info = ServerInfo(
                    host=self._host, pid=self._pid, server_id=self._server_id,
                    concurrency=self._config.concurrency, queues=self._config.queues,
                    strict_priority=self._config.strict_priority, status=self._state,
                    start_time=_timeutil.now(), active_worker_count=len(self._active_task_ids),
                    routes=self._routes(),
                )
                await self._broker.write_server_state(info, worker_list, SERVER_HEARTBEAT_INTERVAL * 2)
                if self._active_task_ids:
                    queue_tasks: dict[tuple[str, str, str], list[str]] = {}
                    for tid, meta in self._active_task_ids.items():
                        queue_tasks.setdefault(meta, []).append(tid)
                    for (qname, task_type, route), tids in queue_tasks.items():
                        await self._broker.extend_lease(task_type, route, qname, tids, LEASE_DURATION)
            except Exception as e: self._logger.warn("心跳失败: %s", e)
            try: await _asyncio.wait_for(self._shutdown_event.wait(), timeout=SERVER_HEARTBEAT_INTERVAL)
            except _asyncio.TimeoutError: pass

    async def _forwarder_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await self._broker.forward_if_ready(self._task_routes(), self._queues)
            except Exception as e: self._logger.warn("前移失败: %s", e)
            try: await _asyncio.wait_for(self._shutdown_event.wait(), timeout=self._config.delayed_task_check_interval)
            except _asyncio.TimeoutError: pass

    async def _recoverer_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                expired = await self._broker.list_lease_expired(self._task_routes(), self._queues)
                for msg in expired:
                    if msg.retried >= msg.retry: await self._broker.archive(msg, "孤儿任务恢复")
                    else: await self._broker.retry(msg, _timeutil.now(), "孤儿任务恢复", True)
            except Exception as e: self._logger.warn("恢复失败: %s", e)
            try: await _asyncio.wait_for(self._shutdown_event.wait(), timeout=60)
            except _asyncio.TimeoutError: pass

    async def _janitor_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                for task_route in self._task_routes():
                    task_type, route = split_typename(task_route)
                    for qname in self._queues:
                        ckey = completed_key(task_type, route, qname)
                        await self._redis.zremrangebyscore(ckey, "-inf", _timeutil.now())
            except Exception as e: self._logger.warn("清理失败: %s", e)
            try: await _asyncio.wait_for(self._shutdown_event.wait(), timeout=self._config.janitor_interval)
            except _asyncio.TimeoutError: pass

    async def _healthcheck_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                ok = await self._broker.ping()
                if not ok and self._config.health_check_func: await self._config.health_check_func(Exception("Redis 健康检查失败"))
            except Exception as e:
                if self._config.health_check_func: await self._config.health_check_func(e)
            try: await _asyncio.wait_for(self._shutdown_event.wait(), timeout=self._config.health_check_interval)
            except _asyncio.TimeoutError: pass

    async def _aggregator_loop(self) -> None:
        if not self._config.group_aggregator: return
        while not self._shutdown_event.is_set():
            try:
                for task_route in self._task_routes():
                    task_type, route = split_typename(task_route)
                    for qname in self._queues:
                        groups = await self._broker.list_groups(task_type, route, qname)
                        for gname in groups:
                            task_ids = await self._broker.aggregation_check(
                                task_type, route, qname, gname,
                                self._config.group_max_delay,
                                self._config.group_max_size,
                                self._config.group_grace_period,
                            )
                            if not task_ids:
                                continue
                            # aggregation_check 的 Lua 脚本已原子取出并删除组内任务 ID
                            tasks = []
                            for tid_str in task_ids:
                                tkey = task_key(task_type, route, qname, tid_str)
                                data = await self._redis.hget(tkey, "msg")
                                if data:
                                    msg = TaskMessage.from_json(data.decode())
                                    tasks.append(Task(msg.type, msg.payload))
                            if tasks and self._config.group_aggregator:
                                aggregated = self._config.group_aggregator(gname, tasks)
                                amsg = TaskMessage(type=aggregated.typename, payload=aggregated.payload, id="", queue=qname)
                                await self._broker.enqueue(amsg)
                                for tid_str in task_ids:
                                    await self._redis.delete(task_key(task_type, route, qname, tid_str))
                                await self._redis.srem(all_groups_key(task_type, route, qname), gname)
                                self._logger.info("聚合组 %s: %d 个任务已聚合", gname, len(tasks))
            except Exception as e: self._logger.warn("聚合失败: %s", e)
            try: await _asyncio.wait_for(self._shutdown_event.wait(), timeout=2)
            except _asyncio.TimeoutError: pass

    async def _wait_for_signal(self) -> None:
        loop = _asyncio.get_running_loop()
        stop_event = _asyncio.Event()
        try:
            for sig in (_signal.SIGINT, _signal.SIGTERM): loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError: pass
        while not stop_event.is_set() and not self._shutdown_event.is_set():
            await _asyncio.sleep(0.1)
