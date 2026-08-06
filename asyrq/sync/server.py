# asyrq/sync/server.py - 同步版任务服务器（线程池模型）
from __future__ import annotations

import os as _os
import socket as _socket
import signal as _signal
import threading as _threading
import time as _time
import uuid as _uuid
import math as _math
import random as _random
import concurrent.futures as _futures
from dataclasses import dataclass, field
from typing import Optional, Callable

import redis as _redis

from ..task import Task
from ..options import Option, apply_options
from ..internal.base import (
    TaskMessage, TaskState, ServerInfo,
    DEFAULT_QUEUE_NAME, DEFAULT_MAX_RETRY, DEFAULT_TIMEOUT,
    DEFAULT_SHUTDOWN_TIMEOUT, SERVER_HEARTBEAT_INTERVAL, LEASE_DURATION,
    DEFAULT_GROUP_MAX_DELAY, DEFAULT_GROUP_GRACE_PERIOD, DEFAULT_GROUP_MAX_SIZE,
    completed_key, split_typename,
)
from ..internal import timeutil as _timeutil
from ..internal.log import Logger, DefaultLogger, LogLevel
from ..connection import RedisClientOpt
from ..errors import SkipRetry
from ..handler import Context
from ..middleware import MiddlewareFunc
from .rdb import SyncRDB
from .result_writer import SyncResultWriter
from .handler import SyncHandler, SyncServeMux, wrap_sync_handler_func


# ======== 默认重试延迟 ========
def default_retry_delay(retry_count: int, error: Exception, task: Task) -> int:
    n = retry_count
    base = int(_math.pow(float(n), 4))
    jitter = _random.randint(0, 30) * (n + 1)
    return base + 15 + jitter


def default_is_failure(error: Exception) -> bool:
    return True


# ======== ServeMux（同步版） ========
# 直接使用 sync.handler.SyncServeMux


# ======== Config ========
@dataclass
class SyncConfig:
    """同步版服务器配置。"""
    concurrency: int = field(default_factory=lambda: (_os.cpu_count() or 4))
    name: str = ""
    code: str = ""
    queues: dict = field(default_factory=lambda: {DEFAULT_QUEUE_NAME: 1})
    strict_priority: bool = False
    error_handler: Optional[Callable] = None
    logger: Optional[Logger] = None
    log_level: LogLevel = LogLevel.INFO
    log_dir: str = "logs"
    shutdown_timeout: int = DEFAULT_SHUTDOWN_TIMEOUT
    health_check_interval: int = 15
    delayed_task_check_interval: int = 1
    task_check_interval: float = 0.1
    retry_delay_func: Optional[Callable] = None
    is_failure_func: Optional[Callable] = None
    group_max_delay: int = DEFAULT_GROUP_MAX_DELAY
    group_grace_period: int = DEFAULT_GROUP_GRACE_PERIOD
    group_max_size: int = DEFAULT_GROUP_MAX_SIZE
    group_aggregator: Optional[Callable] = None
    janitor_interval: int = 8
    janitor_batch_size: int = 100


# ======== SyncServer ========
class SyncServer:
    """同步版任务服务器（线程池模型）。

    Usage:
        app = SyncServer(RedisClientOpt(...), SyncConfig(name="worker", concurrency=10))

        @app.task("email:send")
        def handle_email(ctx, task):
            data = json.loads(task.payload())
            print(f"发送邮件: {data['to']}")

        app.run()  # 阻塞，Ctrl+C 退出
    """

    def __init__(self, redis_conn, config: Optional[SyncConfig] = None):
        self._config = config or SyncConfig()
        self._logger = self._config.logger or DefaultLogger(
            level=self._config.log_level, log_dir=self._config.log_dir, name="server"
        )

        self._host = _socket.gethostname()
        self._pid = _os.getpid()
        if self._config.code:
            self._server_id = self._config.code
        elif self._config.name:
            self._server_id = f"{self._config.name}:{_uuid.uuid4().hex[:8]}"
        else:
            self._server_id = _uuid.uuid4().hex

        # 同步版每个线程需要独立 Redis 连接（redis-py 的 ConnectionPool 是线程安全的）
        if isinstance(redis_conn, _redis.Redis):
            self._redis = redis_conn
            self._owns_connection = False
        else:
            self._redis_opts = redis_conn
            self._redis = redis_conn.make_sync_redis_client()
            self._owns_connection = True

        self._broker = SyncRDB(self._redis, self._logger)
        self._mux = SyncServeMux()
        self._handler: Optional[SyncHandler] = None
        self._state = "new"

        self._shutdown_event = _threading.Event()
        self._active_task_ids: dict = {}
        self._active_task_ids_lock = _threading.Lock()

        self._queues = sorted(
            self._config.queues.keys(),
            key=lambda q: self._config.queues[q],
            reverse=True,
        )
        self._threads: list[_threading.Thread] = []
        self._executor: Optional[_futures.ThreadPoolExecutor] = None

    # ======== 公共 API ========

    def run(self, handler: Optional[SyncHandler] = None) -> None:
        """阻塞运行直到 Ctrl+C。"""
        self.start(handler)
        self._wait_for_signal()
        self.shutdown()

    def start(self, handler: Optional[SyncHandler] = None) -> None:
        """启动服务器。"""
        if self._state != "new":
            raise RuntimeError(f"服务器已启动: {self._state}")

        self._state = "active"
        self._handler = handler or self._mux
        self._executor = _futures.ThreadPoolExecutor(max_workers=self._config.concurrency)

        # 启动日志
        name_tag = f" name={self._config.name}" if self._config.name else ""
        code_tag = f" code={self._config.code}" if self._config.code else ""
        routes_info = ", ".join(self._routes()) or "(无)"
        self._logger.info(
            f"━━━ SyncServer 启动 ━━━{name_tag}{code_tag} "
            f"concurrency={self._config.concurrency} queues={self._config.queues} "
            f"routes=[{routes_info}]"
        )

        # 启动后台线程
        self._start_thread(self._processor_loop, "processor")
        self._start_thread(self._heartbeat_loop, "heartbeat")
        self._start_thread(self._forwarder_loop, "forwarder")
        self._start_thread(self._recoverer_loop, "recoverer")
        self._start_thread(self._janitor_loop, "janitor")
        self._start_thread(self._healthcheck_loop, "healthcheck")

    def shutdown(self) -> None:
        """优雅关闭。"""
        if self._state in ("closed", "new"):
            return

        self._logger.info("开始优雅关闭...")
        self._state = "stopped"
        self._shutdown_event.set()

        # 等待后台线程退出
        for t in self._threads:
            t.join(timeout=5)

        # 关闭线程池
        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=True)

        # 清理 Redis
        try:
            self._broker.clear_server_state(self._host, self._pid, self._server_id)
        except Exception:
            pass

        if self._owns_connection:
            try:
                self._broker.close()
            except Exception:
                pass

        self._state = "closed"
        self._logger.info("服务器已关闭")

    def task(self, pattern: str):
        """FastAPI 风格装饰器。"""
        def decorator(obj):
            if isinstance(obj, type):
                self._mux.handle(pattern, obj())
            else:
                # 同步函数 -> 包装为 Handler
                self._mux.handle_func(pattern, obj)
            return obj
        return decorator

    def use(self, *middlewares):
        """注册中间件。"""
        self._mux.use(*middlewares)

    def _routes(self) -> list[str]:
        """返回当前生效 handler 注册的路由（外部传入的 mux 也计入）。"""
        handler = self._handler if isinstance(self._handler, SyncServeMux) else self._mux
        return handler.routes()

    def _task_routes(self) -> list[str]:
        """返回可消费的路由列表（精确路由；前缀/catch-all 跳过）。"""
        return [r for r in self._routes() if r and not r.endswith(":")]

    # ======== 后台线程 ========

    def _start_thread(self, target, name: str):
        t = _threading.Thread(target=target, name=name, daemon=True)
        t.start()
        self._threads.append(t)

    def _processor_loop(self):
        """处理器循环：dequeue -> 提交线程池。"""
        while not self._shutdown_event.is_set():
            if self._state == "quiet":
                _time.sleep(0.1)
                continue
            try:
                msg = self._broker.dequeue(
                    self._task_routes(), self._queues, self._config.strict_priority,
                    weights=self._config.queues,
                )
                if msg is None:
                    _time.sleep(self._config.task_check_interval)
                    continue
                # 提交到线程池
                self._executor.submit(self._worker, msg)
            except Exception as e:
                self._logger.error(f"处理器错误: {e}")
                _time.sleep(1)

    def _worker(self, msg: TaskMessage):
        """Worker：在线程池中处理单个任务。"""
        task_id = msg.id
        tid = task_id[:12]
        t0 = _timeutil.now()

        with self._active_task_ids_lock:
            task_type, route = split_typename(msg.type)
            self._active_task_ids[task_id] = (msg.queue, task_type, route)

        try:
            task = Task(typename=msg.type, payload=msg.payload, headers=msg.headers)
            # 每个 worker 线程用独立的 Redis 连接写结果
            worker_redis = self._redis
            if self._owns_connection:
                worker_redis = self._redis_opts.make_sync_redis_client()
            worker_broker = SyncRDB(worker_redis, self._logger)
            writer = SyncResultWriter(
                task_id, worker_broker, msg.queue,
                typename=msg.type, result_key=msg.headers.get("_result_key", ""),
            )
            task._writer = writer
            task._setup_context(task_id, msg.queue, msg.timeout, msg.deadline, t0)

            ctx = Context()
            ctx.task_id = task_id
            ctx.timeout = msg.timeout
            ctx.deadline = msg.deadline
            ctx._start_ns = t0

            self._logger.debug("▶ [%s] %s queue=%s", tid, msg.type, msg.queue)

            try:
                self._handler.process_task(ctx, task)
                self._broker.done(msg)
                elapsed = (_timeutil.now() - t0) / 1e9
                self._logger.info("✓ [%s] %s 完成 耗时 %.2fs", tid, msg.type, elapsed)

            except SkipRetry:
                self._broker.archive(msg, "skip retry")
                self._logger.info("⚠ [%s] %s SkipRetry 归档", tid, msg.type)

            except Exception as e:
                self._logger.error("✗ [%s] %s 失败: %s", tid, msg.type, e)
                self._handle_task_error(msg, task, e)

            finally:
                if self._owns_connection and worker_redis is not self._redis:
                    worker_redis.close()

        finally:
            with self._active_task_ids_lock:
                self._active_task_ids.pop(task_id, None)

    def _handle_task_error(self, msg: TaskMessage, task: Task, error: Exception):
        """处理错误：重试或归档。"""
        error_msg = str(error)

        if self._config.error_handler:
            try:
                ctx = Context()
                ctx.task_id = msg.id
                ctx._start_ns = _timeutil.now()
                self._config.error_handler(ctx, task, error)
            except Exception:
                pass

        if isinstance(error, SkipRetry):
            self._broker.archive(msg, error_msg)
            return

        if msg.retried >= msg.retry:
            self._broker.archive(msg, error_msg)
            self._logger.info("📦 [%s] 归档（重试耗尽 %s/%s）", msg.id[:12], msg.retried, msg.retry)
            return

        delay_func = self._config.retry_delay_func or default_retry_delay
        delay_secs = delay_func(msg.retried, error, task)
        process_at = _timeutil.now() + delay_secs * 1_000_000_000

        is_failure_func = self._config.is_failure_func or default_is_failure
        is_failure = is_failure_func(error)

        self._broker.retry(msg, process_at, error_msg, is_failure)
        self._logger.info(
            "🔄 [%s] 重试 %s/%s delay=%ss err=%s",
            msg.id[:12], msg.retried + 1, msg.retry, delay_secs, error_msg[:80]
        )

    def _heartbeat_loop(self):
        """心跳线程。"""
        while not self._shutdown_event.is_set():
            try:
                with self._active_task_ids_lock:
                    workers = [{"task_id": tid, "queue": meta[0]} for tid, meta in self._active_task_ids.items()]

                info = ServerInfo(
                    host=self._host, pid=self._pid, server_id=self._server_id,
                    concurrency=self._config.concurrency, queues=self._config.queues,
                    strict_priority=self._config.strict_priority, status=self._state,
                    start_time=_timeutil.now(), active_worker_count=len(workers),
                    routes=self._routes(),
                )
                self._broker.write_server_state(info, workers, SERVER_HEARTBEAT_INTERVAL * 2)

                # 延长租约
                with self._active_task_ids_lock:
                    task_ids = dict(self._active_task_ids)
                if task_ids:
                    queue_tasks: dict = {}
                    for tid, meta in task_ids.items():
                        queue_tasks.setdefault(meta, []).append(tid)
                    for (qname, task_type, route), tids in queue_tasks.items():
                        self._broker.extend_lease(task_type, route, qname, tids, LEASE_DURATION)

            except Exception as e:
                self._logger.warn(f"心跳失败: {e}")

            self._shutdown_event.wait(timeout=SERVER_HEARTBEAT_INTERVAL)

    def _forwarder_loop(self):
        """前移线程。"""
        while not self._shutdown_event.is_set():
            try:
                self._broker.forward_if_ready(self._task_routes(), self._queues)
            except Exception as e:
                self._logger.warn(f"前移失败: {e}")
            self._shutdown_event.wait(timeout=self._config.delayed_task_check_interval)

    def _recoverer_loop(self):
        """恢复线程。"""
        while not self._shutdown_event.is_set():
            try:
                expired = self._broker.list_lease_expired(self._task_routes(), self._queues)
                for msg in expired:
                    if msg.retried >= msg.retry:
                        self._broker.archive(msg, "孤儿任务恢复")
                    else:
                        self._broker.retry(msg, _timeutil.now(), "孤儿任务恢复", True)
                    self._logger.info("🔄 恢复孤儿任务: %s", msg.id[:12])
            except Exception as e:
                self._logger.warn(f"恢复失败: {e}")
            self._shutdown_event.wait(timeout=60)

    def _janitor_loop(self):
        """清理线程。"""
        while not self._shutdown_event.is_set():
            try:
                now = _timeutil.now()
                for task_route in self._task_routes():
                    task_type, route = split_typename(task_route)
                    for qname in self._queues:
                        ckey = completed_key(task_type, route, qname)
                        self._broker._client.zremrangebyscore(ckey, "-inf", now)
            except Exception as e:
                self._logger.warn(f"清理失败: {e}")
            self._shutdown_event.wait(timeout=self._config.janitor_interval)

    def _healthcheck_loop(self):
        """健康检查线程。"""
        while not self._shutdown_event.is_set():
            try:
                ok = self._broker.ping()
                if not ok and self._config.error_handler:
                    pass
            except Exception:
                pass
            self._shutdown_event.wait(timeout=self._config.health_check_interval)

    def _wait_for_signal(self):
        """等待 Ctrl+C 信号。"""
        try:
            _signal.signal(_signal.SIGINT, lambda *_: self._shutdown_event.set())
            _signal.signal(_signal.SIGTERM, lambda *_: self._shutdown_event.set())
        except (ValueError, AttributeError):
            pass  # 非主线程无法注册信号

        while not self._shutdown_event.is_set():
            self._shutdown_event.wait(timeout=0.5)
