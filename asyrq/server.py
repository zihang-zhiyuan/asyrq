# server.py — 任务服务器模块
# 提供任务消费、路由和并发处理功能，1:1 对应 Go asynq 的 Server 和 ServeMux

from __future__ import annotations
from typing import Any, Awaitable, Callable
import asyncio as _asyncio
import os as _os
import signal as _signal
import socket as _socket
import uuid as _uuid
import math as _math
import random as _random
from dataclasses import dataclass, field

import redis.asyncio as _redis

from .task import Task, TaskInfo
from .handler import Handler, HandlerFunc, Context, wrap_handler_func
from .middleware import MiddlewareFunc, apply_middlewares
from .result_writer import ResultWriter
from .options import Option, apply_options
from .internal.base import (
    TaskMessage, TaskState, ServerInfo,
    DEFAULT_QUEUE_NAME, DEFAULT_MAX_RETRY, DEFAULT_TIMEOUT,
    DEFAULT_SHUTDOWN_TIMEOUT,
    SERVER_HEARTBEAT_INTERVAL, LEASE_DURATION,
    DEFAULT_GROUP_MAX_DELAY, DEFAULT_GROUP_GRACE_PERIOD, DEFAULT_GROUP_MAX_SIZE,
    completed_key,
)
from .internal.rdb import RDB, _get_lua_script
from .internal import timeutil as _timeutil
from .internal.log import Logger, DefaultLogger, LogLevel
from .connection import RedisClientOpt
from .errors import SkipRetry

# ============================================================================
# 配置类型
# ============================================================================

# 错误处理函数类型 — 当 Handler 返回错误时调用
# 1:1 对应 Go asynq 的 ErrorHandler
ErrorHandler = Callable[["Context", Task, Exception], Awaitable[None]]

# 重试延迟计算函数类型 — 根据重试次数计算延迟时间
# 1:1 对应 Go asynq 的 RetryDelayFunc
RetryDelayFunc = Callable[[int, Exception, Task], int]

# 失败判定函数类型 — 判断错误是否算作"失败"
IsFailureFunc = Callable[[Exception], bool]

@dataclass
class Config:
    """服务器配置。

    1:1 对应 Go asynq 的 Config 结构体。
    """
    # 最大并发处理任务数，默认 CPU 核心数
    concurrency: int = field(default_factory=lambda: (_os.cpu_count() or 4))
    name: str = ""
    code: str = ""

    # 队列及优先级权重映射 {"queue_name": priority}
    # 默认只有 "default" 队列，权重 1
    queues: Dict[str, int] = field(default_factory=lambda: {DEFAULT_QUEUE_NAME: 1})

    # 严格优先级模式：高优先级队列必须完全清空后才处理低优先级队列
    strict_priority: bool = False

    # 任务处理错误时的回调
    error_handler: Optional[ErrorHandler] = None

    # 自定义日志器
    logger: Optional[Logger] = None
    log_level: LogLevel = LogLevel.INFO
    log_dir: str = "logs"

    # 优雅关闭最长等待时间（秒），超时后强制终止
    shutdown_timeout: int = DEFAULT_SHUTDOWN_TIMEOUT

    # 健康检查回调，定期报告 Redis 连接状况
    health_check_func: Optional[Callable[[Optional[Exception]], Awaitable[None]]] = None

    # 健康检查间隔（秒）
    health_check_interval: int = 15

    # 定时/重试任务检查间隔（秒）
    delayed_task_check_interval: int = 5

    # 空队列时轮询间隔（秒）
    task_check_interval: int = 1

    # 重试延迟计算函数，默认使用指数退避
    retry_delay_func: Optional[RetryDelayFunc] = None

    # 失败判定函数，默认所有异常都是失败
    is_failure_func: Optional[IsFailureFunc] = None

    # ======== 任务组聚合配置 ========

    # 聚合组最大延迟（秒），超时强制聚合
    group_max_delay: int = DEFAULT_GROUP_MAX_DELAY

    # 聚合组宽限期（秒），第一个任务入组后等待
    group_grace_period: int = DEFAULT_GROUP_GRACE_PERIOD

    # 聚合组最大任务数，达到后强制聚合
    group_max_size: int = DEFAULT_GROUP_MAX_SIZE

    # 聚合函数 — 将一组任务合并为一个任务
    group_aggregator: Optional[Callable[[str, list[Task]], Task]] = None

    # ======== 清理配置 ========

    # 已完成任务清理间隔（秒）
    janitor_interval: int = 8

    # 每次清理最多删除的已完成任务数
    janitor_batch_size: int = 100

# ============================================================================
# 默认重试延迟函数
# ============================================================================

def default_retry_delay(retry_count: int, error: Exception, task: Task) -> int:
    """默认指数退避重试延迟。

    1:1 对应 Go asynq 的 DefaultRetryDelayFunc。

    公式: seconds = n^4 + 15 + rand(0..30) * (n + 1)

    Args:
        retry_count: 当前重试次数（从0开始）
        error: 错误对象
        task: 任务对象

    Returns:
        int: 延迟秒数
    """
    n = retry_count  # 当前重试次数
    # 指数退避: n^4 提供快速增长基础
    base = int(_math.pow(float(n), 4))  # n 的四次方
    # 固定偏移 + 随机抖动（防止惊群效应）
    jitter = _random.randint(0, 30) * (n + 1)  # 随机抖动
    return base + 15 + jitter  # 返回总延迟秒数

def default_is_failure(error: Exception) -> bool:
    """默认失败判定：所有错误都是失败。"""
    return True

# ============================================================================
# ServeMux — 任务路由器
# ============================================================================

class ServeMux(Handler):
    """任务路由器，根据任务类型名称将任务分发到对应的 Handler。

    1:1 对应 Go asynq 的 ServeMux 结构体。
    支持前缀匹配（类似 HTTP ServeMux）。

    Usage:
        mux = ServeMux()
        mux.handle_func("email:send", handle_email_send)
        mux.handle("image:process", image_processor)
        mux.use(logging_middleware)

        # 前缀匹配
        mux.handle("email:", email_default_handler)
    """

    def __init__(self):
        """初始化任务路由器。"""
        self._handlers: Dict[str, Handler] = {}
        self._prefix_handlers: Dict[str, Handler] = {}
        self._middlewares: List[MiddlewareFunc] = []

    def routes(self) -> list[str]:
        """返回所有已注册的路由（精确匹配 + 前缀匹配）。"""
        patterns = list(self._handlers.keys()) + list(self._prefix_handlers.keys())
        # 收集嵌套 ServeMux 的路由
        for h in self._handlers.values():
            if isinstance(h, ServeMux):
                patterns.extend(h.routes())
        for h in self._prefix_handlers.values():
            if isinstance(h, ServeMux):
                patterns.extend(h.routes())
        return sorted(set(patterns))  # 去重排序

    def handle(self, pattern: str, handler: Handler) -> None:
        """注册任务类型 → 处理器的映射。

        支持前缀匹配：以 ":" 结尾的 pattern 会匹配所有以此开头的任务类型。
        空字符串 "" 作为最低优先级 catch-all（匹配所有未命中类型）。

        Args:
            pattern: 任务类型模式（如 "email:send" 或 "email:" 或 ""）
            handler: 任务处理器
        """
        if pattern == "":
            # catch-all — 最低优先级兜底
            self._prefix_handlers[""] = handler
        elif pattern.endswith(":"):
            # 前缀匹配 — 如 "email:" 匹配 "email:send", "email:notify" 等
            self._prefix_handlers[pattern] = handler
        else:
            # 精确匹配 — 如 "email:send" 只匹配 "email:send"
            self._handlers[pattern] = handler

    def handle_func(self, pattern: str, func: HandlerFunc) -> None:
        """注册任务类型 → 处理函数的映射。"""
        self.handle(pattern, wrap_handler_func(func))

    def task(self, pattern: str):
        """装饰器注册处理器（FastAPI 风格），子 mux 也支持。"""
        def decorator(obj):
            if isinstance(obj, type):
                self.handle(pattern, obj())
            else:
                self.handle_func(pattern, obj)
            return obj
        return decorator

    def use(self, *middlewares: MiddlewareFunc) -> None:
        """注册全局中间件。

        中间件按注册顺序以洋葱模型执行。

        Args:
            *middlewares: 要注册的中间件函数
        """
        self._middlewares.extend(middlewares)

    async def process_task(self, ctx: Context, task: Task) -> None:
        """路由并处理任务。

        实现了 Handler 接口，允许 ServeMux 作为子路由器嵌套使用。

        匹配优先级：精确匹配 > 最长前缀匹配

        Args:
            ctx: 任务处理上下文
            task: 要处理的任务
        """
        handler = self._find_handler(task.typename)  # 查找匹配的处理器

        # 应用中间件链
        if self._middlewares:
            handler = apply_middlewares(handler, self._middlewares)

        # 执行处理
        await handler.process_task(ctx, task)

    def _find_handler(self, typename: str) -> Handler:
        """根据任务类型名查找处理器。

        匹配优先级：精确匹配 > 最长前缀匹配 > 出错

        Args:
            typename: 任务类型名称

        Returns:
            Handler: 匹配的处理器

        Raises:
            ValueError: 未找到匹配的处理器
        """
        # 先尝试精确匹配
        if typename in self._handlers:
            return self._handlers[typename]

        # 再尝试前缀匹配（最长匹配优先）
        best_match: Optional[Handler] = None
        best_len: int = 0
        for prefix, handler in self._prefix_handlers.items():
            if typename.startswith(prefix) and len(prefix) > best_len:
                best_match = handler
                best_len = len(prefix)

        if best_match:
            return best_match

        # catch-all "" 作为最后兜底（优先级最低）
        if "" in self._prefix_handlers:
            return self._prefix_handlers[""]

        raise ValueError(f"未找到任务 '{typename}' 的处理器")

    def _merge_into(self, target: "ServeMux") -> None:
        """将当前 mux 中所有已注册的 handler 合并到目标 mux。

        目标 mux 已有的 handler 不会被覆盖（外部优先）。

        Args:
            target: 合并目标 ServeMux
        """
        for pattern, handler in self._handlers.items():
            if pattern not in target._handlers:
                target._handlers[pattern] = handler
        for pattern, handler in self._prefix_handlers.items():
            if pattern not in target._prefix_handlers:
                target._prefix_handlers[pattern] = handler
        for mw in self._middlewares:
            if mw not in target._middlewares:
                target._middlewares.append(mw)

    def _has_handlers(self) -> bool:
        """检查是否有已注册的 handler。"""
        return bool(self._handlers or self._prefix_handlers)

# ============================================================================
# Server — 任务处理服务器
# ============================================================================

class Server:
    """异步任务处理服务器。

    1:1 对应 Go asynq 的 Server 结构体。
    负责从 Redis 队列中取出任务并分发给 worker 处理。

    内部启动以下后台协程：
    - processor: 核心处理循环，持续出队并处理任务
    - heartbeat: 定期写入服务器心跳和延长租约
    - forwarder: 定期将到期任务从 scheduled/retry 前移到 pending
    - recoverer: 定期检测并恢复孤儿任务（worker 崩溃后遗留的）
    - janitor: 定期清理已过保留期的完成任务
    - aggregator: 定期检查并聚合任务组

    Usage:
        config = Config(concurrency=10, queues={"critical": 6, "default": 3})
        server = Server(RedisClientOpt(addr="localhost:6379"), config)
        mux = ServeMux()
        mux.handle_func("email:send", handle_email)

        await server.run(mux)  # 阻塞直到信号
        # 或
        await server.start(mux)  # 非阻塞
        # ... 做其他事情 ...
        await server.shutdown()
    """

    def __init__(self, redis_conn: RedisClientOpt | _redis.Redis, config: Optional[Config] = None):
        """初始化服务器。

        Args:
            redis_conn: Redis 连接配置或已有客户端
            config: 服务器配置，默认使用 Config()
        """
        self._config = config or Config()  # 服务器配置
        self._logger = self._config.logger or DefaultLogger(self._config.log_level)  # 日志器

        # 服务器标识信息
        self._host = _socket.gethostname()
        self._pid = _os.getpid()

        if isinstance(redis_conn, _redis.Redis):
            self._redis = redis_conn
            self._owns_connection = False
        else:
            self._redis = redis_conn.make_redis_client()
            self._owns_connection = True

        # 创建 Redis Broker
        self._broker = RDB(self._redis, self._logger)

        # 内置路由器（FastAPI 风格）
        self._mux = ServeMux()
        self._handler: Handler | ServeMux | None = None

        # 服务器标识
        self._server_id = (
            self._config.code or
            (f"{self._config.name}:{_uuid.uuid4().hex[:8]}" if self._config.name else _uuid.uuid4().hex)
        )

        # 服务器状态
        self._state: str = "new"        # new → active → quiet → stopped → closed

        # 控制信号
        self._shutdown_event = _asyncio.Event()    # 关闭信号
        self._quiet_event = _asyncio.Event()       # 静默信号（停止接受新任务）

        # 后台任务引用（用于取消和等待）
        self._bg_tasks: List[_asyncio.Task] = []

        # Worker 跟踪
        self._active_workers: Dict[str, dict[str, Any]] = {}  # 活跃 worker 信息
        # 活跃任务 ID → 队列名映射（用于按队列延长租约）
        self._active_task_ids: Dict[str, str] = {}  # {task_id: queue_name}

        # 队列名列表（按优先级排序）
        self._queues = sorted(
            self._config.queues.keys(),
            key=lambda q: self._config.queues[q],
            reverse=True,  # 降序：高优先级在前
        )

    # ========================================================================
    # 公共 API
    # ========================================================================

    async def run(self, handler: Handler | ServeMux | None = None) -> None:
        """启动服务器并阻塞直到收到系统信号。"""
        await self.start(handler)
        await self._wait_for_signal()
        await self.shutdown()

    async def start(self, handler: Handler | ServeMux | None = None) -> None:
        """启动服务器（非阻塞）。

        如果同时通过 @server.task() 装饰器注册了 handler 并传入了外部 handler，
        会自动合并：外部 handler 优先级更高，装饰器注册的作为补充。
        """
        if self._state != "new":
            raise RuntimeError(f"服务器已启动，当前状态: {self._state}")

        self._state = "active"

        # 合并装饰器注册的子路由到外部 handler
        self._handler = self._build_handler(handler)

        self._logger.info(
            f"服务器启动: 并发={self._config.concurrency}, "
            f"队列={self._queues}, 严格优先级={self._config.strict_priority}"
        )

        # 启动所有后台协程
        loop = _asyncio.get_running_loop()

        # 1. 心跳协程 — 定期向 Redis 报告服务器存活状态
        self._bg_tasks.append(loop.create_task(self._heartbeat_loop(),
            name="heartbeat"))

        # 2. 前移协程 — 定期检查 scheduled/retry 中的到期任务
        self._bg_tasks.append(loop.create_task(self._forwarder_loop(),
            name="forwarder"))

        # 3. 恢复协程 — 检测并恢复孤儿任务
        self._bg_tasks.append(loop.create_task(self._recoverer_loop(),
            name="recoverer"))

        # 4. 清理协程 — 删除已完成但过期的任务
        self._bg_tasks.append(loop.create_task(self._janitor_loop(),
            name="janitor"))

        # 5. 健康检查协程 — 定期检查 Redis 连接
        self._bg_tasks.append(loop.create_task(self._healthcheck_loop(),
            name="healthcheck"))

        # 6. 聚合协程 — 管理任务组聚合
        if self._config.group_aggregator:
            self._bg_tasks.append(loop.create_task(self._aggregator_loop(),
                name="aggregator"))

        # 7. 处理器协程 — 核心任务处理循环
        self._bg_tasks.append(loop.create_task(self._processor(), name="processor"))

        # ======== 启动信息 ========
        name_tag = f" name={self._config.name}" if self._config.name else ""
        code_tag = f" code={self._config.code}" if self._config.code else ""
        routes_info = ", ".join(self._mux.routes()) or "(无)"
        self._logger.info(
            f"━━━ Server 启动 ━━━{name_tag}{code_tag} "
            f"concurrency={self._config.concurrency} queues={self._config.queues} "
            f"routes=[{routes_info}]"
        )

    def _build_handler(self, handler: Handler | ServeMux | None) -> Handler:
        """构建最终的 handler，合并装饰器子路由。

        合并规则：
        - 无外部 handler：直接使用 self._mux
        - 外部是 ServeMux 且 self._mux 有 handler：合并（外部优先）
        - 外部是普通 Handler 且 self._mux 有 handler：self._mux 加 catch-all 回退
        - 其余情况：直接使用外部 handler
        """
        # 无外部 handler，直接用内置 mux
        if handler is None:
            return self._mux

        # 内置 mux 没有注册任何 handler，直接用外部
        if not self._mux._has_handlers():
            return handler

        # 外部是 ServeMux → 合并
        if isinstance(handler, ServeMux):
            self._mux._merge_into(handler)
            return handler

        # 外部是普通 Handler → self._mux 做路由，外部做未匹配的兜底
        self._mux.handle("", handler)
        return self._mux

    def task(self, pattern: str):
        """FastAPI 风格装饰器：注册任务处理器。"""
        def decorator(obj):
            if isinstance(obj, type):
                self._mux.handle(pattern, obj())
            else:
                self._mux.handle_func(pattern, obj)
            return obj
        return decorator

    def use(self, *middlewares):
        """注册中间件。"""
        self._mux.use(*middlewares)

    async def shutdown(self) -> None:
        """执行优雅关闭。

        1:1 对应 Go asynq 的 Server.Shutdown 方法。
        1. 停止接受新任务（quiet）
        2. 等待当前正在处理的任务完成
        3. 将未完成的任务重新入队
        4. 清理服务器状态
        """
        if self._state in ("closed", "new"):
            return  # 已关闭或未启动

        self._logger.info("开始优雅关闭...")
        self._state = "quiet"  # 先进入静默模式，停止接受新任务
        self._quiet_event.set()

        # 等待后台循环退出并等待 worker 完成
        self._state = "stopped"
        self._shutdown_event.set()
        await self._wait_for_bg_tasks()

        # 清理 Redis 中的服务器状态
        try:
            await self._broker.clear_server_state(self._host, self._pid, self._server_id)
        except Exception as e:
            self._logger.warn(f"清理服务器状态失败: {e}")

        # 关闭 Redis 连接
        if self._owns_connection:
            await self._broker.close()

        self._state = "closed"
        self._logger.info("服务器已关闭")

    async def stop(self) -> None:
        """停止服务器（停止接受新任务但不等待完成）。

        1:1 对应 Go asynq 的 Server.Stop 方法。
        """
        if self._state != "active":
            return
        self._state = "stopped"
        self._shutdown_event.set()

    async def quiet(self) -> None:
        """让服务器进入静默模式（不停止，但不接受新任务）。

        1:1 对应 Go asynq 的 Server.Quiet 方法。
        """
        if self._state != "active":
            return
        self._state = "quiet"
        self._quiet_event.set()

    # ========================================================================
    # 内部 — 后台协程
    # ========================================================================

    async def _processor(self) -> None:
        """核心处理器 — 持续从 Redis 出队任务并分发给 worker。

        使用 asyncio.Semaphore 控制并发 worker 数量。
        """
        semaphore = _asyncio.Semaphore(self._config.concurrency)  # 并发信号量

        while not self._shutdown_event.is_set():
            # 静默模式下不获取新任务
            if self._state == "quiet":
                await _asyncio.sleep(0.1)
                continue

            # 检查是否需要关闭
            if self._state == "stopped":
                break

            try:
                # 从 Redis 出队一个任务（传递权重用于加权选择）
                msg = await self._broker.dequeue(
                    self._queues, self._config.strict_priority,
                    weights=self._config.queues,  # 传递队列权重
                )

                if msg is None:
                    # 队列为空，等待后重试
                    await _asyncio.sleep(self._config.task_check_interval)
                    continue

                # 获取信号量许可（控制并发数）
                await semaphore.acquire()

                # 为这个任务创建一个 worker 协程（fire-and-forget，不跟踪）
                _asyncio.create_task(
                    self._worker(msg, semaphore),
                    name=f"worker-{msg.id[:8]}"
                )

            except _asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"处理器错误: {e}")
                await _asyncio.sleep(1)  # 错误后等待 1 秒

        # 等待所有 worker 完成
        self._logger.info("处理器已停止，等待 worker 完成...")

    async def _worker(self, msg: TaskMessage, semaphore: _asyncio.Semaphore) -> None:
        """Worker 协程 — 处理单个任务。

        Args:
            msg: 出队的任务消息
            semaphore: 并发控制信号量
        """
        task_id = msg.id  # 任务 ID

        try:
            # 创建 Task 对象
            task = Task(
                typename=msg.type,           # 任务类型名
                payload=msg.payload,          # 负载数据
                headers=msg.headers,          # 请求头
            )

            # 创建结果写入器（传入 typename 用于构建 state/result key）
            writer = ResultWriter(task_id, self._broker, msg.queue, typename=msg.type)
            task.set_result_writer(writer)

            # 创建处理上下文
            ctx = Context()

            # 记录 worker 信息
            self._active_task_ids[task_id] = msg.queue  # 记录任务所属队列

            # 执行处理器（带超时控制）
            try:
                if msg.timeout > 0:
                    # 有超时设置：使用 asyncio.wait_for
                    await _asyncio.wait_for(
                        self._handler.process_task(ctx, task),
                        timeout=msg.timeout,
                    )
                else:
                    # 无超时：直接执行
                    await self._handler.process_task(ctx, task)

                # 处理成功
                await self._broker.done(msg)
                self._logger.debug(f"任务完成: id={task_id}")

            except _asyncio.TimeoutError:
                # 任务超时 — 进入重试流程
                await self._handle_task_error(msg, task, TimeoutError("任务处理超时"))

            except _asyncio.CancelledError:
                # 服务器关闭导致任务被取消 — 将任务重新入队以便恢复
                await self._broker.requeue(msg)
                self._logger.info(f"任务已重新入队（关闭中断）: id={task_id}")

            except SkipRetry:
                # 明确标记为不重试 — 直接归档
                await self._broker.archive(msg, "skip retry")
                self._logger.info(f"任务已归档（SkipRetry）: id={task_id}")

            except Exception as e:
                # 处理失败 — 进入重试或归档流程
                await self._handle_task_error(msg, task, e)

        finally:
            # 释放信号量
            self._active_task_ids.pop(task_id, None)
            semaphore.release()

    async def _handle_task_error(self, msg: TaskMessage, task: Task, error: Exception) -> None:
        """处理任务执行错误：决定重试还是归档。

        Args:
            msg: 任务消息
            task: 任务对象
            error: 发生的错误
        """
        error_msg = str(error)  # 错误消息

        # 调用错误回调（如果配置了）
        if self._config.error_handler:
            try:
                ctx = Context()
                await self._config.error_handler(ctx, task, error)
            except Exception:
                pass  # 错误回调不应影响主流程

        # 检查是否是 SkipRetry
        if isinstance(error, SkipRetry):
            await self._broker.archive(msg, error_msg)
            return

        # 检查重试是否耗尽
        if msg.retried >= msg.retry:
            await self._broker.archive(msg, error_msg)
            self._logger.info(f"任务已归档（重试耗尽 {msg.retried}/{msg.retry}）: id={msg.id}")
            return

        # 计算重试延迟
        delay_func = self._config.retry_delay_func or default_retry_delay
        delay_secs = delay_func(msg.retried, error, task)

        # 计算下次重试时间
        process_at = _timeutil.now() + delay_secs * 1_000_000_000

        # 判定是否算作失败
        is_failure_func = self._config.is_failure_func or default_is_failure
        is_failure = is_failure_func(error)

        # 执行重试
        await self._broker.retry(msg, process_at, error_msg, is_failure)

        self._logger.info(
            f"任务重试: id={msg.id}, retried={msg.retried+1}/{msg.retry}, "
            f"delay={delay_secs}s, error={error_msg[:100]}"
        )

    async def _heartbeat_loop(self) -> None:
        """心跳协程 — 定期向 Redis 写入服务器状态和延长 worker 租约。"""
        while not self._shutdown_event.is_set():
            try:
                # 快照活跃任务（防止 worker 并发修改 dict）
                active_snapshot = list(self._active_task_ids.items())

                # 构建 worker 信息列表
                worker_info_list = [
                    {"task_id": tid, "queue": qname}
                    for tid, qname in active_snapshot
                ]

                info = ServerInfo(
                    host=self._host, pid=self._pid, server_id=self._server_id,
                    concurrency=self._config.concurrency,
                    queues=self._config.queues,
                    strict_priority=self._config.strict_priority,
                    status=self._state, start_time=_timeutil.now(),
                    active_worker_count=len(self._active_task_ids),
                    routes=self._mux.routes(),  # 已注册的路由
                )

                # 写入服务器状态（TTL 为心跳间隔的 2 倍）
                ttl = SERVER_HEARTBEAT_INTERVAL * 2
                await self._broker.write_server_state(info, worker_info_list, ttl)

                # 延长活跃任务的租约（按队列分别处理）
                if active_snapshot:
                    # 按队列分组任务 ID
                    queue_tasks: Dict[str, list[str]] = {}
                    for tid, qname in active_snapshot:
                        if qname not in queue_tasks:
                            queue_tasks[qname] = []
                        queue_tasks[qname].append(tid)
                    # 为每个队列单独延长租约
                    for qname, tids in queue_tasks.items():
                        await self._broker.extend_lease(
                            qname, tids, LEASE_DURATION
                        )

            except Exception as e:
                self._logger.warn(f"心跳发送失败: {e}")

            await _asyncio.sleep(SERVER_HEARTBEAT_INTERVAL)

    async def _forwarder_loop(self) -> None:
        """前移协程 — 定期将到期任务从 scheduled/retry 移到 pending。"""
        while not self._shutdown_event.is_set():
            try:
                await self._broker.forward_if_ready(self._queues)
            except Exception as e:
                self._logger.warn(f"前移检查失败: {e}")

            await _asyncio.sleep(self._config.delayed_task_check_interval)

    async def _recoverer_loop(self) -> None:
        """恢复协程 — 检测并处理租约到期的孤儿任务。"""
        while not self._shutdown_event.is_set():
            try:
                # 查找租约到期的任务
                expired_tasks = await self._broker.list_lease_expired(self._queues)

                for msg in expired_tasks:
                    if msg.retried >= msg.retry:
                        # 重试次数耗尽 → 归档
                        await self._broker.archive(
                            msg, "孤儿任务恢复（重试耗尽）"
                        )
                    else:
                        # 重新调度 → 放回 pending
                        process_at = _timeutil.now()  # 立即重新处理
                        await self._broker.retry(
                            msg, process_at, "孤儿任务恢复", True
                        )
                    self._logger.info(f"恢复孤儿任务: id={msg.id}")

            except Exception as e:
                self._logger.warn(f"恢复检查失败: {e}")

            # 每隔 60 秒检查一次
            await _asyncio.sleep(60)

    async def _janitor_loop(self) -> None:
        """清理协程 — 定期删除已过保留期的完成任务。"""
        while not self._shutdown_event.is_set():
            try:
                for qname in self._queues:
                    ckey = completed_key(qname)
                    now = _timeutil.now()

                    # 使用批量清理 Lua 脚本
                    await _get_lua_script("delete_expired_completed")(
                        keys=[ckey],
                        args=[str(now), str(self._config.janitor_batch_size)],
                    )
            except Exception as e:
                self._logger.warn(f"清理检查失败: {e}")

            await _asyncio.sleep(self._config.janitor_interval)

    async def _healthcheck_loop(self) -> None:
        """健康检查协程 — 定期检查 Redis 连接状况。"""
        while not self._shutdown_event.is_set():
            try:
                ok = await self._broker.ping()
                if not ok and self._config.health_check_func:
                    await self._config.health_check_func(
                        Exception("Redis 健康检查失败")
                    )
            except Exception as e:
                if self._config.health_check_func:
                    await self._config.health_check_func(e)

            await _asyncio.sleep(self._config.health_check_interval)

    async def _aggregator_loop(self) -> None:
        """聚合协程 — 定期检查并聚合任务组。"""
        if not self._config.group_aggregator:
            return

        while not self._shutdown_event.is_set():
            try:
                for qname in self._queues:
                    groups = await self._broker.list_groups(qname)
                    for gname in groups:
                        trigger_key = await self._broker.aggregation_check(
                            qname, gname,
                            self._config.group_max_delay,
                            self._config.group_max_size,
                            self._config.group_grace_period,
                        )
                        if trigger_key:
                            # 读取组内所有任务
                            gkey = f"asynq:qname:aggregation:{gname}"
                            task_ids = await self._redis.zrange(gkey, 0, -1)
                            tasks = []
                            for tid in task_ids:
                                tid_str = tid.decode("utf-8") if isinstance(tid, bytes) else tid
                                tkey = f"asynq:qname:t:{tid_str}"
                                data = await self._redis.hget(tkey, "msg")
                                if data:
                                    msg = TaskMessage.from_json(data.decode("utf-8"))
                                    tasks.append(Task(msg.type, msg.payload))

                            if tasks:
                                # 执行聚合
                                aggregated = self._config.group_aggregator(gname, tasks)
                                # 将聚合任务入队
                                from .client import Client
                                client = Client(self._redis)
                                await client.enqueue(aggregated)
                                self._logger.info(f"聚合组 {gname}: {len(tasks)} 个任务已聚合")

            except Exception as e:
                self._logger.warn(f"聚合检查失败: {e}")

            await _asyncio.sleep(2)  # 每 2 秒检查一次

    # ========================================================================
    # 内部 — 信号处理
    # ========================================================================

    async def _wait_for_signal(self) -> None:
        """等待系统信号（SIGINT/SIGTERM）或手动关闭。

        Unix 上通过 add_signal_handler 注册 SIGINT/SIGTERM；
        Windows 上通过 signal.signal 注册 SIGINT/SIGBREAK 作为降级方案。
        """
        loop = _asyncio.get_running_loop()
        stop_event = _asyncio.Event()

        try:
            for sig in (_signal.SIGINT, _signal.SIGTERM):
                loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows 不支持 add_signal_handler → 使用 signal.signal 降级
            import signal as _stdlib_signal

            def _handle_signal(signum: int, frame: Any) -> None:
                self._logger.info(f"收到信号 {signum}，准备优雅关闭...")
                stop_event.set()

            for sig in (_signal.SIGINT, _signal.SIGBREAK):
                try:
                    _stdlib_signal.signal(sig, _handle_signal)
                except (ValueError, OSError):
                    pass  # 该信号无法在主线程外注册（忽略）

        # 等待停止信号或手动关闭
        while not stop_event.is_set() and not self._shutdown_event.is_set():
            await _asyncio.sleep(0.1)

    async def _wait_for_bg_tasks(self) -> None:
        """等待所有后台任务完成（或超时强制取消）。"""
        # 过滤出活跃的后台任务
        active_tasks = [t for t in self._bg_tasks if not t.done()]

        if not active_tasks:
            return

        try:
            # 等待完成（最多等待 shutdown_timeout 秒）
            await _asyncio.wait_for(
                _asyncio.gather(*active_tasks, return_exceptions=True),
                timeout=self._config.shutdown_timeout,
            )
        except _asyncio.TimeoutError:
            self._logger.warn("关闭超时，强制取消后台任务")
            for t in active_tasks:
                if not t.done():
                    t.cancel()  # 强制取消
