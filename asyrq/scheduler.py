# scheduler.py — 周期性任务调度器模块
# 提供基于 Cron 表达式的定时任务调度功能，1:1 对应 Go asynq 的 Scheduler

from __future__ import annotations
from typing import Awaitable, Callable, Dict, List, Optional
import asyncio as _asyncio
import uuid as _uuid
import os as _os
import socket as _socket

import redis.asyncio as _redis
import croniter as _croniter
import time as _time

from .task import Task, TaskInfo
from .client import Client
from .options import Option
from .internal.base import SERVER_HEARTBEAT_INTERVAL
from .internal.rdb import RDB
from .internal import timeutil as _timeutil
from .internal.log import Logger, DefaultLogger, LogLevel
from .connection import RedisClientOpt
from .errors import EnqueueError

# ============================================================================
# 调度器配置
# ============================================================================

class SchedulerOpts:
    """调度器选项。

    1:1 对应 Go asynq 的 SchedulerOpts 结构体。
    """

    def __init__(
        self,
        logger: Optional[Logger] = None,
        log_level: LogLevel = LogLevel.INFO,
        # 预入队回调 — 在任务即将入队前调用
        pre_enqueue_func: Optional[Callable[[Task], Awaitable[None]]] = None,
        # 后入队回调 — 在任务成功入队后调用
        post_enqueue_func: Optional[Callable[[TaskInfo, Optional[Exception]], Awaitable[None]]] = None,
        # 入队错误处理 — 当入队失败时调用（例如可以发送告警）
        enqueue_error_handler: Optional[Callable[[Task, list[Option], Exception], Awaitable[None]]] = None,
    ):
        """初始化调度器选项。

        Args:
            logger: 自定义日志器
            log_level: 日志级别
            pre_enqueue_func: 入队前的回调函数
            post_enqueue_func: 入队后的回调函数
            enqueue_error_handler: 入队失败时的回调函数
        """
        self.logger = logger or DefaultLogger(log_level)  # 日志器
        self.log_level = log_level                         # 日志级别
        self.pre_enqueue_func = pre_enqueue_func           # 预入队回调
        self.post_enqueue_func = post_enqueue_func         # 后入队回调
        self.enqueue_error_handler = enqueue_error_handler # 错误处理回调

# ============================================================================
# 调度器条目
# ============================================================================

class _SchedulerEntry:
    """调度器条目 — 记录一个周期性任务的注册信息。

    内部使用，对应用户不可见。
    """

    def __init__(
        self,
        entry_id: str,         # 条目唯一 ID
        cron_spec: str,        # Cron 表达式或 @every 格式
        task: Task,            # 关联的任务对象
        opts: List[Option],    # 入队选项
    ):
        """初始化调度器条目。

        Args:
            entry_id: 唯一标识符
            cron_spec: Cron 规范
            task: 任务模板
            opts: 入队选项
        """
        self.id = entry_id              # 条目 ID
        self.cron_spec = cron_spec      # Cron 规范
        self.task = task                # 任务模板
        self.opts = list(opts)          # 入队选项
        self.next_run: int = 0          # 下次执行时间（纳秒时间戳）
        self.prev_run: int = 0          # 上次执行时间
        self.cron_iter = None           # croniter 迭代器

# ============================================================================
# Scheduler — 调度器
# ============================================================================

class Scheduler:
    """周期性任务调度器。

    1:1 对应 Go asynq 的 Scheduler 结构体。
    根据 Cron 表达式定期将任务入队。

    支持的 Cron 格式:
        - 标准 5 字段 Cron: "*/5 * * * *" (分钟 小时 日 月 星期)
        - 6 字段 Cron: "*/5 * * * * *" (秒 分钟 小时 日 月 星期)
        - @every 格式: "@every 1m", "@every 30s", "@every 1h"

    Usage:
        scheduler = Scheduler(RedisClientOpt(addr="localhost:6379"))

        task = Task("report:daily", payload)
        entry_id = await scheduler.register("@every 1h", task)
        # 或使用 Cron 表达式
        entry_id = await scheduler.register("0 0 * * *", task)

        await scheduler.run()  # 启动并阻塞
    """

    def __init__(
        self,
        redis_conn: RedisClientOpt,
        opts: Optional[SchedulerOpts] = None,
    ):
        """初始化调度器。

        Args:
            redis_conn: Redis 连接配置
            opts: 调度器选项
        """
        self._opts = opts or SchedulerOpts()     # 调度器选项
        self._logger = self._opts.logger          # 日志器

        # 创建 Redis 客户端和 Broker
        self._redis = redis_conn.make_redis_client()
        self._broker = RDB(self._redis, self._logger)
        self._client = Client(redis_client=self._redis, logger=self._logger)  # 内部 Client

        # 注册的条目映射 {entry_id: entry}
        self._entries: Dict[str, _SchedulerEntry] = {}

        # 调度器标识
        self._host = _socket.gethostname()
        self._pid = _os.getpid()

        # 状态控制
        self._state: str = "new"               # new → active → closed
        self._shutdown_event = _asyncio.Event()
        self._bg_task: Optional[_asyncio.Task] = None

    async def register(
        self,
        cron_spec: str,
        task: Task,
        *opts: Option,
    ) -> str:
        """注册一个周期性任务。

        1:1 对应 Go asynq 的 Scheduler.Register 方法。

        Args:
            cron_spec: Cron 表达式或 @every 格式
            task: 任务模板（每次触发时会创建新实例入队）
            *opts: 入队时的额外选项

        Returns:
            str: 条目的唯一 ID（用于后续 Unregister）

        Raises:
            ValueError: Cron 表达式无效
        """
        # 生成条目 ID
        entry_id = f"{self._host}:{self._pid}:{_uuid.uuid4().hex[:12]}"

        # 创建条目
        entry = _SchedulerEntry(
            entry_id=entry_id,
            cron_spec=cron_spec,
            task=task,
            opts=list(opts),
        )

        # 解析 Cron 规范并创建迭代器
        entry.cron_iter = self._create_cron_iter(cron_spec)

        # 计算首次执行时间
        entry.next_run = self._next_cron_time(entry.cron_iter)

        # 注册条目
        self._entries[entry_id] = entry

        # 写入 Redis（用于跨进程可见）
        await self._sync_entries_to_redis()

        self._logger.info(f"已注册周期性任务: id={entry_id}, spec={cron_spec}, type={task.typename}")
        return entry_id

    async def unregister(self, entry_id: str) -> None:
        """取消注册一个周期性任务。

        1:1 对应 Go asynq 的 Scheduler.Unregister 方法。

        Args:
            entry_id: 注册时返回的条目 ID
        """
        if entry_id in self._entries:
            del self._entries[entry_id]  # 从内存中移除
            await self._sync_entries_to_redis()  # 同步到 Redis
            self._logger.info(f"已取消注册周期性任务: id={entry_id}")
        else:
            self._logger.warn(f"未找到条目: id={entry_id}")

    async def run(self) -> None:
        """启动调度器并阻塞运行。

        1:1 对应 Go asynq 的 Scheduler.Run 方法。
        """
        await self.start()  # 启动后台循环

        # 等待关闭信号
        try:
            while not self._shutdown_event.is_set():
                await _asyncio.sleep(1)
        except _asyncio.CancelledError:
            pass

        await self.shutdown()  # 执行关闭

    async def start(self) -> None:
        """启动调度器（非阻塞）。

        1:1 对应 Go asynq 的 Scheduler.Start 方法。
        """
        if self._state != "new":
            return  # 已经启动

        self._state = "active"
        self._logger.info("调度器已启动")

        # 启动主循环
        loop = _asyncio.get_running_loop()
        self._bg_task = loop.create_task(
            self._main_loop(), name="scheduler-loop"
        )

    async def shutdown(self) -> None:
        """关闭调度器。

        1:1 对应 Go asynq 的 Scheduler.Shutdown 方法。
        """
        if self._state == "closed":
            return

        self._state = "closed"
        self._shutdown_event.set()

        # 取消主循环
        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
            try:
                await self._bg_task
            except _asyncio.CancelledError:
                pass

        # 关闭连接
        await self._broker.close()
        self._logger.info("调度器已关闭")

    # ========================================================================
    # 内部 — 主循环
    # ========================================================================

    async def _main_loop(self) -> None:
        """调度器主循环 — 每秒检查一次是否有到期的 cron 任务需要执行。"""
        while not self._shutdown_event.is_set():
            try:
                now = _timeutil.now()  # 当前纳秒时间戳

                # 检查每个条目的到期时间
                for entry_id, entry in list(self._entries.items()):
                    if entry.next_run <= now:
                        # 该条目到期，需要在单独的协程中执行入队
                        _asyncio.create_task(
                            self._enqueue_scheduled(entry),
                            name=f"sched-{entry_id[:8]}"
                        )

                        # 更新下次执行时间
                        entry.prev_run = now
                        entry.next_run = self._next_cron_time(entry.cron_iter)

                # 更新 Redis 中的条目信息
                await self._sync_entries_to_redis()

            except _asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"调度器循环错误: {e}")

            # 每秒检查一次
            await _asyncio.sleep(1)

    async def _enqueue_scheduled(self, entry: _SchedulerEntry) -> None:
        """执行调度的任务入队。

        Args:
            entry: 调度器条目
        """
        entry_id = entry.id

        # 调用预入队回调
        if self._opts.pre_enqueue_func:
            try:
                await self._opts.pre_enqueue_func(entry.task)
            except Exception as e:
                self._logger.warn(f"预入队回调失败: {e}")

        try:
            # 使用内部 Client 将任务入队
            task_info = await self._client.enqueue(entry.task, *entry.opts)

            # 记录入队事件到 Redis
            await self._broker.record_scheduler_enqueue_event(
                entry_id, task_info.id
            )

            self._logger.debug(f"调度入队: entry={entry_id}, task={task_info.id}")

            # 调用后入队回调
            if self._opts.post_enqueue_func:
                await self._opts.post_enqueue_func(task_info, None)

        except Exception as e:
            self._logger.error(f"调度入队失败: entry={entry_id}, error={e}")

            # 调用错误处理
            if self._opts.enqueue_error_handler:
                try:
                    await self._opts.enqueue_error_handler(
                        entry.task, entry.opts, e
                    )
                except Exception:
                    pass  # 错误回调不应再次抛错

            # 调用后入队回调（带错误信息）
            if self._opts.post_enqueue_func:
                await self._opts.post_enqueue_func(TaskInfo(), e)

    async def _sync_entries_to_redis(self) -> None:
        """将所有注册的条目同步到 Redis（用于多进程可见）。"""
        entries_data = []
        for entry_id, entry in self._entries.items():
            entries_data.append({
                "id": entry_id,
                "cron_spec": entry.cron_spec,
                "task_type": entry.task.typename,
                "next_run": entry.next_run,
                "prev_run": entry.prev_run,
            })

        if entries_data:
            ttl = SERVER_HEARTBEAT_INTERVAL * 2
            await self._broker.write_scheduler_entries(entries_data, ttl)

    # ========================================================================
    # 内部 — Cron 解析
    # ========================================================================

    @staticmethod
    def _create_cron_iter(cron_spec: str) -> _croniter.croniter:
        """根据 Cron 规范创建 croniter 迭代器。

        支持:
            - 标准 5 字段: "*/5 * * * *"
            - 6 字段: "*/5 * * * * *" (秒字段)
            - @every 格式: "@every 30s", "@every 1m", "@every 1h"

        Args:
            cron_spec: Cron 规范字符串

        Returns:
            croniter: Cron 迭代器

        Raises:
            ValueError: 格式无效时
        """
        if cron_spec.startswith("@every "):
            # 解析 @every 格式 → 转换为等价的 cron 表达式
            duration_str = cron_spec[7:].strip()  # 去掉 "@every " 前缀
            return _EveryCronIter(duration_str)  # 使用自定义迭代器
        else:
            # 标准 cron 表达式
            fields = cron_spec.strip().split()
            if len(fields) == 6:
                # 6 字段：秒 分钟 小时 日 月 星期
                return _croniter.croniter(cron_spec, _time.time())
            else:
                # 5 字段：分钟 小时 日 月 星期
                return _croniter.croniter(cron_spec, _time.time())

    @staticmethod
    def _next_cron_time(cron_iter) -> int:
        """计算 cron 的下次触发时间。

        Args:
            cron_iter: croniter 实例

        Returns:
            int: 下次触发时间的纳秒级时间戳
        """
        next_sec = cron_iter.get_next(ret_type=float)  # 获取下次触发秒数
        return int(next_sec * 1_000_000_000)  # 转为纳秒

class _EveryCronIter:
    """@every 格式的自定义 Cron 迭代器。

    模拟 croniter 接口，支持 @every 30s / @every 1m / @every 1h 等格式。
    """

    def __init__(self, duration_str: str):
        """初始化 @every 迭代器。

        Args:
            duration_str: 如 "30s", "5m", "1h", "2h30m"

        Raises:
            ValueError: 无效的 duration 格式
        """
        self._interval_seconds = self._parse_duration(duration_str)  # 解析为秒数

    def get_next(self, ret_type=float) -> float:
        """返回下一个触发时间（从当前时间起 + interval）。

        Args:
            ret_type: float 返回时间戳，int 返回 datetime

        Returns:
            float: 下次触发时间的 Unix 时间戳
        """
        now = _time.time()
        return now + self._interval_seconds  # 当前时间 + 间隔

    def get_prev(self, ret_type=float) -> float:
        """返回上一个触发时间。"""
        now = _time.time()
        return now - self._interval_seconds

    @staticmethod
    def _parse_duration(duration_str: str) -> int:
        """解析 duration 字符串为秒数。

        支持格式: 30s, 5m, 1h, 2h30m, 1d

        Args:
            duration_str: 时间字符串

        Returns:
            int: 秒数

        Raises:
            ValueError: 无效格式
        """
        total_seconds = 0  # 累计秒数
        num_buf = ""        # 数字缓冲区

        for char in duration_str:
            if char.isdigit():
                num_buf += char  # 累积数字
            elif char == "s":
                total_seconds += int(num_buf)  # 秒
                num_buf = ""
            elif char == "m":
                total_seconds += int(num_buf) * 60  # 分钟 → 秒
                num_buf = ""
            elif char == "h":
                total_seconds += int(num_buf) * 3600  # 小时 → 秒
                num_buf = ""
            elif char == "d":
                total_seconds += int(num_buf) * 86400  # 天 → 秒
                num_buf = ""

        if total_seconds == 0:
            raise ValueError(f"无效的 duration 格式: {duration_str}")

        return total_seconds  # 返回总秒数
