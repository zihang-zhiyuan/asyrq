# asyrq/sync/scheduler.py - 同步版定时调度器
from __future__ import annotations

import os as _os
import socket as _socket
import threading as _threading
import uuid as _uuid
import time as _time
from typing import Optional, Callable

import redis as _redis
import croniter as _croniter

from ..task import Task, TaskInfo
from ..options import Option
from ..internal import timeutil as _timeutil
from ..internal.log import Logger, DefaultLogger, LogLevel
from ..connection import RedisClientOpt
from .client import SyncClient
from .rdb import SyncRDB


class _SyncSchedulerEntry:
    """调度器条目。"""
    def __init__(self, entry_id: str, cron_spec: str, task: Task, opts: list):
        self.id = entry_id
        self.cron_spec = cron_spec
        self.task = task
        self.opts = list(opts)
        self.next_run: int = 0
        self.cron_iter = None


class SyncScheduler:
    """同步版定时调度器。

    Usage:
        scheduler = SyncScheduler(RedisClientOpt(...))
        scheduler.register("@every 1m", Task("health:check"))
        scheduler.register("0 9 * * *", Task("report:daily"))
        scheduler.run()  # 阻塞
    """

    def __init__(self, redis_conn, logger: Optional[Logger] = None):
        self._logger = logger or DefaultLogger(level=LogLevel.INFO, log_dir="logs", name="scheduler")

        if isinstance(redis_conn, _redis.Redis):
            self._redis = redis_conn
            self._owns_connection = False
        else:
            self._redis = redis_conn.make_sync_redis_client()
            self._owns_connection = True

        self._broker = SyncRDB(self._redis, self._logger)
        self._client = SyncClient(self._redis, self._logger)
        self._entries: dict[str, _SyncSchedulerEntry] = {}
        self._host = _socket.gethostname()
        self._pid = _os.getpid()
        self._state = "new"
        self._shutdown_event = _threading.Event()
        self._thread: Optional[_threading.Thread] = None

    def register(self, cron_spec: str, task: Task, *opts: Option) -> str:
        """注册周期性任务（同步）。"""
        entry_id = f"{self._host}:{self._pid}:{_uuid.uuid4().hex[:12]}"
        entry = _SyncSchedulerEntry(entry_id, cron_spec, task, list(opts))
        entry.cron_iter = self._create_cron_iter(cron_spec)
        entry.next_run = self._next_cron_time(entry.cron_iter)
        self._entries[entry_id] = entry
        self._logger.info(f"已注册: id={entry_id} spec={cron_spec} type={task.typename}")
        return entry_id

    def unregister(self, entry_id: str) -> None:
        """取消注册。"""
        if entry_id in self._entries:
            del self._entries[entry_id]
            self._logger.info(f"已取消: id={entry_id}")

    def run(self) -> None:
        """阻塞运行。"""
        self.start()
        while not self._shutdown_event.is_set():
            self._shutdown_event.wait(timeout=1)
        self.shutdown()

    def start(self) -> None:
        """非阻塞启动。"""
        if self._state != "new":
            return
        self._state = "active"
        self._logger.info("调度器已启动")
        self._thread = _threading.Thread(target=self._main_loop, name="scheduler", daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        """关闭。"""
        if self._state == "closed":
            return
        self._state = "closed"
        self._shutdown_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        if self._owns_connection:
            self._broker.close()
        self._logger.info("调度器已关闭")

    # ======== 内部 ========

    def _main_loop(self):
        """主循环。"""
        while not self._shutdown_event.is_set():
            try:
                now = _timeutil.now()
                for entry_id, entry in list(self._entries.items()):
                    if entry.next_run <= now:
                        try:
                            task_info = self._client.enqueue(entry.task, *entry.opts)
                            self._logger.debug(f"调度入队: {entry_id} -> {task_info.id[:12]}")
                        except Exception as e:
                            self._logger.error(f"调度入队失败: {entry_id}, {e}")
                        entry.next_run = self._next_cron_time(entry.cron_iter)
            except Exception as e:
                self._logger.error(f"调度循环错误: {e}")

            self._shutdown_event.wait(timeout=1)

    @staticmethod
    def _create_cron_iter(cron_spec: str):
        """创建 cron 迭代器。"""
        if cron_spec.startswith("@every "):
            return _EveryCronIter(cron_spec[7:].strip())
        return _croniter.croniter(cron_spec, _time.time())

    @staticmethod
    def _next_cron_time(cron_iter) -> int:
        """计算下次触发时间（纳秒）。"""
        next_sec = cron_iter.get_next(ret_type=float)
        return int(next_sec * 1_000_000_000)


class _EveryCronIter:
    """@every 格式的 Cron 迭代器。"""

    def __init__(self, duration_str: str):
        self._interval = self._parse_duration(duration_str)

    def get_next(self, ret_type=float) -> float:
        return _time.time() + self._interval

    def get_prev(self, ret_type=float) -> float:
        return _time.time() - self._interval

    @staticmethod
    def _parse_duration(s: str) -> int:
        total = 0
        num = ""
        for ch in s:
            if ch.isdigit():
                num += ch
            elif ch == "s":
                total += int(num); num = ""
            elif ch == "m":
                total += int(num) * 60; num = ""
            elif ch == "h":
                total += int(num) * 3600; num = ""
            elif ch == "d":
                total += int(num) * 86400; num = ""
        if total == 0:
            raise ValueError(f"无效的 duration: {s}")
        return total
