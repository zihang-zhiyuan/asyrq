# asyrq/sync/client.py - 同步版任务客户端
from __future__ import annotations

import uuid as _uuid
from typing import Optional

import redis as _redis

from ..task import Task, TaskInfo
from ..options import Option, apply_options, validate_options
from ..internal.base import TaskMessage, TaskState, DEFAULT_QUEUE_NAME, DEFAULT_MAX_RETRY, DEFAULT_TIMEOUT
from ..internal.log import Logger, DefaultLogger, LogLevel
from ..connection import RedisClientOpt
from ..errors import EnqueueError
from .rdb import SyncRDB


class SyncClient:
    """同步版任务客户端。

    Usage:
        client = SyncClient(RedisClientOpt(addr="localhost:6379"))
        info = client.enqueue(Task("email:send", payload))
        client.close()
    """

    def __init__(self, redis_conn, logger: Optional[Logger] = None):
        """初始化同步客户端。

        Args:
            redis_conn: RedisClientOpt 或已有的 redis.Redis 客户端
            logger: 自定义日志器
        """
        self._logger = logger or DefaultLogger(level=LogLevel.INFO, log_dir="logs", name="client")

        if isinstance(redis_conn, _redis.Redis):
            self._redis = redis_conn
            self._owns_connection = False
        else:
            self._redis = redis_conn.make_sync_redis_client()
            self._owns_connection = True

        self._broker = SyncRDB(self._redis, self._logger)

    def enqueue(self, task: Task, *opts: Option) -> TaskInfo:
        """入队任务（同步）。"""
        all_opts = list(task.options) + list(opts)
        validate_options(all_opts)
        config = apply_options(all_opts)

        headers = task.headers
        if config.get("result_key"):
            headers["_result_key"] = config["result_key"]

        msg = TaskMessage(
            type=task.typename,
            payload=task.payload,
            id=config.get("task_id") or _uuid.uuid4().hex,
            queue=config["queue"],
            retry=config["retry"],
            timeout=config["timeout"],
            deadline=config["deadline"],
            retention=config["retention"],
            group_key=config["group"],
            headers=headers,
        )

        try:
            if config["unique_ttl"] > 0:
                if config["process_at"] > 0:
                    task_id = self._broker.schedule_unique(msg, config["process_at"], config["unique_ttl"])
                else:
                    task_id = self._broker.enqueue_unique(msg, config["unique_ttl"])
                if not task_id:
                    raise EnqueueError(f"任务已存在（唯一模式）: type={task.typename}")
            elif config["process_at"] > 0:
                task_id = self._broker.schedule(msg, config["process_at"])
            else:
                task_id = self._broker.enqueue(msg)

            return TaskInfo(
                id=task_id, queue=msg.queue, type=msg.type, payload=msg.payload,
                state=TaskState.SCHEDULED if config["process_at"] > 0 else TaskState.PENDING,
                max_retry=msg.retry, retried=0,
                timeout=msg.timeout, deadline=msg.deadline,
                group=msg.group_key, next_process_at=config["process_at"],
                retention=msg.retention, headers=msg.headers,
            )
        except EnqueueError:
            raise
        except Exception as e:
            raise EnqueueError(f"无法将任务入队: {e}") from e

    def ping(self) -> bool:
        return self._broker.ping()

    def close(self) -> None:
        if self._owns_connection:
            self._broker.close()
        self._logger.info("客户端已关闭")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
