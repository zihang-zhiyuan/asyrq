# client.py - 任务客户端
from __future__ import annotations

import uuid as _uuid
from typing import Optional

import redis.asyncio as _redis

from .task import Task, TaskInfo
from .options import Option, apply_options, validate_options, Queue, MaxRetry, Timeout, ProcessIn, ProcessAt, Unique, Retention, TaskID, ResultKey, Group
from .internal.base import TaskMessage, TaskState, DEFAULT_QUEUE_NAME, DEFAULT_MAX_RETRY, DEFAULT_TIMEOUT
from .internal.rdb import RDB
from .internal import timeutil as _timeutil
from .internal.log import Logger, DefaultLogger, LogLevel
from .connection import RedisClientOpt
from .errors import EnqueueError


class Client:
    def __init__(
        self,
        redis_conn: Any = None,
        server: str = "",
        redis_addr: str = "127.0.0.1:6379",
        redis_password: str = "",
        redis_db: int = 0,
        redis_client: Optional[_redis.Redis] = None,
        logger: Optional[Logger] = None,
        log_dir: str = "logs",
    ):
        self._logger = logger or DefaultLogger(level=LogLevel.INFO, log_dir=log_dir, name="client")
        self._server = server  # 服务端 code，自动拼 route 前缀

        if redis_client:
            self._redis = redis_client; self._owns_connection = False; self._redis_created = True
        elif redis_conn is not None:
            if not hasattr(redis_conn, "make_redis_client"):
                raise TypeError("redis_conn 必须是 RedisClientOpt / RedisFailoverClientOpt / RedisClusterClientOpt")
            self._redis_opts = redis_conn
            self._owns_connection = True; self._redis_created = False
        else:
            self._redis_opts = RedisClientOpt(addr=redis_addr, password=redis_password, db=redis_db)
            self._owns_connection = True; self._redis_created = False

        self._broker: Optional[RDB] = None

    def _ensure_redis(self) -> None:
        if not self._redis_created:
            self._redis = self._redis_opts.make_redis_client(); self._redis_created = True
        if self._broker is None:
            self._broker = RDB(self._redis, self._logger)

    async def enqueue(
        self,
        task: Any = None,
        *opts: Option,
        route: str = "",
        queue: str = "default",
        max_retry: Optional[int] = None,
        timeout: Optional[int] = None,
        process_in: Optional[int] = None,
        process_at: Optional[int] = None,
        unique: Optional[int] = None,
        retention: Optional[int] = None,
        task_id: Optional[str] = None,
        result_key: Optional[str] = None,
        group: Optional[str] = None,
    ) -> TaskInfo:
        """入队任务。

        Args:
            route: 路由名（如 "rqdj"），自动拼 server:route
            task: Task 对象（必填，位置参数）
            queue: 队列名（default/critical/...）
            max_retry/process_in/unique/...: 关键字参数选项
        """
        self._ensure_redis()

        if task is None:
            raise ValueError("必须传入 task 参数")

        # 构建完整路由名: server:route
        full_typename = f"{self._server}:{route}" if self._server else route

        # task 可以是 bytes/str（自动包装为 Task）或 Task 对象
        if isinstance(task, (bytes, str)):
            if not full_typename:
                raise ValueError("使用 bytes/str 入队时必须提供 route 或 server")
            task = Task(full_typename, task if isinstance(task, bytes) else task.encode())
        elif not isinstance(task, Task):
            raise ValueError("task 必须是 bytes/str/Task")

        # 未指定 server/route 时保留 Task 自带的类型名
        if not full_typename:
            full_typename = task.typename
        task._typename = full_typename

        # 关键字参数转 Option
        kw_opts: list[Option] = []
        if max_retry is not None: kw_opts.append(MaxRetry(max_retry))
        if timeout is not None: kw_opts.append(Timeout(timeout))
        if process_in is not None: kw_opts.append(ProcessIn(process_in))
        if process_at is not None: kw_opts.append(ProcessAt(process_at))
        if unique is not None: kw_opts.append(Unique(unique))
        if retention is not None: kw_opts.append(Retention(retention))
        if task_id is not None: kw_opts.append(TaskID(task_id))
        if result_key is not None: kw_opts.append(ResultKey(result_key))
        if group is not None: kw_opts.append(Group(group))

        all_opts = list(task.options) + list(opts) + kw_opts
        # 把 queue 关键字也加入
        if queue != "default":
            all_opts.append(Queue(queue))

        validate_options(all_opts)
        config = apply_options(all_opts)

        headers = task.headers
        if config.get("result_key"):
            headers["_result_key"] = config["result_key"]

        msg = TaskMessage(
            type=full_typename, payload=task.payload,
            id=config.get("task_id") or _uuid.uuid4().hex,
            queue=config["queue"], retry=config["retry"],
            timeout=config["timeout"], deadline=config["deadline"],
            retention=config["retention"], group_key=config["group"],
            headers=headers,
        )

        try:
            if config["unique_ttl"] > 0:
                task_id = await self._enqueue_unique(msg, config)
                if not task_id:
                    raise EnqueueError(f"任务已存在（唯一模式）: type={full_typename}")
            elif config["process_at"] > 0:
                task_id = await self._schedule(msg, config)
            else:
                task_id = await self._broker.enqueue(msg)

            return TaskInfo(
                id=task_id, queue=msg.queue, type=msg.type,
                payload=msg.payload,
                state=TaskState.SCHEDULED if config["process_at"] > 0 else TaskState.PENDING,
                max_retry=msg.retry, retried=0, timeout=msg.timeout,
                deadline=msg.deadline, group=msg.group_key,
                next_process_at=config["process_at"], retention=msg.retention,
                headers=msg.headers,
            )
        except EnqueueError:
            # 唯一模式重复入队是预期的业务分支，不作为错误日志
            raise
        except Exception as e:
            self._logger.error("任务入队失败: %s", e)
            raise EnqueueError(f"无法将任务入队: {e}") from e

    async def _enqueue_unique(self, msg: TaskMessage, config: dict) -> str:
        if config["process_at"] > 0: return await self._broker.schedule_unique(msg, config["process_at"], config["unique_ttl"])
        elif config["group"]: return await self._broker.add_to_group_unique(msg, config["group"], config["unique_ttl"])
        else: return await self._broker.enqueue_unique(msg, config["unique_ttl"])

    async def _schedule(self, msg: TaskMessage, config: dict) -> str:
        if config["unique_ttl"] > 0: return await self._broker.schedule_unique(msg, config["process_at"], config["unique_ttl"])
        return await self._broker.schedule(msg, config["process_at"])

    async def ping(self) -> bool:
        self._ensure_redis()
        return await self._broker.ping()

    async def close(self) -> None:
        if self._owns_connection and self._broker:
            await self._broker.close()
        self._logger.info("客户端已关闭")

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()
