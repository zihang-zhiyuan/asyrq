# client.py — 任务客户端模块
# 提供任务入队、调度和去重功能，1:1 对应 Go asynq 的 Client 类型

from __future__ import annotations
import uuid as _uuid


import redis.asyncio as _redis

from .task import Task, TaskInfo, new_task
from .options import Option, apply_options, validate_options
from .internal.base import (
    TaskMessage, TaskState, DEFAULT_QUEUE_NAME,
    DEFAULT_MAX_RETRY, DEFAULT_TIMEOUT, UNIQUE_LOCK_TTL,
)
from .internal.rdb import RDB
from .internal import timeutil as _timeutil
from .internal.log import Logger, DefaultLogger
from .connection import RedisClientOpt
from .errors import EnqueueError


class Client:
    """任务客户端，负责任务的创建和入队。

    1:1 对应 Go asynq 的 Client 结构体。

    Usage:
        client = Client(RedisClientOpt(addr="localhost:6379"))
        task = Task("email:send", payload)
        info = await client.enqueue(task)
        info = await client.enqueue(task, Queue("critical"), MaxRetry(3))
    """

    def __init__(self, redis_conn: RedisClientOpt | _redis.Redis, logger: Optional[Logger] = None):
        """初始化客户端。

        Args:
            redis_conn: Redis 连接配置（RedisClientOpt）或已有的 Redis 客户端
            logger: 自定义日志器
        """
        self._logger = logger or DefaultLogger()  # 设置日志器

        # 根据参数类型选择创建或复用 Redis 客户端
        if isinstance(redis_conn, _redis.Redis):
            # 直接使用传入的 Redis 客户端
            self._redis = redis_conn
            self._owns_connection = False  # 不是自己创建的连接，关闭时不释放
        else:
            # 根据 RedisClientOpt 创建新的 Redis 客户端
            self._redis = redis_conn.make_redis_client()
            self._owns_connection = True  # 自己创建的连接，关闭时释放

        # 创建 RDB Broker，封装所有 Redis 操作
        self._broker = RDB(self._redis, self._logger)

    async def enqueue(
        self,
        task: Task,
        *opts: Option,
    ) -> TaskInfo:
        """将任务入队以便异步处理。

        1:1 对应 Go asynq 的 Client.Enqueue 方法。

        支持即时执行、延迟执行和定时执行：
        - 无 ProcessIn/ProcessAt: 任务立即进入 pending 队列
        - ProcessIn(n): 任务在 n 秒后执行
        - ProcessAt(t): 任务在时刻 t 执行

        Args:
            task: 要入队的任务
            *opts: 额外的任务选项（会与 Task 创建时的选项合并）

        Returns:
            TaskInfo: 入队后的任务信息

        Raises:
            EnqueueError: 入队失败
            ValueError: 选项冲突
        """
        # 合并任务创建时的选项和入队时的选项
        all_opts = list(task.options()) + list(opts)

        # 验证选项无冲突
        validate_options(all_opts)

        # 解析所有选项为配置字典
        config = apply_options(all_opts)

        # 构建 TaskMessage（任务消息体）
        msg = TaskMessage(
            type=task.typename,              # 任务类型名
            payload=task.payload(),           # 负载数据
            id=config.get("task_id") or _uuid.uuid4().hex,  # 任务 ID
            queue=config["queue"],            # 目标队列
            retry=config["retry"],            # 最大重试次数
            timeout=config["timeout"],        # 超时时间
            deadline=config["deadline"],      # 截止时间
            retention=config["retention"],    # 保留时间
            group_key=config["group"],        # 聚合组
            headers=task.headers(),           # 请求头
        )

        # 根据配置选择入队方式
        try:
            if config["unique_ttl"] > 0:
                # 唯一入队 — 去重模式
                task_id = await self._enqueue_unique(msg, config)
                if not task_id:
                    raise EnqueueError(f"任务已存在（唯一模式）: type={task.typename}")
            elif config["process_at"] > 0:
                # 定时执行 — 延迟或定时模式
                task_id = await self._schedule(msg, config)
            else:
                # 立即执行 — 标准入队
                task_id = await self._broker.enqueue(msg)

            # 构造并返回 TaskInfo
            return TaskInfo(
                id=task_id,
                queue=msg.queue,
                type=msg.type,
                payload=msg.payload,
                state=(
                    TaskState.SCHEDULED
                    if config["process_at"] > 0
                    else TaskState.PENDING
                ),
                max_retry=msg.retry,
                retried=0,
                timeout=msg.timeout,
                deadline=msg.deadline,
                group=msg.group_key,
                next_process_at=config["process_at"],
                retention=msg.retention,
                headers=msg.headers,
            )

        except Exception as e:
            self._logger.error(f"任务入队失败: {e}")
            raise EnqueueError(f"无法将任务入队: {e}") from e

    async def _enqueue_unique(self, msg: TaskMessage, config: dict) -> str:
        """唯一模式入队（去重）。"""
        if config["process_at"] > 0:
            # 唯一定时入队
            return await self._broker.schedule_unique(
                msg, config["process_at"], config["unique_ttl"]
            )
        elif config["group"]:
            # 唯一聚合组入队
            return await self._broker.add_to_group_unique(
                msg, config["group"], config["unique_ttl"]
            )
        else:
            # 唯一立即入队
            return await self._broker.enqueue_unique(msg, config["unique_ttl"])

    async def _schedule(self, msg: TaskMessage, config: dict) -> str:
        """定时模式入队。"""
        if config["unique_ttl"] > 0:
            return await self._broker.schedule_unique(
                msg, config["process_at"], config["unique_ttl"]
            )
        return await self._broker.schedule(msg, config["process_at"])

    async def schedule(
        self,
        task: Task,
        process_at: int,
        *opts: Option,
    ) -> TaskInfo:
        """将任务调度到指定时间执行。

        1:1 对应 Go asynq 的 Client.Schedule 方法。

        Args:
            task: 任务对象
            process_at: 执行时间（纳秒时间戳）
            *opts: 额外选项

        Returns:
            TaskInfo: 调度后的任务信息
        """
        from .options import ProcessAt
        return await self.enqueue(task, ProcessAt(process_at), *opts)

    async def ping(self) -> bool:
        """检查 Redis 连接是否正常。

        Returns:
            bool: True 表示正常
        """
        return await self._broker.ping()

    async def close(self) -> None:
        """关闭客户端连接。"""
        if self._owns_connection:
            await self._broker.close()  # 只有自己创建的连接才关闭
        self._logger.info("客户端已关闭")

    async def __aenter__(self) -> "Client":
        """异步上下文管理器入口。支持 async with Client(...) as c:"""
        return self

    async def __aexit__(self, *args) -> None:
        """异步上下文管理器出口。"""
        await self.close()
