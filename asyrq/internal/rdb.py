# internal/rdb.py — Redis Broker 实现模块
# 封装所有 Redis 操作，使用 Lua 脚本保证原子性，1:1 对应 Go asynq 的 internal/rdb 包

from __future__ import annotations
from typing import Any, Dict, List, Optional
import json as _json
import os as _os
import uuid as _uuid
from pathlib import Path as _Path

import redis.asyncio as _redis

from .base import (
    Broker, TaskMessage, TaskState, ServerInfo,
    DEFAULT_QUEUE_NAME, DEFAULT_MAX_RETRY,
    ALL_QUEUES_KEY, SERVERS_KEY,
    split_typename,
    pending_key, active_key, scheduled_key, retry_key, archived_key, completed_key,
    task_key, paused_key, lease_key, group_key, all_groups_key,
    unique_key, workers_key, servers_key, processed_key, failed_key,
    server_info_key, scheduler_key, scheduler_history_key, REDIS_PREFIX,
    QUEUE_PAUSED_TTL, LEASE_DURATION, UNIQUE_LOCK_TTL,
)
from .log import Logger, DefaultLogger, LogLevel
# 导入自定义错误类型，替代裸 Exception
from ..errors import EnqueueError

# ============================================================================
# Lua 脚本加载
# ============================================================================

# Lua 脚本目录路径
_LUA_DIR = _Path(__file__).parent.parent / "lua"

def _load_lua_script(filename: str) -> str:
    """从文件加载 Lua 脚本内容。

    Args:
        filename: Lua 脚本文件名

    Returns:
        str: Lua 脚本源代码
    """
    script_path = _LUA_DIR / filename  # 构建脚本文件路径
    with open(script_path, "r", encoding="utf-8") as f:  # 以 UTF-8 编码打开
        return f.read()  # 读取并返回脚本内容

# 预加载所有 Lua 脚本（启动时一次性加载到内存）
_LUA_SCRIPTS: Dict[str, Any] = {}

def _get_lua_script(name: str) -> Any:
    """获取已注册的 Lua 脚本对象（支持 SCRIPT LOAD/EVALSHA 优化）。

    Args:
        name: 脚本名称（不含 .lua 后缀）

    Returns:
        Any: redis 库的 Script 对象
    """
    return _LUA_SCRIPTS.get(name)

def _register_lua_script(redis_client: _redis.Redis, name: str) -> None:
    """注册一个 Lua 脚本到 Redis 客户端。

    Args:
        redis_client: Redis 客户端实例
        name: 脚本名称（不含 .lua 后缀）
    """
    script_content = _load_lua_script(name + ".lua")  # 加载脚本源码
    script_obj = redis_client.register_script(script_content)  # 注册到 Redis
    _LUA_SCRIPTS[name] = script_obj  # 缓存脚本对象

# ============================================================================
# RDB — Redis Broker 实现
# ============================================================================

class RDB(Broker):
    """Redis Broker 实现，封装所有与 Redis 的交互。

    1:1 对应 Go asynq 的 internal/rdb.RDB 结构体。
    实现 Broker 接口中定义的所有方法，使用 Lua 脚本保证原子操作。
    """

    def __init__(
        self,
        redis_client: _redis.Redis,
        logger: Optional[Logger] = None,
    ):
        """初始化 RDB 实例。

        Args:
            redis_client: 异步 Redis 客户端实例
            logger: 日志器，默认使用 DefaultLogger
        """
        self._client = redis_client      # Redis 客户端引用
        self._logger = logger or DefaultLogger()  # 日志器
        self._queues_cache: set = set()  # 队列名本地缓存（减少 SADD 调用）
        self._register_all_scripts()     # 注册所有 Lua 脚本

    def _register_all_scripts(self) -> None:
        """注册所有 Lua 脚本到 Redis 客户端。"""
        script_names = [
            "enqueue", "enqueue_unique", "schedule", "schedule_unique",
            "dequeue", "done", "retry", "archive", "requeue", "requeue_delayed",
            "cleanup_active",
            "forward", "list_lease_expired", "write_server_state",
            "clear_server_state", "aggregation_check", "delete_expired_completed",
            "add_to_group_unique",
        ]
        for name in script_names:  # 遍历所有脚本名
            _register_lua_script(self._client, name)  # 注册每个脚本

    # ========================================================================
    # 连接管理
    # ========================================================================

    async def ping(self) -> bool:
        """检查 Redis 连接是否正常。

        Returns:
            bool: True 表示连接正常
        """
        try:
            return await self._client.ping()  # Redis PING 命令
        except Exception:
            return False  # 连接失败返回 False

    async def close(self) -> None:
        """关闭 Redis 连接。"""
        await self._client.aclose()  # 异步关闭连接

    # ========================================================================
    # 任务入队操作
    # ========================================================================

    async def enqueue(self, msg: TaskMessage) -> str:
        """将任务消息入队到 pending 队列。

        Args:
            msg: 要入队的任务消息

        Returns:
            str: 任务 ID
        """
        # 生成任务 ID（使用提供的 ID 或自动生成 UUID）
        task_id = msg.id or _uuid.uuid4().hex  # UUID hex 字符串作为任务 ID
        msg.id = task_id  # 确保消息中记录了 ID

        # 注册队列名到全局集合（用于队列发现）
        await self._add_queue_to_set(msg.queue)

        # 准备 Lua 脚本参数
        task_type, route = split_typename(msg.type)
        tkey = task_key(task_type, route, msg.queue, task_id)   # 任务 Hash key
        pkey = pending_key(task_type, route, msg.queue)          # pending 列表 key
        now = self._now_nsec()                 # 当前纳秒时间戳

        # 执行原子入队 Lua 脚本
        result = await _get_lua_script("enqueue")(
            keys=[tkey, pkey],
            args=[msg.to_json(), task_id, now],
        )

        if not result:
            raise EnqueueError(f"任务入队失败: task_id={task_id}")  # 脚本返回 0 = 失败

        self._logger.debug(f"任务已入队: id={task_id}, type={msg.type}, queue={msg.queue}")
        return task_id  # 返回任务 ID

    async def enqueue_unique(self, msg: TaskMessage, ttl: int) -> str:
        """唯一地入队任务（去重）。

        Args:
            msg: 任务消息
            ttl: 唯一锁的 TTL（秒）

        Returns:
            str: 任务 ID，重复任务返回空字符串
        """
        task_id = msg.id or _uuid.uuid4().hex
        msg.id = task_id

        # 添加到队列集合
        await self._add_queue_to_set(msg.queue)

        # 计算唯一键
        task_type, route = split_typename(msg.type)
        ukey = unique_key(task_type, route, msg.queue, msg.payload)

        tkey = task_key(task_type, route, msg.queue, task_id)
        pkey = pending_key(task_type, route, msg.queue)
        now = self._now_nsec()

        result = await _get_lua_script("enqueue_unique")(
            keys=[tkey, pkey, ukey],
            args=[msg.to_json(), task_id, now, ttl],
        )

        if not result:
            return ""  # 任务已存在（被去重）

        return task_id

    async def schedule(self, msg: TaskMessage, process_at: int) -> str:
        """调度任务到未来某个时间执行。

        Args:
            msg: 任务消息
            process_at: 计划执行时间（纳秒时间戳）

        Returns:
            str: 任务 ID
        """
        task_id = msg.id or _uuid.uuid4().hex
        msg.id = task_id

        await self._add_queue_to_set(msg.queue)

        task_type, route = split_typename(msg.type)
        tkey = task_key(task_type, route, msg.queue, task_id)
        skey = scheduled_key(task_type, route, msg.queue)

        result = await _get_lua_script("schedule")(
            keys=[tkey, skey],
            args=[msg.to_json(), task_id, process_at],
        )

        if not result:
            raise EnqueueError(f"任务调度失败: task_id={task_id}")

        return task_id

    async def schedule_unique(self, msg: TaskMessage, process_at: int, ttl: int) -> str:
        """唯一定时调度任务。

        Args:
            msg: 任务消息
            process_at: 计划执行时间
            ttl: 唯一锁 TTL

        Returns:
            str: 任务 ID，重复则返回空字符串
        """
        task_id = msg.id or _uuid.uuid4().hex
        msg.id = task_id

        await self._add_queue_to_set(msg.queue)

        task_type, route = split_typename(msg.type)
        ukey = unique_key(task_type, route, msg.queue, msg.payload)
        tkey = task_key(task_type, route, msg.queue, task_id)
        skey = scheduled_key(task_type, route, msg.queue)

        result = await _get_lua_script("schedule_unique")(
            keys=[tkey, skey, ukey],
            args=[msg.to_json(), task_id, process_at, ttl],
        )

        if not result:
            return ""  # 任务已存在

        return task_id

    # ========================================================================
    # 任务出队
    # ========================================================================

    async def dequeue(
        self,
        task_routes: List[str],
        queues: List[str],
        strict_priority: bool,
        weights: Optional[dict[str, int]] = None,
    ) -> Optional[TaskMessage]:
        """从队列中取出一个待处理任务。

        key 布局为 asyrq:tasks:{task_type}:{route}:{queue}:{suffix}，
        因此按「路由 × 队列」组合逐个尝试出队。

        Args:
            task_routes: 服务器注册的路由列表（如 "pmos_liaoning:rqdj"）
            queues: 按优先级排序的队列名列表
            strict_priority: 严格优先级模式
            weights: 队列权重映射 {queue_name: priority}，用于加权随机选择

        Returns:
            Optional[TaskMessage]: 出队的任务，无任务时返回 None
        """
        import random as _random

        if not task_routes or not queues:
            return None  # 没有路由或队列可供出队

        route_pairs = [split_typename(r) for r in task_routes]

        # 对于严格优先级，按队列顺序、再按路由顺序检查
        if strict_priority:
            for qname in queues:
                for task_type, route in route_pairs:
                    msg = await self._try_dequeue_one(task_type, route, qname)
                    if msg:
                        return msg
        else:
            # 加权优先级：按权重随机选择队列，逐个尝试直到找到任务
            if weights:
                # 权重为 0 的队列不参与消费
                queue_list = [q for q in queues if weights.get(q, 0) > 0]
                if queue_list:
                    weight_list = [weights[q] for q in queue_list]
                else:
                    # 全部权重 <= 0 时退化为等权随机
                    queue_list = list(queues)
                    weight_list = [1] * len(queue_list)
                # 记录已尝试的空队列，全部为空时才返回 None
                tried: set = set()
                while len(tried) < len(queue_list):
                    # 从未尝试的队列中按权重随机选择
                    available = [(q, w) for q, w in zip(queue_list, weight_list) if q not in tried]
                    total_w = sum(w for _, w in available)
                    r = _random.randint(1, max(1, total_w))
                    cumulative = 0
                    for qname, w in available:
                        cumulative += w
                        if r <= cumulative:
                            for task_type, route in route_pairs:
                                msg = await self._try_dequeue_one(task_type, route, qname)
                                if msg:
                                    return msg
                            tried.add(qname)  # 该队列为空，标记并继续尝试
                            break
            else:
                # 无权重配置时随机选择队列，逐个尝试
                shuffled = list(queues)
                _random.shuffle(shuffled)
                for qname in shuffled:
                    for task_type, route in route_pairs:
                        msg = await self._try_dequeue_one(task_type, route, qname)
                        if msg:
                            return msg

        return None  # 所有组合都为空

    async def _try_dequeue_one(self, task_type: str, route: str, qname: str) -> Optional[TaskMessage]:
        """从单个 (task_type, route, queue) 组合尝试出队一个任务。

        Args:
            task_type: 任务类型（如 "pmos_liaoning"）
            route: 路由（如 "rqdj"）
            qname: 队列名

        Returns:
            Optional[TaskMessage]: 出队的任务消息，队列为空时返回 None
        """
        pkey = pending_key(task_type, route, qname)       # pending 列表 key
        akey = active_key(task_type, route, qname)         # active 列表 key
        lkey = lease_key(task_type, route, qname)          # lease hash key
        pauskey = paused_key(task_type, route, qname)      # 暂停标记 key
        now = self._now_nsec()           # 当前纳秒时间戳

        # 执行出队 Lua 脚本
        task_id = await _get_lua_script("dequeue")(
            keys=[pkey, akey, lkey, pauskey],
            args=[str(now), str(LEASE_DURATION), "0"],  # 非严格优先级
        )

        if not task_id:
            return None  # 队列为空

        # 读取任务数据
        task_id_str = task_id.decode("utf-8") if isinstance(task_id, bytes) else task_id
        tkey = task_key(task_type, route, qname, task_id_str)
        data = await self._client.hget(tkey, "msg")  # 从 Hash 中获取 msg 字段

        if not data:
            # 任务数据已丢失：原子清理 active/lease 孤儿条目，避免永久残留
            await _get_lua_script("cleanup_active")(
                keys=[akey, lkey], args=[task_id_str],
            )
            self._logger.warn(f"出队的任务数据丢失，已清理孤儿条目: {task_id_str}")
            return None

        msg = TaskMessage.from_json(data.decode("utf-8"))  # 反序列化
        return msg

    # ========================================================================
    # 任务完成
    # ========================================================================

    async def done(self, msg: TaskMessage) -> None:
        """标记任务为已完成。

        Args:
            msg: 已完成的任务消息
        """
        task_type, route = split_typename(msg.type)
        akey = active_key(task_type, route, msg.queue)
        lkey = lease_key(task_type, route, msg.queue)
        tkey = task_key(task_type, route, msg.queue, msg.id)
        ckey = completed_key(task_type, route, msg.queue)
        pckey = processed_key(task_type, route, msg.queue)  # 已处理计数器
        now = self._now_nsec()

        await _get_lua_script("done")(
            keys=[akey, lkey, tkey, ckey, pckey],
            args=[msg.id, str(now), str(msg.retention), ""],
        )

    async def mark_as_complete(self, msg: TaskMessage) -> None:
        """将任务移入已完成集合（带结果数据）。

        Args:
            msg: 要标记为完成的任务消息
        """
        task_type, route = split_typename(msg.type)
        akey = active_key(task_type, route, msg.queue)
        lkey = lease_key(task_type, route, msg.queue)
        tkey = task_key(task_type, route, msg.queue, msg.id)
        ckey = completed_key(task_type, route, msg.queue)
        pckey = processed_key(task_type, route, msg.queue)
        now = self._now_nsec()

        # 获取 result 数据（如果有）
        result_data = ""
        task_hash = await self._client.hgetall(tkey)
        if task_hash and b"result" in task_hash:
            result_data = task_hash[b"result"].decode("utf-8")

        await _get_lua_script("done")(
            keys=[akey, lkey, tkey, ckey, pckey],
            args=[msg.id, str(now), str(msg.retention), result_data],
        )

    # ========================================================================
    # 任务重试与归档
    # ========================================================================

    async def retry(self, msg: TaskMessage, process_at: int, error_msg: str, is_failure: bool) -> None:
        """将失败任务标记为重试状态。

        Args:
            msg: 要重试的任务消息
            process_at: 下次重试时间（纳秒时间戳）
            error_msg: 错误消息
            is_failure: 是否计入失败统计
        """
        task_type, route = split_typename(msg.type)
        akey = active_key(task_type, route, msg.queue)
        lkey = lease_key(task_type, route, msg.queue)
        rkey = retry_key(task_type, route, msg.queue)
        tkey = task_key(task_type, route, msg.queue, msg.id)
        fkey = failed_key(task_type, route, msg.queue)
        now = self._now_nsec()

        await _get_lua_script("retry")(
            keys=[akey, lkey, rkey, tkey, fkey],
            args=[msg.id, str(process_at), error_msg, str(msg.retried + 1), str(now)],
        )

    async def archive(self, msg: TaskMessage, error_msg: str) -> None:
        """将任务归档（重试耗尽后）。

        Args:
            msg: 要归档的任务消息
            error_msg: 错误消息
        """
        task_type, route = split_typename(msg.type)
        akey = active_key(task_type, route, msg.queue)
        lkey = lease_key(task_type, route, msg.queue)
        arkey = archived_key(task_type, route, msg.queue)
        tkey = task_key(task_type, route, msg.queue, msg.id)
        fkey = failed_key(task_type, route, msg.queue)
        now = self._now_nsec()

        await _get_lua_script("archive")(
            keys=[akey, lkey, arkey, tkey, fkey],
            args=[msg.id, error_msg, str(msg.retried), str(now)],
        )

    async def requeue(self, msg: TaskMessage) -> None:
        """将 active 任务放回 pending 列表（优雅关闭时使用）。

        Args:
            msg: 要重新入队的任务消息
        """
        task_type, route = split_typename(msg.type)
        akey = active_key(task_type, route, msg.queue)
        pkey = pending_key(task_type, route, msg.queue)
        lkey = lease_key(task_type, route, msg.queue)
        tkey = task_key(task_type, route, msg.queue, msg.id)

        await _get_lua_script("requeue")(
            keys=[akey, pkey, lkey, tkey],
            args=[msg.id],
        )

    async def requeue_delayed(
        self, task_type: str, route: str, qname: str, task_id: str, process_at: int
    ) -> bool:
        """把当前执行中的任务原封不动地放回延迟队列（scheduled ZSet）。

        保持相同 task_id / payload，不增加 retried/failed 计数。
        供 handler 调用 task.retry_in(seconds) 时使用。

        Returns:
            bool: True 表示已成功重新入队；False 表示任务不在 active（可能已被处理）
        """
        result = await _get_lua_script("requeue_delayed")(
            keys=[
                active_key(task_type, route, qname),
                lease_key(task_type, route, qname),
                scheduled_key(task_type, route, qname),
                task_key(task_type, route, qname, task_id),
            ],
            args=[task_id, str(process_at)],
        )

        if not result:
            self._logger.warn(
                f"重新入队失败，任务不在 active（可能已被处理）: {task_id}"
            )
            return False
        return True

    # ========================================================================
    # 任务前移
    # ========================================================================

    async def forward_if_ready(self, task_routes: List[str], queues: List[str]) -> None:
        """检查 scheduled 和 retry 集合，将到期任务前移到 pending 列表。

        Args:
            task_routes: 服务器注册的路由列表
            queues: 要检查的队列名列表
        """
        now = self._now_nsec()
        batch_size = 100  # 每次最多前移 100 个任务

        for task_route in task_routes:
            task_type, route = split_typename(task_route)
            for qname in queues:
                skey = scheduled_key(task_type, route, qname)   # scheduled ZSet
                rkey = retry_key(task_type, route, qname)        # retry ZSet
                pkey = pending_key(task_type, route, qname)      # pending 列表

                # 执行前移 Lua 脚本（KEYS[4..6] 用于重建任务数据 key）
                count = await _get_lua_script("forward")(
                    keys=[skey, rkey, pkey, task_type, route, qname],
                    args=[str(now), str(batch_size)],
                )

                if count and int(count) > 0:
                    self._logger.debug(f"前移 {count} 个到期任务: type={task_type} route={route} queue={qname}")

    # ========================================================================
    # 租约管理
    # ========================================================================

    async def list_lease_expired(self, task_routes: List[str], queues: List[str]) -> List[TaskMessage]:
        """列出所有租约到期的活跃任务（孤儿任务检测）。

        Args:
            task_routes: 服务器注册的路由列表
            queues: 要检查的队列列表

        Returns:
            list[TaskMessage]: 租约到期的任务消息列表
        """
        now = self._now_nsec()
        max_count = 50  # 每次最多返回 50 个
        expired_tasks = []

        for task_route in task_routes:
            task_type, route = split_typename(task_route)
            for qname in queues:
                lkey = lease_key(task_type, route, qname)

                task_ids = await _get_lua_script("list_lease_expired")(
                    keys=[lkey],
                    args=[str(now), str(max_count)],
                )

                if not task_ids:
                    continue

                # 读取每个过期任务的数据
                for tid in task_ids:
                    tid_str = tid.decode("utf-8") if isinstance(tid, bytes) else tid
                    tkey = task_key(task_type, route, qname, tid_str)
                    data = await self._client.hget(tkey, "msg")
                    if data:
                        msg = TaskMessage.from_json(data.decode("utf-8"))
                        expired_tasks.append(msg)
                    else:
                        # 有租约但任务数据已丢失：清理孤儿，避免 lease/active 永久残留
                        await _get_lua_script("cleanup_active")(
                            keys=[active_key(task_type, route, qname), lkey],
                            args=[tid_str],
                        )
                        self._logger.warn(f"租约过期但任务数据丢失，已清理孤儿条目: {tid_str}")

        return expired_tasks

    async def extend_lease(self, task_type: str, route: str, qname: str, task_ids: List[str], lease_seconds: int) -> None:
        """延长任务的活动租约。

        Args:
            task_type: 任务类型（如 "pmos_liaoning"）
            route: 路由（如 "rqdj"）
            qname: 队列名
            task_ids: 需要延长租约的任务 ID 列表
            lease_seconds: 租约延长秒数
        """
        lkey = lease_key(task_type, route, qname)
        now = self._now_nsec()
        lease_until = now + lease_seconds * 1_000_000_000  # 新的租约到期时间

        # 批量更新租约
        pipe = self._client.pipeline()  # 使用 pipeline 提高性能
        for tid in task_ids:
            pipe.hset(lkey, tid, str(lease_until))  # 更新每个任务的租约
        await pipe.execute()  # 批量执行

    # ========================================================================
    # 服务器状态
    # ========================================================================

    async def write_server_state(self, info: ServerInfo, workers: List[dict[str, Any]], ttl: int) -> None:
        """写入服务器状态和 worker 信息到 Redis。

        Args:
            info: 服务器信息
            workers: worker 信息列表
            ttl: 心跳 TTL 秒数
        """
        skey = servers_key()
        wkey = workers_key(info.host, info.pid, info.server_id)
        now = self._now_nsec()
        info_json = _json.dumps({
            "host": info.host,
            "pid": info.pid,
            "server_id": info.server_id,
            "concurrency": info.concurrency,
            "queues": info.queues,
            "strict_priority": info.strict_priority,
            "status": info.status,
            "start_time": info.start_time,
            "active_worker_count": info.active_worker_count,
            "routes": info.routes,
        }, ensure_ascii=False)

        await _get_lua_script("write_server_state")(
            keys=[skey, wkey],
            args=[info.server_id, info_json, str(now), str(ttl)],
        )

    async def clear_server_state(self, host: str, pid: int, server_id: str) -> None:
        """清除服务器状态。

        Args:
            host: 主机名
            pid: 进程 ID
            server_id: 服务器唯一 ID
        """
        skey = servers_key()
        wkey = workers_key(host, pid, server_id)

        await _get_lua_script("clear_server_state")(
            keys=[skey, wkey],
            args=[server_id],
        )

    async def list_servers(self) -> List[ServerInfo]:
        """列出所有在线的服务器。

        Returns:
            list[ServerInfo]: 服务器信息列表
        """
        skey = servers_key()
        # 只获取过去 60 秒内有心跳的服务器
        min_score = self._now_nsec() - 60 * 1_000_000_000

        server_ids = await self._client.zrangebyscore(skey, min_score, "+inf")

        servers = []
        for sid in server_ids:
            sid_str = sid.decode("utf-8") if isinstance(sid, bytes) else sid
            skey_detail = server_info_key(sid_str)
            data = await self._client.get(skey_detail)
            if data:
                info_dict = _json.loads(data.decode("utf-8"))
                servers.append(ServerInfo(**info_dict))

        return servers

    # ========================================================================
    # 任务信息查询
    # ========================================================================

    async def get_task_info(self, task_type: str, route: str, qname: str, task_id: str) -> Optional[dict[str, Any]]:
        """获取指定任务的完整信息。

        Args:
            task_type: 任务类型
            route: 路由
            qname: 队列名
            task_id: 任务 ID

        Returns:
            Optional[dict[str, Any]]: 任务信息字典
        """
        tkey = task_key(task_type, route, qname, task_id)
        data = await self._client.hgetall(tkey)  # 获取所有 Hash 字段

        if not data:
            return None  # 任务不存在

        # 将 bytes key 转为字符串
        info = {}
        for k, v in data.items():
            key_str = k.decode("utf-8") if isinstance(k, bytes) else k
            val_str = v.decode("utf-8") if isinstance(v, bytes) else v
            info[key_str] = val_str

        # 添加反转后的 TaskMessage 字段
        if "msg" in info:
            msg = TaskMessage.from_json(info["msg"])
            info.update(msg.to_dict())
            del info["msg"]  # 移除原始 JSON

        return info

    async def list_tasks(
        self, task_type: str, route: str, qname: str, state: TaskState, page_size: int = 100, page_num: int = 1
    ) -> List[dict[str, Any]]:
        """分页列出指定状态的任务。

        Args:
            task_type: 任务类型
            route: 路由
            qname: 队列名
            state: 任务状态
            page_size: 每页数量
            page_num: 页码（从1开始）

        Returns:
            list[dict[str, Any]]: 任务信息列表
        """
        # 根据状态选择 key
        key_map = {
            TaskState.PENDING: pending_key(task_type, route, qname),
            TaskState.ACTIVE: active_key(task_type, route, qname),
            TaskState.SCHEDULED: scheduled_key(task_type, route, qname),
            TaskState.RETRY: retry_key(task_type, route, qname),
            TaskState.ARCHIVED: archived_key(task_type, route, qname),
            TaskState.COMPLETED: completed_key(task_type, route, qname),
        }
        key = key_map.get(state)
        if not key:
            return []

        # 根据数据结构和状态获取任务 ID 列表
        if state in (TaskState.PENDING, TaskState.ACTIVE):
            # List 类型 — LRANGE
            start = (page_num - 1) * page_size
            stop = start + page_size - 1
            task_ids = await self._client.lrange(key, start, stop)
        else:
            # ZSet 类型 — ZRANGE（按 score）
            start = (page_num - 1) * page_size
            stop = start + page_size - 1
            task_ids = await self._client.zrange(key, start, stop)

        # 读取每个任务的详细信息
        tasks = []
        for tid in task_ids:
            tid_str = tid.decode("utf-8") if isinstance(tid, bytes) else tid
            info = await self.get_task_info(task_type, route, qname, tid_str)
            if info:
                info["id"] = tid_str
                tasks.append(info)

        return tasks

    # ========================================================================
    # 队列管理
    # ========================================================================

    async def list_queues(self) -> List[str]:
        """列出所有队列名。

        Returns:
            list[str]: 队列名列表
        """
        queues_data = await self._client.smembers(ALL_QUEUES_KEY)  # SMEMBERS 获取所有成员
        return [
            q.decode("utf-8") if isinstance(q, bytes) else q
            for q in queues_data
        ]

    async def pause_queue(self, task_type: str, route: str, qname: str) -> None:
        """暂停指定队列的处理。

        Args:
            task_type: 任务类型
            route: 路由
            qname: 队列名
        """
        pkey = paused_key(task_type, route, qname)
        await self._client.set(pkey, "1", ex=QUEUE_PAUSED_TTL)  # SET with TTL

    async def unpause_queue(self, task_type: str, route: str, qname: str) -> None:
        """恢复指定队列的处理。

        Args:
            task_type: 任务类型
            route: 路由
            qname: 队列名
        """
        pkey = paused_key(task_type, route, qname)
        await self._client.delete(pkey)  # 删除暂停标记

    async def delete_queue(self, task_type: str, route: str, qname: str, force: bool = False) -> None:
        """删除队列及其所有任务。

        Args:
            task_type: 任务类型
            route: 路由
            qname: 队列名
            force: 是否强制删除（包含有活跃任务的队列）

        Raises:
            Exception: 队列有活跃任务且未使用 force 模式
        """
        if not force:
            # 先检查是否有活跃任务
            akey = active_key(task_type, route, qname)
            active_count = await self._client.llen(akey)
            if active_count > 0:
                raise Exception(f"队列 {qname} 有 {active_count} 个活跃任务，请使用 force=True")

        # 删除队列相关的所有 Redis key
        keys_to_delete = [
            pending_key(task_type, route, qname), active_key(task_type, route, qname),
            scheduled_key(task_type, route, qname), retry_key(task_type, route, qname),
            archived_key(task_type, route, qname), completed_key(task_type, route, qname),
            paused_key(task_type, route, qname), lease_key(task_type, route, qname),
        ]

        # 使用 pipeline 批量删除
        pipe = self._client.pipeline()
        for key in keys_to_delete:
            pipe.delete(key)
        await pipe.execute()

        # 从全局队列集合中移除
        await self._client.srem(ALL_QUEUES_KEY, qname)

    async def current_queue_stats(self, task_type: str, route: str, qname: str) -> Dict[str, int]:
        """获取队列统计信息。

        Args:
            task_type: 任务类型
            route: 路由
            qname: 队列名

        Returns:
            dict[str, int]: 包含 pending, active, scheduled, retry, archived, completed 计数
        """
        # 并行获取各状态的计数
        pipe = self._client.pipeline()
        pipe.llen(pending_key(task_type, route, qname))       # pending 数量
        pipe.llen(active_key(task_type, route, qname))         # active 数量
        pipe.zcard(scheduled_key(task_type, route, qname))     # scheduled 数量
        pipe.zcard(retry_key(task_type, route, qname))          # retry 数量
        pipe.zcard(archived_key(task_type, route, qname))       # archived 数量
        pipe.zcard(completed_key(task_type, route, qname))      # completed 数量
        results = await pipe.execute()        # 执行所有命令

        return {
            "pending": results[0],
            "active": results[1],
            "scheduled": results[2],
            "retry": results[3],
            "archived": results[4],
            "completed": results[5],
        }

    # ========================================================================
    # 任务组聚合
    # ========================================================================

    async def add_to_group(self, msg: TaskMessage, gname: str) -> str:
        """将任务添加到聚合组。

        Args:
            msg: 任务消息
            gname: 组名

        Returns:
            str: 任务 ID
        """
        task_id = msg.id or _uuid.uuid4().hex
        msg.id = task_id

        await self._add_queue_to_set(msg.queue)

        task_type, route = split_typename(msg.type)

        # 写入任务数据
        tkey = task_key(task_type, route, msg.queue, task_id)
        await self._client.hset(tkey, mapping={
            "msg": msg.to_json(),
            "state": "aggregating",
            "group_key": gname,
        })

        # 添加到聚合组 ZSet
        gkey = group_key(task_type, route, msg.queue, gname)
        now = self._now_nsec()
        await self._client.zadd(gkey, {task_id: now})

        # 将组名加入全局组集合
        agkey = all_groups_key(task_type, route, msg.queue)
        await self._client.sadd(agkey, gname)

        return task_id

    async def add_to_group_unique(self, msg: TaskMessage, gname: str, ttl: int) -> str:
        """唯一地将任务添加到聚合组（Lua 脚本保证原子性）。

        Args:
            msg: 任务消息
            gname: 组名
            ttl: 唯一的 TTL

        Returns:
            str: 任务 ID，重复返回空字符串
        """
        task_id = msg.id or _uuid.uuid4().hex
        msg.id = task_id

        await self._add_queue_to_set(msg.queue)

        task_type, route = split_typename(msg.type)
        ukey = unique_key(task_type, route, msg.queue, msg.payload)
        tkey = task_key(task_type, route, msg.queue, task_id)
        gkey = group_key(task_type, route, msg.queue, gname)
        agkey = all_groups_key(task_type, route, msg.queue)
        now = self._now_nsec()

        result = await _get_lua_script("add_to_group_unique")(
            keys=[tkey, gkey, agkey, ukey],
            args=[msg.to_json(), task_id, gname, str(now), str(ttl)],
        )

        if not result:
            return ""  # 任务已存在

        return task_id

    async def list_groups(self, task_type: str, route: str, qname: str) -> List[str]:
        """列出队列中所有聚合组名。

        Args:
            task_type: 任务类型
            route: 路由
            qname: 队列名

        Returns:
            list[str]: 组名列表
        """
        agkey = all_groups_key(task_type, route, qname)
        groups = await self._client.smembers(agkey)
        return [
            g.decode("utf-8") if isinstance(g, bytes) else g
            for g in groups
        ]

    async def aggregation_check(
        self, task_type: str, route: str, qname: str, gname: str, max_delay: int, max_size: int, grace_period: int
    ) -> Optional[List[str]]:
        """检查聚合组是否应该被触发。

        Args:
            task_type: 任务类型
            route: 路由
            qname: 队列名
            gname: 组名
            max_delay: 最大延迟
            max_size: 最大任务数
            grace_period: 宽限期

        Returns:
            Optional[List[str]]: 触发时返回组内任务 ID 列表（组已被原子取出），
                                 None 表示暂不需要触发
        """
        gkey = group_key(task_type, route, qname, gname)
        now = self._now_nsec()

        result = await _get_lua_script("aggregation_check")(
            keys=[gkey],
            args=[str(now), gname, str(max_delay), str(max_size), str(grace_period)],
        )

        if result:
            # Lua 已原子删除组 ZSet 并返回任务 ID 列表
            return [
                t.decode("utf-8") if isinstance(t, bytes) else t
                for t in result
            ]

        return None

    # ========================================================================
    # 调度器操作
    # ========================================================================

    async def write_scheduler_entries(self, entries: List[dict[str, Any]], ttl: int) -> None:
        """批量写入调度器条目到 Redis。

        Args:
            entries: 条目列表
            ttl: TTL 秒数
        """
        pipe = self._client.pipeline()
        for entry in entries:
            skey = scheduler_key(entry["id"])
            pipe.set(skey, _json.dumps(entry, ensure_ascii=False), ex=ttl)
        await pipe.execute()

    async def list_scheduler_entries(self) -> List[dict[str, Any]]:
        """列出所有注册的调度器条目。

        Returns:
            list[dict[str, Any]]: 调度器条目列表
        """
        # 扫描所有调度器 key
        cursor = 0
        entries = []
        while True:
            cursor, keys = await self._client.scan(
                cursor, match=f"{REDIS_PREFIX}:schedulers:*", count=100
            )
            for key in keys:
                data = await self._client.get(key)
                if data:
                    entries.append(_json.loads(data.decode("utf-8")))
            if cursor == 0:
                break
        return entries

    async def record_scheduler_enqueue_event(self, entry_id: str, task_id: str) -> None:
        """记录调度器的入队事件。

        Args:
            entry_id: 调度器条目 ID
            task_id: 入队的任务 ID
        """
        hkey = scheduler_history_key(entry_id)
        now = self._now_nsec()
        await self._client.zadd(hkey, {task_id: now})  # 添加到历史 ZSet

    # ========================================================================
    # 辅助方法
    # ========================================================================

    async def _add_queue_to_set(self, qname: str) -> None:
        """将队列名添加到全局队列集合（带缓存优化）。

        使用本地缓存避免重复 SADD 调用，只有在缓存未命中时才执行 Redis 操作。

        Args:
            qname: 队列名
        """
        if qname in self._queues_cache:
            return  # 已在缓存中，跳过
        await self._client.sadd(ALL_QUEUES_KEY, qname)  # 添加到集合
        self._queues_cache.add(qname)  # 更新缓存

    @staticmethod
    def _now_nsec() -> int:
        """返回当前时间的纳秒级 Unix 时间戳。"""
        import time
        return int(time.time() * 1_000_000_000)  # 秒 → 纳秒
