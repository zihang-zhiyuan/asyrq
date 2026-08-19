# asyrq/sync/rdb.py - 同步版 Redis Broker
# 复用异步版的 Lua 脚本和 Key 函数，仅将 redis.asyncio 替换为 redis（同步）
from __future__ import annotations

import json as _json
import uuid as _uuid
from pathlib import Path as _Path
from typing import Optional

import redis as _redis  # 同步 Redis 客户端

from ..internal.base import (
    TaskMessage, TaskState, ServerInfo,
    DEFAULT_QUEUE_NAME, DEFAULT_MAX_RETRY,
    split_typename,
    pending_key, active_key, scheduled_key, retry_key, archived_key, completed_key,
    task_key, paused_key, lease_key, group_key, all_groups_key,
    unique_key, workers_key, servers_key, processed_key, failed_key,
    server_info_key, scheduler_key, scheduler_history_key, REDIS_PREFIX,
    QUEUE_PAUSED_TTL, LEASE_DURATION, UNIQUE_LOCK_TTL,
    ALL_QUEUES_KEY, SERVERS_KEY,
)
from ..internal.log import Logger, DefaultLogger
from ..errors import EnqueueError

# 复用异步版的 Lua 脚本目录
_LUA_DIR = _Path(__file__).parent.parent / "lua"


def _load_lua_script(filename: str) -> str:
    """加载 Lua 脚本内容。"""
    with open(_LUA_DIR / filename, "r", encoding="utf-8") as f:
        return f.read()


_LUA_SCRIPTS: dict = {}


def _get_lua_script(name: str):
    """获取已注册的 Lua 脚本对象。"""
    return _LUA_SCRIPTS.get(name)


def _register_lua_script(redis_client, name: str) -> None:
    """注册 Lua 脚本到 Redis 客户端。"""
    script_content = _load_lua_script(name + ".lua")
    script_obj = redis_client.register_script(script_content)
    _LUA_SCRIPTS[name] = script_obj


class SyncRDB:
    """同步版 Redis Broker，所有方法同步执行。"""

    def __init__(self, redis_client, logger: Optional[Logger] = None):
        self._client = redis_client
        self._logger = logger or DefaultLogger()
        self._queues_cache: set = set()
        self._register_all_scripts()

    def _register_all_scripts(self) -> None:
        """注册所有 Lua 脚本。"""
        for name in [
            "enqueue", "enqueue_unique", "schedule", "schedule_unique",
            "dequeue", "done", "retry", "archive", "requeue", "requeue_delayed",
            "cleanup_active",
            "forward", "list_lease_expired", "write_server_state",
            "clear_server_state", "aggregation_check", "delete_expired_completed",
            "add_to_group_unique",
        ]:
            _register_lua_script(self._client, name)

    # ======== 连接 ========
    def ping(self) -> bool:
        try:
            return self._client.ping()
        except Exception:
            return False

    def close(self) -> None:
        self._client.close()

    # ======== 入队 ========
    def enqueue(self, msg: TaskMessage) -> str:
        """入队任务。"""
        task_id = msg.id or _uuid.uuid4().hex
        msg.id = task_id
        self._add_queue_to_set(msg.queue)

        task_type, route = split_typename(msg.type)
        tkey = task_key(task_type, route, msg.queue, task_id)
        pkey = pending_key(task_type, route, msg.queue)
        now = self._now_nsec()

        result = _get_lua_script("enqueue")(keys=[tkey, pkey], args=[msg.to_json(), task_id, now])
        if not result:
            raise EnqueueError(f"任务入队失败: task_id={task_id}")
        return task_id

    def enqueue_unique(self, msg: TaskMessage, ttl: int) -> str:
        """唯一入队。"""
        task_id = msg.id or _uuid.uuid4().hex
        msg.id = task_id
        self._add_queue_to_set(msg.queue)
        task_type, route = split_typename(msg.type)
        ukey = unique_key(task_type, route, msg.queue, msg.payload)
        tkey = task_key(task_type, route, msg.queue, task_id)
        pkey = pending_key(task_type, route, msg.queue)
        now = self._now_nsec()
        result = _get_lua_script("enqueue_unique")(keys=[tkey, pkey, ukey], args=[msg.to_json(), task_id, now, ttl])
        return task_id if result else ""

    def schedule(self, msg: TaskMessage, process_at: int) -> str:
        """定时入队。"""
        task_id = msg.id or _uuid.uuid4().hex
        msg.id = task_id
        self._add_queue_to_set(msg.queue)
        task_type, route = split_typename(msg.type)
        tkey = task_key(task_type, route, msg.queue, task_id)
        skey = scheduled_key(task_type, route, msg.queue)
        result = _get_lua_script("schedule")(keys=[tkey, skey], args=[msg.to_json(), task_id, process_at])
        if not result:
            raise EnqueueError(f"任务调度失败: task_id={task_id}")
        return task_id

    def schedule_unique(self, msg: TaskMessage, process_at: int, ttl: int) -> str:
        """唯一定时入队。"""
        task_id = msg.id or _uuid.uuid4().hex
        msg.id = task_id
        self._add_queue_to_set(msg.queue)
        task_type, route = split_typename(msg.type)
        ukey = unique_key(task_type, route, msg.queue, msg.payload)
        tkey = task_key(task_type, route, msg.queue, task_id)
        skey = scheduled_key(task_type, route, msg.queue)
        result = _get_lua_script("schedule_unique")(keys=[tkey, skey, ukey], args=[msg.to_json(), task_id, process_at, ttl])
        return task_id if result else ""

    # ======== 出队 ========
    def dequeue(self, task_routes: list[str], queues: list[str], strict_priority: bool, weights: Optional[dict[str, int]] = None) -> Optional[TaskMessage]:
        """出队任务（按「路由 × 队列」组合尝试）。"""
        import random as _random
        if not task_routes or not queues:
            return None

        route_pairs = [split_typename(r) for r in task_routes]

        if strict_priority:
            for qname in queues:
                for task_type, route in route_pairs:
                    msg = self._try_dequeue_one(task_type, route, qname)
                    if msg:
                        return msg
        elif weights:
            queue_list = [q for q in queues if weights.get(q, 0) > 0]
            if queue_list:
                weight_list = [weights[q] for q in queue_list]
            else:
                # 全部权重 <= 0 时退化为等权随机
                queue_list = list(queues)
                weight_list = [1] * len(queue_list)
            r = _random.randint(1, max(1, sum(weight_list)))
            start_idx = 0
            cumulative = 0
            for i, (qname, w) in enumerate(zip(queue_list, weight_list)):
                cumulative += w
                if r <= cumulative:
                    start_idx = i
                    break
            for offset in range(len(queue_list)):
                qname = queue_list[(start_idx + offset) % len(queue_list)]
                for task_type, route in route_pairs:
                    msg = self._try_dequeue_one(task_type, route, qname)
                    if msg:
                        return msg
        else:
            qname = _random.choice(queues)
            for task_type, route in route_pairs:
                msg = self._try_dequeue_one(task_type, route, qname)
                if msg:
                    return msg
        return None

    def _try_dequeue_one(self, task_type: str, route: str, qname: str) -> Optional[TaskMessage]:
        """从单个队列出队。"""
        pkey = pending_key(task_type, route, qname)
        akey = active_key(task_type, route, qname)
        lkey = lease_key(task_type, route, qname)
        pauskey = paused_key(task_type, route, qname)
        now = self._now_nsec()

        task_id = _get_lua_script("dequeue")(
            keys=[pkey, akey, lkey, pauskey],
            args=[str(now), str(LEASE_DURATION), "0"],
        )
        if not task_id:
            return None

        task_id_str = task_id.decode("utf-8") if isinstance(task_id, bytes) else task_id
        tkey = task_key(task_type, route, qname, task_id_str)
        data = self._client.hget(tkey, "msg")
        if not data:
            # 任务数据已丢失：原子清理 active/lease 孤儿条目，避免永久残留
            _get_lua_script("cleanup_active")(
                keys=[akey, lkey], args=[task_id_str],
            )
            self._logger.warn(f"出队的任务数据丢失，已清理孤儿条目: {task_id_str}")
            return None
        return TaskMessage.from_json(data.decode("utf-8"))

    # ======== 完成/重试/归档 ========
    def done(self, msg: TaskMessage) -> None:
        task_type, route = split_typename(msg.type)
        akey = active_key(task_type, route, msg.queue)
        lkey = lease_key(task_type, route, msg.queue)
        tkey = task_key(task_type, route, msg.queue, msg.id)
        ckey = completed_key(task_type, route, msg.queue)
        pckey = processed_key(task_type, route, msg.queue)
        now = self._now_nsec()
        _get_lua_script("done")(keys=[akey, lkey, tkey, ckey, pckey], args=[msg.id, str(now), str(msg.retention), ""])

    def retry(self, msg: TaskMessage, process_at: int, error_msg: str, is_failure: bool) -> None:
        task_type, route = split_typename(msg.type)
        akey = active_key(task_type, route, msg.queue)
        lkey = lease_key(task_type, route, msg.queue)
        rkey = retry_key(task_type, route, msg.queue)
        tkey = task_key(task_type, route, msg.queue, msg.id)
        fkey = failed_key(task_type, route, msg.queue)
        now = self._now_nsec()
        _get_lua_script("retry")(keys=[akey, lkey, rkey, tkey, fkey], args=[msg.id, str(process_at), error_msg, str(msg.retried + 1), str(now)])

    def archive(self, msg: TaskMessage, error_msg: str) -> None:
        task_type, route = split_typename(msg.type)
        akey = active_key(task_type, route, msg.queue)
        lkey = lease_key(task_type, route, msg.queue)
        arkey = archived_key(task_type, route, msg.queue)
        tkey = task_key(task_type, route, msg.queue, msg.id)
        fkey = failed_key(task_type, route, msg.queue)
        now = self._now_nsec()
        _get_lua_script("archive")(keys=[akey, lkey, arkey, tkey, fkey], args=[msg.id, error_msg, str(msg.retried), str(now)])

    def requeue(self, msg: TaskMessage) -> None:
        """将 active 任务放回 pending 列表（如路由并发容量不足时暂回队列）。"""
        task_type, route = split_typename(msg.type)
        akey = active_key(task_type, route, msg.queue)
        pkey = pending_key(task_type, route, msg.queue)
        lkey = lease_key(task_type, route, msg.queue)
        tkey = task_key(task_type, route, msg.queue, msg.id)
        _get_lua_script("requeue")(keys=[akey, pkey, lkey, tkey], args=[msg.id])

    # ======== 前移 ========
    def forward_if_ready(self, task_routes: list[str], queues: list[str]) -> None:
        """将到期任务从 scheduled/retry 前移到 pending。"""
        now = self._now_nsec()
        batch_size = 100
        for task_route in task_routes:
            task_type, route = split_typename(task_route)
            for qname in queues:
                skey = scheduled_key(task_type, route, qname)
                rkey = retry_key(task_type, route, qname)
                pkey = pending_key(task_type, route, qname)
                count = _get_lua_script("forward")(
                    keys=[skey, rkey, pkey, task_type, route, qname],
                    args=[str(now), str(batch_size)],
                )
                if count and int(count) > 0:
                    self._logger.debug(f"前移 {count} 个到期任务: type={task_type} route={route} queue={qname}")

    # ======== 租约 ========
    def list_lease_expired(self, task_routes: list[str], queues: list[str]) -> list[TaskMessage]:
        now = self._now_nsec()
        max_count = 50
        expired = []
        for task_route in task_routes:
            task_type, route = split_typename(task_route)
            for qname in queues:
                lkey = lease_key(task_type, route, qname)
                task_ids = _get_lua_script("list_lease_expired")(keys=[lkey], args=[str(now), str(max_count)])
                if not task_ids:
                    continue
                for tid in task_ids:
                    tid_str = tid.decode("utf-8") if isinstance(tid, bytes) else tid
                    tkey = task_key(task_type, route, qname, tid_str)
                    data = self._client.hget(tkey, "msg")
                    if data:
                        expired.append(TaskMessage.from_json(data.decode("utf-8")))
                    else:
                        # 有租约但任务数据已丢失：清理孤儿，避免 lease/active 永久残留
                        _get_lua_script("cleanup_active")(
                            keys=[active_key(task_type, route, qname), lkey],
                            args=[tid_str],
                        )
                        self._logger.warn(f"租约过期但任务数据丢失，已清理孤儿条目: {tid_str}")
        return expired

    def extend_lease(self, task_type: str, route: str, qname: str, task_ids: list[str], lease_seconds: int) -> None:
        lkey = lease_key(task_type, route, qname)
        now = self._now_nsec()
        lease_until = now + lease_seconds * 1_000_000_000
        pipe = self._client.pipeline()
        for tid in task_ids:
            pipe.hset(lkey, tid, str(lease_until))
        pipe.execute()

    def requeue_delayed(
        self, task_type: str, route: str, qname: str, task_id: str, process_at: int
    ) -> bool:
        """把当前执行中的任务原封不动地放回延迟队列（scheduled ZSet）。"""
        result = _get_lua_script("requeue_delayed")(
            keys=[
                active_key(task_type, route, qname),
                lease_key(task_type, route, qname),
                scheduled_key(task_type, route, qname),
                task_key(task_type, route, qname, task_id),
            ],
            args=[task_id, str(process_at)],
        )
        if not result:
            self._logger.warn(f"重新入队失败，任务不在 active（可能已被处理）: {task_id}")
            return False
        return True

    # ======== 服务器状态 ========
    def write_server_state(self, info: ServerInfo, workers: list, ttl: int) -> None:
        skey = servers_key()
        wkey = workers_key(info.host, info.pid, info.server_id)
        now = self._now_nsec()
        info_json = _json.dumps({
            "host": info.host, "pid": info.pid, "server_id": info.server_id,
            "concurrency": info.concurrency, "queues": info.queues,
            "strict_priority": info.strict_priority, "status": info.status,
            "start_time": info.start_time, "active_worker_count": info.active_worker_count,
            "routes": info.routes,
        }, ensure_ascii=False)
        _get_lua_script("write_server_state")(keys=[skey, wkey], args=[info.server_id, info_json, str(now), str(ttl)])

    def clear_server_state(self, host: str, pid: int, server_id: str) -> None:
        skey = servers_key()
        wkey = workers_key(host, pid, server_id)
        _get_lua_script("clear_server_state")(keys=[skey, wkey], args=[server_id])

    def list_servers(self) -> list:
        skey = servers_key()
        min_score = self._now_nsec() - 60 * 1_000_000_000
        server_ids = self._client.zrangebyscore(skey, min_score, "+inf")
        servers = []
        for sid in server_ids:
            sid_str = sid.decode("utf-8") if isinstance(sid, bytes) else sid
            data = self._client.get(server_info_key(sid_str))
            if data:
                servers.append(ServerInfo(**_json.loads(data)))
        return servers

    # ======== 队列管理 ========
    def list_queues(self) -> list[str]:
        queues = self._client.smembers(ALL_QUEUES_KEY)
        return [q.decode("utf-8") if isinstance(q, bytes) else q for q in queues]

    def current_queue_stats(self, task_type: str, route: str, qname: str) -> dict[str, int]:
        pipe = self._client.pipeline()
        pipe.llen(pending_key(task_type, route, qname))
        pipe.llen(active_key(task_type, route, qname))
        pipe.zcard(scheduled_key(task_type, route, qname))
        pipe.zcard(retry_key(task_type, route, qname))
        pipe.zcard(archived_key(task_type, route, qname))
        pipe.zcard(completed_key(task_type, route, qname))
        results = pipe.execute()
        return {
            "pending": results[0], "active": results[1], "scheduled": results[2],
            "retry": results[3], "archived": results[4], "completed": results[5],
        }

    # ======== 辅助 ========
    def _add_queue_to_set(self, qname: str) -> None:
        if qname in self._queues_cache:
            return
        self._client.sadd(ALL_QUEUES_KEY, qname)
        self._queues_cache.add(qname)

    @staticmethod
    def _now_nsec() -> int:
        import time
        return int(time.time() * 1_000_000_000)
