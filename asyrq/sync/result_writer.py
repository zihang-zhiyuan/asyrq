# asyrq/sync/result_writer.py - 同步版结果写入器
from __future__ import annotations

from typing import Optional


class SyncResultWriter:
    """同步版结果写入器，方法均为同步。"""

    def __init__(self, task_id: str, broker, qname: str, typename: str = "", result_key: str = ""):
        self._task_id = task_id
        self._broker = broker
        self._qname = qname
        self._typename = typename
        self._result_key = result_key

    def task_id(self) -> str:
        return self._task_id

    def update_state(self, data: dict) -> None:
        """上报中间状态（立即写入 Redis）。Key: asyrq:{type}:state:{task_id}"""
        import json as _json
        key = f"asyrq:{self._typename}:state:{self._task_id}"
        self._broker._client.set(key, _json.dumps(data, ensure_ascii=False), ex=3600)

    def finish(self, data: dict, retention: int = 0) -> None:
        """写入最终结果。默认 asyrq:{type}:result:{task_id}；配置 ResultKey 时用 {result_key}:{task_id}"""
        import json as _json
        if self._result_key:
            key = f"{self._result_key}:{self._task_id}"
        else:
            key = f"asyrq:{self._typename}:result:{self._task_id}"
        kwargs = {"ex": retention} if retention > 0 else {}
        self._broker._client.set(key, _json.dumps(data, ensure_ascii=False), **kwargs)

    def requeue(self, seconds: int) -> None:
        """把当前任务原封不动地放回延迟队列，seconds 秒后再取出执行。"""
        import time as _time
        from ..internal.base import split_typename

        task_type, route = split_typename(self._typename)
        process_at = int(_time.time() * 1_000_000_000) + seconds * 1_000_000_000
        self._broker.requeue_delayed(
            task_type, route, self._qname, self._task_id, process_at,
        )
