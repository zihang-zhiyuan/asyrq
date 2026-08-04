# asyrq/sync/result_writer.py - 同步版结果写入器
from __future__ import annotations

from typing import Optional


class SyncResultWriter:
    """同步版结果写入器，方法均为同步。"""

    def __init__(self, task_id: str, broker, qname: str, typename: str = ""):
        self._task_id = task_id
        self._broker = broker
        self._qname = qname
        self._typename = typename

    def task_id(self) -> str:
        return self._task_id

    def update_state(self, data: dict) -> None:
        """上报中间状态（立即写入 Redis）。"""
        import json as _json
        key = f"{self._typename}:state:{self._task_id}"
        self._broker._client.set(key, _json.dumps(data, ensure_ascii=False), ex=3600)

    def finish(self, data: dict, retention: int = 0) -> None:
        """写入最终结果。"""
        import json as _json
        key = f"{self._typename}:result:{self._task_id}"
        kwargs = {"ex": retention} if retention > 0 else {}
        self._broker._client.set(key, _json.dumps(data, ensure_ascii=False), **kwargs)
