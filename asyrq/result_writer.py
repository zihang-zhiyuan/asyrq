# result_writer.py — 结果写入器模块
from __future__ import annotations

from typing import Optional


class ResultWriter:
    """任务结果写入器。

    两个方法都立即写入 Redis（统一使用 asyrq: 固定前缀）:
        update_state(dict) → asyrq:{type}:state:{task_id}  1小时TTL
        finish(dict)       → asyrq:{type}:result:{task_id}  Retention 控制 TTL
    """

    def __init__(self, task_id: str, broker, qname: str, typename: str = "", result_key: str = ""):
        self._task_id = task_id
        self._broker = broker
        self._qname = qname
        self._typename = typename
        self._result_key = result_key
        self._data: Optional[bytes] = None  # 兼容旧版 write()

    def task_id(self) -> str:
        return self._task_id

    async def update_state(self, data: dict) -> None:
        """上报中间状态（立即写入 Redis，可多次调用）。

        Key: asyrq:{task_type}:state:{task_id}  TTL: 1 小时
        """
        import json as _json
        key = f"asyrq:{self._typename}:state:{self._task_id}"
        await self._broker._client.set(key, _json.dumps(data, ensure_ascii=False), ex=3600)

    async def finish(self, data: dict, retention: int = 0) -> None:
        """写入最终结果（立即写入 Redis）。

        Key: 默认 asyrq:{task_type}:result:{task_id}；
             配置了 ResultKey 时使用 {result_key}:{task_id}
        retention > 0 时设置过期时间
        """
        import json as _json
        if self._result_key:
            key = f"{self._result_key}:{self._task_id}"
        else:
            key = f"asyrq:{self._typename}:result:{self._task_id}"
        kwargs = {"ex": retention} if retention > 0 else {}
        await self._broker._client.set(key, _json.dumps(data, ensure_ascii=False), **kwargs)

    # ---- 兼容旧版 ----
    async def write(self, data: bytes) -> int:
        self._data = data
        return len(data)

    async def report_progress(self, data: dict) -> None:
        await self.update_state(data)

    def get_data(self) -> Optional[bytes]:
        return self._data
