# handler.py - Handler 接口
from __future__ import annotations

import time as _time
from typing import Awaitable, Callable
from abc import ABC, abstractmethod


class Handler(ABC):
    """任务处理器抽象基类。

    Pythonic API（新）:
        class EmailHandler(Handler):
            async def process_task(self, task):
                data = json.loads(task.payload)
                await task.finish({"status": "ok"})

    兼容旧 API:
        class EmailHandler(Handler):
            async def process_task(self, ctx, task):
                ...
    """

    @abstractmethod
    async def process_task(self, *args) -> None:
        """处理任务。

        新签名: process_task(self, task)
        旧签名: process_task(self, ctx, task)  仍兼容
        """
        ...


HandlerFunc = Callable[..., Awaitable[None]]


class _HandlerFuncAdapter(Handler):
    """函数适配器。"""

    def __init__(self, func: HandlerFunc):
        self._func = func

    async def process_task(self, *args) -> None:
        await self._func(*args)


def wrap_handler_func(func: HandlerFunc) -> Handler:
    """包装函数为 Handler。"""
    return _HandlerFuncAdapter(func)


class Context:
    """[已废弃] 任务上下文，功能已合并到 Task。

    保留用于向后兼容，新代码请直接用 task.id / task.timeout / task.remaining 等。
    """

    def __init__(self):
        self._cancelled = False
        self._start_ns: int = int(_time.time() * 1e9)
        self.deadline: int = 0
        self.timeout: int = 0
        self.task_id: str = ""

    def cancel(self) -> None:
        self._cancelled = True

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    @property
    def remaining(self) -> int:
        if self.timeout <= 0:
            return -1
        elapsed = (int(_time.time() * 1e9) - self._start_ns) // 1_000_000_000
        return max(0, self.timeout - elapsed)
