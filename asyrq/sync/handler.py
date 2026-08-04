# asyrq/sync/handler.py - 同步版 Handler 接口
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable


class SyncHandler(ABC):
    """同步版任务处理器抽象基类。

    Usage:
        class EmailHandler(SyncHandler):
            def process_task(self, ctx, task):
                data = json.loads(task.payload)
                send_email(data)
    """

    @abstractmethod
    def process_task(self, ctx, task) -> None:
        ...


# 同步处理函数类型
SyncHandlerFunc = Callable


class _SyncFuncAdapter(SyncHandler):
    """将同步函数包装为 SyncHandler。"""

    def __init__(self, func: SyncHandlerFunc):
        self._func = func

    def process_task(self, ctx, task) -> None:
        return self._func(ctx, task)


def wrap_sync_handler_func(func: SyncHandlerFunc) -> SyncHandler:
    """包装同步函数为 SyncHandler。"""
    return _SyncFuncAdapter(func)


class SyncServeMux(SyncHandler):
    """同步版任务路由器。

    匹配优先级：精确匹配 > 最长前缀匹配 > catch-all
    """

    def __init__(self):
        self._handlers: dict[str, SyncHandler] = {}
        self._prefix_handlers: dict[str, SyncHandler] = {}
        self._middlewares: list = []

    def handle(self, pattern: str, handler: SyncHandler) -> None:
        if pattern.endswith(":") or pattern == "":
            self._prefix_handlers[pattern] = handler
        else:
            self._handlers[pattern] = handler

    def handle_func(self, pattern: str, func: SyncHandlerFunc) -> None:
        self.handle(pattern, wrap_sync_handler_func(func))

    def task(self, pattern: str):
        def decorator(obj):
            if isinstance(obj, type):
                self.handle(pattern, obj())
            else:
                self.handle_func(pattern, obj)
            return obj
        return decorator

    def use(self, *middlewares) -> None:
        self._middlewares.extend(middlewares)

    def routes(self) -> list[str]:
        patterns = list(self._handlers.keys()) + list(self._prefix_handlers.keys())
        for h in self._handlers.values():
            if isinstance(h, SyncServeMux):
                patterns.extend(h.routes())
        for h in self._prefix_handlers.values():
            if isinstance(h, SyncServeMux):
                patterns.extend(h.routes())
        return sorted(set(patterns))

    def process_task(self, ctx, task) -> None:
        handler = self._find_handler(task.type)
        handler.process_task(ctx, task)

    def _find_handler(self, typename: str) -> SyncHandler:
        if typename in self._handlers:
            return self._handlers[typename]

        best_match = None
        best_len = 0
        for prefix, handler in self._prefix_handlers.items():
            if prefix and typename.startswith(prefix) and len(prefix) > best_len:
                best_match = handler
                best_len = len(prefix)
            elif prefix == "" and best_match is None:
                best_match = handler

        if best_match:
            return best_match

        raise ValueError(f"未找到任务 '{typename}' 的处理器")
