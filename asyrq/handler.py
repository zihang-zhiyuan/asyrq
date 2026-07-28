# handler.py — Handler 接口和 HandlerFunc 适配器
# 定义任务处理器的标准接口，1:1 对应 Go asynq 的 Handler 接口和 HandlerFunc

from __future__ import annotations
from typing import Awaitable, Callable
from abc import ABC, abstractmethod

class Handler(ABC):
    """任务处理器抽象基类。

    1:1 对应 Go asynq 的 Handler 接口。
    所有任务处理逻辑都通过实现此接口来定义。

    Usage (类方式):
        class EmailHandler(Handler):
            async def process_task(self, ctx, task):
                data = json.loads(task.payload())
                await send_email(data)
                return None  # 返回 None 表示成功
    """

    @abstractmethod
    async def process_task(self, ctx, task: "Task") -> None:
        """处理任务的核心方法。

        Args:
            ctx: 任务上下文（包含超时和取消信息）
            task: 要处理的任务对象

        Returns:
            None: 返回 None 表示处理成功

        Raises:
            SkipRetry: 抛出此错误将跳过重试直接归档
            Exception: 其他任何异常都会触发重试逻辑
        """
        ...

# HandlerFunc 类型别名 — 任务处理函数
# 1:1 对应 Go asynq 的 HandlerFunc 类型
# 函数签名: async def func(ctx, task) -> None
HandlerFunc = Callable[["Context", "Task"], Awaitable[None]]

class _HandlerFuncAdapter(Handler):
    """HandlerFunc 到 Handler 的适配器。

    将普通函数包装为实现了 Handler 接口的对象。
    内部使用，用户无需直接创建。
    """

    def __init__(self, func: HandlerFunc):
        """初始化适配器。

        Args:
            func: 要包装的处理函数
        """
        self._func = func  # 保存被包装的函数

    async def process_task(self, ctx, task: "Task") -> None:
        """调用被包装的处理函数。

        Args:
            ctx: 任务上下文
            task: 要处理的任务对象
        """
        return await self._func(ctx, task)  # 直接委托给函数

def wrap_handler_func(func: HandlerFunc) -> Handler:
    """将 HandlerFunc 函数包装为 Handler 对象。

    Args:
        func: 异步处理函数

    Returns:
        Handler: 包装后的 Handler 实例
    """
    return _HandlerFuncAdapter(func)

class Context:
    """任务处理上下文，包含超时控制和取消信号。

    传递给 Handler.process_task() 的上下文对象，
    用于控制任务处理的超时和响应取消请求。
    """

    def __init__(self):
        """初始化上下文。"""
        self._cancelled = False  # 取消标志位

    def cancel(self) -> None:
        """取消当前任务的执行。"""
        self._cancelled = True

    @property
    def cancelled(self) -> bool:
        """返回任务是否已被取消。"""
        return self._cancelled
