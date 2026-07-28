# middleware.py — 中间件模块
# 定义中间件机制，支持在处理链中插入横切关注点，1:1 对应 Go asynq 的 MiddlewareFunc

from __future__ import annotations
from typing import Awaitable, Callable
from .handler import Handler, Context

# MiddlewareFunc 类型别名 — 中间件函数
# 1:1 对应 Go asynq 的 MiddlewareFunc 类型
# func(Handler) -> Handler
MiddlewareFunc = Callable[[Handler], Handler]

class _MiddlewareChain(Handler):
    """中间件链的处理器包装器。

    将 Handler 与中间件链组合起来，按照洋葱模型依次执行。
    最外层中间件最先执行，最内层的是最终的实际 Handler。

    执行顺序（注册顺序 A, B, C）:
        A(before) → B(before) → C(before) → handler → C(after) → B(after) → A(after)

    内部使用，由 ServeMux 管理。
    """

    def __init__(self, handler: Handler, middlewares: List[MiddlewareFunc]):
        """初始化中间件链。

        Args:
            handler: 最终执行的实际处理器
            middlewares: 中间件函数列表（注册顺序）
        """
        # 按照注册顺序反向包装
        # 因为最内层是 handler，我们需要从最内层开始包裹
        wrapped = handler  # 从最内层的 handler 开始
        for mw in reversed(middlewares):  # 反向遍历中间件列表
            wrapped = mw(wrapped)  # 依次用中间件包裹
        self._handler = wrapped  # 保存包装后的处理器

    async def process_task(self, ctx: Context, task: "Task") -> None:
        """执行中间件链处理。

        Args:
            ctx: 任务处理上下文
            task: 要处理的任务
        """
        return await self._handler.process_task(ctx, task)  # 调用最外层包装器

def apply_middlewares(handler: Handler, middlewares: List[MiddlewareFunc]) -> Handler:
    """将中间件列表应用到处理器上。

    Args:
        handler: 最终的实际处理器
        middlewares: 中间件列表（按注册顺序）

    Returns:
        Handler: 包装了所有中间件的 Handler
    """
    if not middlewares:
        return handler  # 无中间件时直接返回原 Handler
    return _MiddlewareChain(handler, middlewares)

# ============================================================================
# 内置中间件示例
# ============================================================================

def logging_middleware(logger=None) -> MiddlewareFunc:
    """创建一个日志中间件。

    记录每个任务的开始、结束和错误信息。

    Args:
        logger: 日志器实例，默认为 None（使用 print）

    Returns:
        MiddlewareFunc: 日志中间件函数
    """
    def middleware(handler: Handler) -> Handler:
        class _LoggingHandler(Handler):
            async def process_task(self, ctx: Context, task: "Task") -> None:
                try:
                    # 记录任务开始
                    _log(logger, f"开始处理任务: {task.type()}")
                    await handler.process_task(ctx, task)  # 调用下一个处理器
                    # 记录任务完成
                    _log(logger, f"任务处理完成: {task.type()}")
                except Exception as e:
                    # 记录任务失败
                    _log(logger, f"任务处理失败: {task.type()}, 错误: {e}")
                    raise  # 重新抛出异常，让 Server 处理重试逻辑
        return _LoggingHandler()
    return middleware

def _log(logger, msg: str) -> None:
    """内部日志辅助函数。"""
    if logger:
        logger.info(msg)
