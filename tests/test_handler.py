# tests/test_handler.py — Handler 和 ServeMux 的单元测试

import pytest
from asyrq import Handler, HandlerFunc, Context, ServeMux, Task
from asyrq.middleware import logging_middleware


class TestHandler:
    """Handler 接口的单元测试。"""

    async def test_handler_interface(self):
        """测试 Handler 抽象类。"""
        class MyHandler(Handler):
            async def process_task(self, ctx, task):
                pass

        handler = MyHandler()
        ctx = Context()
        task = Task("test")
        # 不应抛出异常
        await handler.process_task(ctx, task)

    async def test_handler_func(self):
        """测试 HandlerFunc 适配。"""
        called = False

        async def my_func(ctx, task):
            nonlocal called
            called = True

        from asyrq.handler import wrap_handler_func
        handler = wrap_handler_func(my_func)
        ctx = Context()
        task = Task("test")
        await handler.process_task(ctx, task)
        assert called


class TestServeMux:
    """ServeMux 的单元测试。"""

    async def test_exact_match(self):
        """测试精确匹配。"""
        mux = ServeMux()
        results = []

        async def handler_a(ctx, task):
            results.append("a")

        async def handler_b(ctx, task):
            results.append("b")

        mux.handle_func("email:send", handler_a)
        mux.handle_func("email:receive", handler_b)

        ctx = Context()
        await mux.process_task(ctx, Task("email:send"))
        assert results == ["a"]

    async def test_prefix_match(self):
        """测试前缀匹配。"""
        mux = ServeMux()
        results = []

        async def default_handler(ctx, task):
            results.append("default")

        async def specific_handler(ctx, task):
            results.append("specific")

        mux.handle_func("email:", default_handler)
        mux.handle_func("email:send", specific_handler)

        ctx = Context()
        # email:send 应该匹配精确的 specific_handler
        await mux.process_task(ctx, Task("email:send"))
        assert results == ["specific"]

        # email:receive 应该匹配前缀 default_handler
        await mux.process_task(ctx, Task("email:receive"))
        assert results == ["specific", "default"]

    async def test_no_handler_raises(self):
        """测试未匹配到处理器时抛出错误。"""
        mux = ServeMux()
        ctx = Context()

        with pytest.raises(ValueError, match="未找到任务"):
            await mux.process_task(ctx, Task("unknown:task"))


class TestMiddleware:
    """中间件的单元测试。"""

    async def test_middleware_chain_order(self):
        """测试中间件执行顺序（洋葱模型）。"""
        mux = ServeMux()
        order = []

        def make_mw(name):
            def middleware(handler):
                class Wrapper(Handler):
                    async def process_task(self, ctx, task):
                        order.append(f"{name}_before")
                        await handler.process_task(ctx, task)
                        order.append(f"{name}_after")
                return Wrapper()
            return middleware

        async def actual_handler(ctx, task):
            order.append("handler")

        mux.handle_func("test", actual_handler)
        mux.use(make_mw("A"), make_mw("B"), make_mw("C"))

        ctx = Context()
        await mux.process_task(ctx, Task("test"))

        # 洋葱模型: A_before → B_before → C_before → handler → C_after → B_after → A_after
        assert order == [
            "A_before", "B_before", "C_before",
            "handler",
            "C_after", "B_after", "A_after",
        ]
