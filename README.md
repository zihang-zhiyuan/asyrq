# asyrq

<div align="center">

**Python 异步分布式任务队列 — 基于 Redis，1:1 复刻 Go [asynq](https://github.com/hibiken/asynq)**

[![PyPI version](https://img.shields.io/pypi/v/asyrq)](https://pypi.org/project/asyrq/)
[![Python](https://img.shields.io/pypi/pyversions/asyrq)](https://pypi.org/project/asyrq/)
[![License](https://img.shields.io/pypi/l/asyrq)](https://github.com/zihang-zhiyuan/asyrq/blob/master/LICENSE)

</div>

> ⚡ **本项目由 AI 辅助开发。** 代码、文档、测试和 CI 工作流均在 AI 协作下完成，人工负责审核与引导。如果你发现任何问题，欢迎提交 [Issue](https://github.com/zihang-zhiyuan/asyrq/issues) 或 PR。

---

## 目录

- [特性](#特性)
- [安装](#安装)
- [快速开始](#快速开始)
- [核心概念](#核心概念)
- [Client — 任务生产者](#client--任务生产者)
- [Server — 任务消费者](#server--任务消费者)
- [中间件](#中间件)
- [定时调度](#定时调度)
- [任务组聚合](#任务组聚合)
- [结果获取](#结果获取)
- [配置参考](#配置参考)
- [架构](#架构)
- [监控](#监控)
- [与 Go asynq 对照](#与-go-asynq-对照)
- [License](#license)

## 特性

| 类别 | 功能 |
|------|------|
| **入队方式** | 即时 / 延迟 / 定时 / 去重 / 聚合组 |
| **优先级** | 多队列权重分配 / 严格优先级模式 |
| **重试机制** | 指数退避 + 随机抖动 / 自定义延迟函数 / SkipRetry |
| **优雅退出** | 信号处理（Unix + Windows）/ 超时强制终止 / 状态清理 |
| **故障恢复** | 租约机制 / 孤儿任务自动检测恢复 |
| **定时调度** | Cron 表达式 / `@every` 间隔 / 预入队回调 |
| **并发控制** | 全局并发 + 按任务类型 Semaphore 限流 |
| **中间件** | 洋葱模型，支持日志、重试、超时等横切关注点 |
| **路由匹配** | 精确匹配 + 前缀匹配 + catch-all 兜底 |
| **结果存储** | 自动 / 自定义 ResultKey / 保留期清理 |
| **监控** | 全部 key 使用 `asyrq:` 前缀（任务: `asyrq:tasks:{type}:{route}:{queue}`，服务器: `asyrq:servers`） |
| **连接** | 单节点 / 哨兵 / 集群 / URI 解析 |
| **API 风格** | FastAPI 风格 `@app.task()` 装饰器 |

## 安装

```bash
pip install asyrq
```

- Python ≥ 3.10
- Redis ≥ 4.0

## 快速开始

> 确保本地 Redis 在 `127.0.0.1:6379` 运行。

### 1. 创建生产者

```python
import asyncio, json
from asyrq import Client, Task, RedisClientOpt, Queue, MaxRetry, ProcessIn, Unique

async def main():
    client = Client(RedisClientOpt(addr="127.0.0.1:6379"))

    # 普通任务
    await client.enqueue(
        Task("email:send", json.dumps({"to": "alice@example.com"}).encode())
    )

    # 高优先队列，限制重试
    await client.enqueue(
        Task("email:send", json.dumps({"to": "bob@example.com"}).encode()),
        Queue("critical"),
        MaxRetry(3),
    )

    # 延迟 30 秒
    await client.enqueue(
        Task("email:remind", b"remind-me"),
        ProcessIn(30),
    )

    # 60 秒内去重
    try:
        await client.enqueue(Task("cache:warm", b"daily"), Unique(60))
    except Exception:
        print("重复任务，已跳过")

    await client.close()

asyncio.run(main())
```

### 2. 创建消费者（FastAPI 风格）

```python
import asyncio, json
from asyrq import Server, Config, RedisClientOpt, Context, Task

app = Server(
    RedisClientOpt(addr="127.0.0.1:6379"),
    Config(
        name="邮件服务",
        code="email-worker",
        concurrency=10,
        queues={"critical": 6, "default": 3},
    ),
)

@app.task("email:send")
async def handle_email(ctx: Context, task: Task):
    data = json.loads(task.payload())
    print(f"发送邮件: {data['to']}")

    # 写入结果 — 自动存到 email:send:result:{task_id}
    # 上报中间状态（实时写入 Redis，1 小时过期）
    await task.update_state({"step": "sending", "to": data["to"]})

    # 写入最终结果 -> 自动存到 email:send:result:{task_id}
    await task.finish({"status": "ok", "to": data["to"]})

asyncio.run(app.run())  # 阻塞，Ctrl+C 优雅退出
```

### 3. 使用 ServeMux + 自动合并路由

```python
import asyncio
from asyrq import Server, ServeMux, Config, RedisClientOpt, Context, Task

app = Server(RedisClientOpt(addr="127.0.0.1:6379"))
mux = ServeMux()

@mux.task("email:send")
async def handle_email(ctx: Context, task: Task):
    print("外部 mux：发送邮件")

@app.task("sms:send")           # 装饰器注册
async def handle_sms(ctx: Context, task: Task):
    print("装饰器：发送短信")

# start() 自动合并两个 mux 的路由
await app.start(mux)
# 或者阻塞运行：
# asyncio.run(app.run(mux))
```

## 核心概念

### 任务 (Task)

```python
from asyrq import Task

task = Task(
    "email:send",                              # 类型名，用于路由匹配
    json.dumps({"to": "hi@example.com"}).encode(),  # 负载数据（bytes）
    headers={"request_id": "abc-123"},          # 可选 headers
)
```

### 任务选项 (Option)

所有选项均为 `client.enqueue(task, *opts)` 的可选参数：

| 选项 | 说明 | 示例 |
|------|------|------|
| `Queue("name")` | 目标队列（大小写不敏感） | `Queue("critical")` |
| `MaxRetry(n)` | 最大重试次数 | `MaxRetry(3)` |
| `Timeout(secs)` | 单次处理超时 | `Timeout(30)` |
| `ProcessIn(secs)` | 延迟秒数后执行 | `ProcessIn(60)` |
| `ProcessAt(nsec)` | 指定纳秒时间戳执行 | `ProcessAt(ts)` |
| `Unique(ttl)` | TTL 秒内去重 | `Unique(300)` |
| `Retention(secs)` | 完成后保留秒数 | `Retention(3600)` |
| `Group("name")` | 任务聚合组 | `Group("batch")` |
| `TaskID("id")` | 自定义任务 ID | `TaskID("my-id")` |
| `ResultKey("key")` | 自定义结果存储 key | `ResultKey("my:result")` |

## Client — 任务生产者

```python
from asyrq import Client, RedisClientOpt

client = Client(RedisClientOpt(addr="127.0.0.1:6379"))
# 或从 URI 解析
client = Client(RedisClientOpt.from_uri("redis://:pwd@127.0.0.1:6379/0"))

await client.ping()         # 检查连接
await client.enqueue(...)   # 入队
await client.schedule(...)  # 定时调度
await client.close()        # 关闭连接
```

支持连接模式：

```python
# 哨兵模式
RedisClientOpt(
    sentinel_addrs=["sentinel1:26379", "sentinel2:26379"],
    master_name="mymaster",
)

# 集群模式
RedisClusterClientOpt(addrs=["node1:6379", "node2:6379"])
```

## Server — 任务消费者

### Handler 接口

```python
from asyrq import Handler, Context, Task

class EmailHandler(Handler):
    async def process_task(self, ctx: Context, task: Task):
        data = json.loads(task.payload())
        await send_email(data)
```

### ServeMux 路由

```python
from asyrq import ServeMux

mux = ServeMux()

# 精确匹配
mux.handle("email:send", EmailHandler())

# 前缀匹配（所有以 "email:" 开头的任务类型）
mux.handle("email:", fallback_handler)

# catch-all 兜底（所有未匹配的类型）
mux.handle("", default_handler)

# 装饰器风格
@mux.task("report:daily")
async def handle_report(ctx, task):
    ...
```

匹配优先级：**精确匹配 > 最长前缀 > catch-all**

### Config 配置

```python
from asyrq import Config

Config(
    name="worker-1",                    # 显示名称
    code="w1",                          # 唯一编码
    concurrency=20,                     # 全局最大并发
    queues={"critical": 6, "default": 3},  # 队列权重
    strict_priority=False,              # True=高优队列清空后才处理低优
    shutdown_timeout=8,                 # 优雅关闭超时秒数
    retry_delay_func=custom_delay,      # 自定义重试延迟
    error_handler=log_error,            # 错误回调
    is_failure_func=is_fatal,           # 失败判定
)
```

### 按任务类型并发控制

```python
_email_sem = asyncio.Semaphore(2)    # 最多 2 并发
_report_sem = asyncio.Semaphore(10)  # 最多 10 并发

@app.task("email:send")
async def handle_email(ctx, task):
    async with _email_sem:
        await send_email(task)

@app.task("report:gen")
async def handle_report(ctx, task):
    async with _report_sem:
        await generate_report(task)
```

### 错误处理

```python
from asyrq import SkipRetry

@app.task("flaky:task")
async def handle_flaky(ctx, task):
    try:
        await risky_operation()
    except TransientError:
        raise                    # 自动重试
    except PermanentError:
        raise SkipRetry("不可恢复的错误")  # 不重试，直接归档

# 自定义重试延迟
def custom_delay(retry_count: int, error: Exception, task: Task) -> int:
    return min(60, 2 ** retry_count)  # 最多 60 秒

# 错误回调
async def on_error(ctx: Context, task: Task, error: Exception):
    await send_alert(f"任务失败: {task.typename} {error}")

app = Server(redis_opt, Config(error_handler=on_error))
```

## 中间件

洋葱模型，按注册顺序执行：

```python
from asyrq import ServeMux, Context, Task, Handler

def logging_middleware(next_handler: Handler) -> Handler:
    class _Wrapper(Handler):
        async def process_task(self, ctx: Context, task: Task):
            print(f"[开始] {task.typename} id={task.id()}")
            try:
                await next_handler.process_task(ctx, task)
                print(f"[完成] {task.typename}")
            except Exception as e:
                print(f"[失败] {task.typename}: {e}")
                raise
    return _Wrapper()

mux = ServeMux()
mux.use(logging_middleware, metrics_middleware)
# 执行顺序: logging(before) → metrics(before) → handler → metrics(after) → logging(after)
```

## 定时调度

```python
from asyrq import Scheduler, Task

scheduler = Scheduler(RedisClientOpt(addr="127.0.0.1:6379"))

# 注册定时任务
await scheduler.register("@every 5m", Task("health:check", b""))   # 每 5 分钟
await scheduler.register("*/10 * * * *", Task("sync:delta", b""))  # 每 10 分钟
await scheduler.register("0 9 * * 1-5", Task("report:daily", b""))  # 工作日 9:00
await scheduler.register("0 0 1 * *", Task("report:monthly", b""))  # 每月 1 号

# 启动调度器
await scheduler.run()   # 阻塞
# 或
await scheduler.start()  # 非阻塞
```

支持 `croniter` 所有 Cron 表达式格式。

## 任务组聚合

将一段时间内的多个任务合并为一组批量处理：

```python
async def aggregate_batch(gname: str, tasks: list[Task]) -> Task:
    """将同组任务聚合为一个批量任务。"""
    payloads = [json.loads(t.payload()) for t in tasks]
    return Task("batch:process", json.dumps(payloads).encode())

app = Server(
    redis_opt,
    Config(
        group_aggregator=aggregate_batch,
        group_max_delay=30,     # 最多等待 30 秒
        group_max_size=100,     # 最多 100 个任务为一组
        group_grace_period=5,   # 首个任务后等待 5 秒
    ),
)

# 生产者
await client.enqueue(Task("data:ingest", payload), Group("batch"))
await client.enqueue(Task("data:ingest", payload2), Group("batch"))
# → 自动聚合成一个 Task("batch:process", merged_payload)
```

## 结果与状态

### 两套 API（都立即写入 Redis）

| 方法 | Redis Key | TTL | 用途 |
|------|----------|-----|------|
| `task.update_state(dict)` | `{type}:state:{id}` | 1 小时 | 处理中实时上报状态 |
| `task.finish(dict)` | `{type}:result:{id}` | 永久 | 最终结果 |

```python
@app.task("email:send")
async def handle_email(ctx, task):
    # 处理中：多次上报状态
    await task.update_state({"step": "fetching", "pct": 30})
    await task.update_state({"step": "parsing", "pct": 80})

    # 完成：写入最终结果
    await task.finish({"status": "ok", "items": items})
```

查询：

```python
import redis.asyncio as redis
r = redis.Redis(...)

# 查实时状态
state = await r.get(f"email:send:state:{task_id}")

# 查最终结果（读后自行 DEL）
result = await r.get(f"email:send:result:{task_id}")
await r.delete(f"email:send:result:{task_id}")

# 或自定义 key
await client.enqueue(Task("email:send", payload), ResultKey("my:result"))
result = await r.get("my:result")  # 读 my:result:{task_id}
```

## 配置参考

### Server Config

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `name` | `str` | `""` | 消费者显示名称 |
| `code` | `str` | `""` | 消费者唯一编码（Server ID） |
| `concurrency` | `int` | CPU 核数 | 全局最大并发数 |
| `queues` | `dict[str,int]` | `{"default":1}` | 队列名→权重 |
| `strict_priority` | `bool` | `False` | 严格优先级模式 |
| `shutdown_timeout` | `int` | `8` | 优雅关闭超时秒数 |
| `log_level` | `LogLevel` | `INFO` | 日志级别 |
| `log_dir` | `str` | `"logs"` | 日志文件目录 |
| `retry_delay_func` | `Callable` | 指数退避 | 重试延迟计算 |
| `error_handler` | `Callable` | `None` | 错误回调 |
| `is_failure_func` | `Callable` | 全部算失败 | 失败判定 |
| `health_check_interval` | `int` | `15` | 健康检查间隔秒 |
| `delayed_task_check_interval` | `int` | `5` | 定时任务前移间隔秒 |
| `task_check_interval` | `int` | `1` | 空队列轮询间隔秒 |
| `janitor_interval` | `int` | `8` | 清理间隔秒 |
| `janitor_batch_size` | `int` | `100` | 每次清理最大数 |
| `group_max_delay` | `int` | `60` | 聚合最大延迟秒 |
| `group_grace_period` | `int` | `10` | 聚合宽限期秒 |
| `group_max_size` | `int` | `50` | 聚合最大任务数 |

### 重试延迟公式

默认指数退避：`seconds = n⁴ + 15 + rand(0~30) × (n + 1)`

| 重试次数 | 延迟约 |
|---------|--------|
| 0 | 15~30s |
| 1 | 16~77s |
| 2 | 31~120s |
| 3 | 96~210s |

## 架构

```
                         ┌─────────────────┐
                         │  asynqmon 面板   │
                         │ (兼容 Web 监控)  │
                         └────────┬────────┘
                                  │
┌──────────┐     ┌───────────────┴───────────────┐     ┌────────────────────────┐
│  Client  │────▶│             Redis              │◀────│        Server          │
│ (生产者) │     │  ┌─────┐ ┌────┐ ┌──────────┐  │     │                        │
└──────────┘     │  │List │ │ZSet│ │Hash/Set   │  │     │  后台协程（7 个）:      │
                 │  │pend │ │sch │ │task/hbeat │  │     │  ├─ processor          │
┌──────────┐     │  └─────┘ └────┘ └──────────┘  │     │  ├─ heartbeat          │
│Scheduler │────▶│      15 个 Lua 原子脚本        │     │  ├─ forwarder          │
│ (定时器) │     └───────────────────────────────┘     │  ├─ recoverer          │
└──────────┘                                           │  ├─ janitor            │
                                                       │  ├─ healthcheck        │
Redis 数据结构（任务 key 前缀: asyrq:tasks:{task_type}:{route}:{queue}）:  │  └─ aggregator  │
  asyrq:tasks:{type}:{route}:{queue}:pending    → List   (即时任务)  │                   │
  asyrq:tasks:{type}:{route}:{queue}:scheduled  → ZSet   (定时任务)  │  并发模型:         │
  asyrq:tasks:{type}:{route}:{queue}:retry      → ZSet   (重试任务)  │  Semaphore+asyncio │
  asyrq:tasks:{type}:{route}:{queue}:{id}       → Hash   (任务数据)  │  优雅退出→清理注册  │
  asyrq:tasks:{type}:{route}:{queue}:completed  → ZSet   (已完成)    └───────────────────┘
  asyrq:servers                                 → ZSet   (服务器心跳)
```

## 监控

> 注意：所有 Redis key 均使用 `asyrq:` 前缀（任务: `asyrq:tasks:...`，
> 服务器心跳/队列注册: `asyrq:servers` / `asyrq:queues` 等），
> 与 Go asynq 的 `asynq:` 键名不同，因此 [asynqmon](https://github.com/hibiken/asynqmon)
> 等按 `asynq:` 前缀扫描的监控工具不适用于本项目。

## 与 Go asynq 对照

| Go asynq | asyrq |
|----------|-------|
| `asynq.NewTask("t", payload)` | `Task("t", payload)` |
| `asynq.NewClient(opt)` | `Client(RedisClientOpt(...))` |
| `client.Enqueue(t, opts...)` | `await client.enqueue(t, *opts)` |
| `client.Close()` | `await client.close()` |
| `asynq.Queue("q")` | `Queue("q")` |
| `asynq.MaxRetry(n)` | `MaxRetry(n)` |
| `asynq.ProcessIn(d)` | `ProcessIn(seconds)` |
| `asynq.ProcessAt(t)` | `ProcessAt(nanosec)` |
| `asynq.Unique(ttl)` | `Unique(seconds)` |
| `mux.HandleFunc(p, f)` | `mux.handle_func(p, f)` 或 `@mux.task(p)` |
| `mux.Use(mws...)` | `mux.use(*mws)` |
| `srv := asynq.NewServer(opt, cfg)` | `app = Server(RedisClientOpt(...), Config(...))` |
| `srv.Run(mux)` | `await app.run(mux)` |
| `srv.Start(mux)` | `await app.start(mux)` |
| `srv.Shutdown()` | `await app.shutdown()` |
| `asynq.NewScheduler(opt, ...)` | `Scheduler(RedisClientOpt(...))` |

## License

MIT © asyrq contributors
