# asyrq — Python Redis 分布式任务队列

Python 1:1 复刻 Go 语言 [asynq](https://github.com/hibiken/asynq) 库的 Redis 分布式任务队列。

## 特性

- **保证至少一次执行** — 任务不会丢失
- **任务重试** — 指数退避重试策略
- **故障恢复** — 孤儿任务自动恢复
- **优先级队列** — 多队列权重分配
- **定时任务** — 支持进程内延迟和定时执行
- **周期性任务** — Cron 表达式调度
- **唯一任务** — TTL 内去重
- **任务分组** — 批量聚合处理
- **中间件** — 洋葱模型中间件链
- **Web UI** — 兼容 asynqmon 监控面板
- **Redis Cluster / Sentinel** — 高可用支持

## 安装

```bash
pip install asyrq
```

## 快速开始

### 创建任务

```python
from asyrq import Task, Client, RedisClientOpt

task = Task("email:send", b'{"to": "user@example.com", "subject": "Hello"}')
```

### 入队任务

```python
from asyrq import Client, RedisClientOpt, Queue, MaxRetry

client = Client(RedisClientOpt(addr="localhost:6379"))

# 立即执行
await client.enqueue(task)

# 使用选项
await client.enqueue(
    task,
    Queue("critical"),     # 指定队列
    MaxRetry(3),            # 最大重试 3 次
)
```

### 处理任务

```python
from asyrq import Server, ServeMux, Config, Handler, Context

async def handle_email(ctx: Context, task: Task):
    data = json.loads(task.payload())
    print(f"发送邮件到: {data['to']}")
    # 返回 None 表示成功
    # 抛出 SkipRetry 表示不重试

mux = ServeMux()
mux.handle_func("email:send", handle_email)

config = Config(
    concurrency=10,
    queues={"critical": 6, "default": 3, "low": 1},
)

server = Server(RedisClientOpt(addr="localhost:6379"), config)
await server.run(mux)  # 阻塞运行
```

### 延迟和定时任务

```python
from asyrq import ProcessIn, ProcessAt

# 30 秒后执行
await client.enqueue(task, ProcessIn(30))

# 指定时间执行
await client.enqueue(task, ProcessAt(specific_timestamp))
```

### 周期性任务

```python
from asyrq import Scheduler

scheduler = Scheduler(RedisClientOpt(addr="localhost:6379"))

# 每分钟执行
await scheduler.register("@every 1m", task)

# Cron 表达式
await scheduler.register("0 9 * * *", task)  # 每天 9:00

await scheduler.run()
```

### 中间件

```python
def my_middleware(next_handler):
    class WrappedHandler(Handler):
        async def process_task(self, ctx, task):
            print(f"开始: {task.type()}")
            result = await next_handler.process_task(ctx, task)
            print(f"完成: {task.type()}")
            return result
    return WrappedHandler()

mux.use(my_middleware)
```

## API 对应表

| Go asynq | asyrq |
|----------|----------|
| `asynq.NewTask("type", payload)` | `Task("type", payload)` |
| `client.Enqueue(task, opts...)` | `await client.enqueue(task, *opts)` |
| `asynq.Queue("name")` | `Queue("name")` |
| `asynq.MaxRetry(n)` | `MaxRetry(n)` |
| `asynq.ProcessIn(d)` | `ProcessIn(seconds)` |
| `asynq.ProcessAt(t)` | `ProcessAt(nsec_timestamp)` |
| `asynq.Unique(ttl)` | `Unique(seconds)` |
| `mux.HandleFunc(p, h)` | `mux.handle_func(p, h)` |
| `mux.Use(mws...)` | `mux.use(*mws)` |
| `server.Run(mux)` | `await server.run(mux)` |
| `scheduler.Register(spec, task)` | `await scheduler.register(spec, task)` |

## 兼容性

- Redis 版本: 4.0+ (需要 Lua 脚本支持)
- Python 版本: 3.9+
- 完全兼容 asynqmon Web 监控面板

## 许可

MIT
