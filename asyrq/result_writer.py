# result_writer.py — 结果写入器模块
# 定义 ResultWriter，用于将任务处理结果写回 Redis，1:1 对应 Go asynq 的 ResultWriter




from __future__ import annotations
from __future__ import annotations
class ResultWriter:
    """任务结果写入器。

    1:1 对应 Go asynq 的 ResultWriter 结构体。
    在任务执行过程中，Handler 可以通过 Task.ResultWriter() 获取此对象，
    将处理结果写回 Redis 供后续查询。

    Usage:
        writer = task.result_writer()
        writer.write(json.dumps({"status": "ok"}).encode())
    """

    def __init__(self, task_id: str, broker: "Broker", qname: str):
        """初始化结果写入器。

        Args:
            task_id: 关联的任务 ID
            broker: Redis Broker 实例，用于写回操作
            qname: 队列名
        """
        self._task_id = task_id       # 任务唯一标识符
        self._broker = broker         # Redis Broker 引用
        self._qname = qname           # 队列名
        self._data: Optional[bytes] = None  # 写入的结果数据

    def task_id(self) -> str:
        """返回关联的任务 ID。1:1 对应 Go asynq 的 ResultWriter.TaskID() 方法。"""
        return self._task_id

    async def write(self, data: bytes) -> int:
        """写入结果数据。

        多次调用会覆盖之前的结果数据。
        实际数据会在任务完成时（Done/MarkAsComplete）写入 Redis。

        1:1 对应 Go asynq 的 ResultWriter.Write() 方法。

        Args:
            data: 要写入的结果数据（二进制格式）

        Returns:
            int: 写入的字节数
        """
        self._data = data  # 暂存数据，等待任务完成时写入 Redis
        return len(data)  # 返回写入的字节数

    def get_data(self) -> Optional[bytes]:
        """获取已写入的结果数据（内部使用）。"""
        return self._data
