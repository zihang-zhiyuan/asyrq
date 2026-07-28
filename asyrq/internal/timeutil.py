# internal/timeutil.py — 时间工具模块
# 提供时间相关的辅助函数，1:1 对应 Go asynq 的 internal/timeutil 包
from __future__ import annotations
import time as _time


def now() -> int:
    """返回当前 UTC 时间的 Unix 纳秒时间戳。

    Go asynq 内部使用纳秒精度的时间戳，我们保持一致。

    Returns:
        int: 当前时间的纳秒级 Unix 时间戳
    """
    return int(_time.time() * 1_000_000_000)  # 秒转纳秒: 乘以 10^9


def now_seconds() -> int:
    """返回当前 UTC 时间的 Unix 秒级时间戳。

    Returns:
        int: 当前时间的秒级 Unix 时间戳
    """
    return int(_time.time())  # 返回整数秒


def nsec_to_sec(nsec: int) -> int:
    """将纳秒时间戳转换为秒级时间戳。

    Args:
        nsec: 纳秒级 Unix 时间戳

    Returns:
        int: 秒级 Unix 时间戳
    """
    return nsec // 1_000_000_000  # 纳秒除以 10^9 得到秒


def sec_to_nsec(sec: int) -> int:
    """将秒级时间戳转换为纳秒时间戳。

    Args:
        sec: 秒级 Unix 时间戳

    Returns:
        int: 纳秒级 Unix 时间戳
    """
    return sec * 1_000_000_000  # 秒乘以 10^9 得到纳秒


def deadline_to_timeout(deadline: int) -> int:
    """根据 deadline 时间戳计算剩余超时时间（秒）。

    Args:
        deadline: 截止时间的纳秒级 Unix 时间戳

    Returns:
        int: 剩余秒数，如果已过期则返回负数
    """
    remaining = deadline - now()  # 计算剩余纳秒
    return nsec_to_sec(remaining)  # 转换为秒返回
