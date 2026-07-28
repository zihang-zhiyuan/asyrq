# internal/log.py — 日志模块
# 提供日志接口和默认实现，1:1 对应 Go asynq 的 internal/log 包

from __future__ import annotations
import logging as _logging
from abc import ABC, abstractmethod
from enum import IntEnum


class LogLevel(IntEnum):
    """日志级别枚举，从低到高排列。"""
    DEBUG = _logging.DEBUG      # 调试信息
    INFO = _logging.INFO        # 普通信息
    WARN = _logging.WARNING     # 警告信息
    ERROR = _logging.ERROR      # 错误信息
    FATAL = _logging.CRITICAL   # 致命错误


class Logger(ABC):
    """日志器抽象基类，定义日志记录的标准接口。

    用户可以自定义实现此接口来集成自己的日志系统。
    """

    @abstractmethod
    def debug(self, *args, **kwargs) -> None:
        """记录 DEBUG 级别日志。"""
        ...

    @abstractmethod
    def info(self, *args, **kwargs) -> None:
        """记录 INFO 级别日志。"""
        ...

    @abstractmethod
    def warn(self, *args, **kwargs) -> None:
        """记录 WARN 级别日志。"""
        ...

    @abstractmethod
    def error(self, *args, **kwargs) -> None:
        """记录 ERROR 级别日志。"""
        ...

    @abstractmethod
    def fatal(self, *args, **kwargs) -> None:
        """记录 FATAL 级别日志。"""
        ...


class DefaultLogger(Logger):
    """基于 Python 标准库 logging 的默认日志器实现。

    在用户未提供自定义日志器时使用。
    """

    def __init__(self, level: LogLevel = LogLevel.INFO):
        """初始化默认日志器。

        Args:
            level: 日志输出级别，默认 INFO
        """
        self._logger = _logging.getLogger("asyrq")  # 使用专用 logger 名称
        self._logger.setLevel(int(level))  # 设置日志级别
        if not self._logger.handlers:  # 如果还没有处理器
            handler = _logging.StreamHandler()  # 创建控制台输出处理器
            handler.setFormatter(
                _logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(message)s",  # 日志格式: 时间 [级别] 消息
                    datefmt="%Y-%m-%d %H:%M:%S",  # 时间格式
                )
            )
            self._logger.addHandler(handler)  # 添加处理器到 logger

    def debug(self, *args, **kwargs) -> None:
        """记录 DEBUG 级别日志。"""
        self._logger.debug(*args, **kwargs)

    def info(self, *args, **kwargs) -> None:
        """记录 INFO 级别日志。"""
        self._logger.info(*args, **kwargs)

    def warn(self, *args, **kwargs) -> None:
        """记录 WARN 级别日志。"""
        self._logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs) -> None:
        """记录 ERROR 级别日志。"""
        self._logger.error(*args, **kwargs)

    def fatal(self, *args, **kwargs) -> None:
        """记录 FATAL 级别日志。"""
        self._logger.critical(*args, **kwargs)
