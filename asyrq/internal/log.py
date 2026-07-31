# internal/log.py — 日志模块
# 支持控制台输出、按大小/时间滚动文件、第三方 logger 注入
from __future__ import annotations

import logging as _logging
import os as _os
from abc import ABC, abstractmethod
from enum import IntEnum
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler


class LogLevel(IntEnum):
    """日志级别枚举。"""
    DEBUG = _logging.DEBUG
    INFO = _logging.INFO
    WARN = _logging.WARNING
    ERROR = _logging.ERROR
    FATAL = _logging.CRITICAL


class Logger(ABC):
    """日志器抽象基类。

    只需实现 5 个方法即可接入任何第三方 logger:
        class LoguruLogger(Logger):
            def __init__(self): from loguru import logger as l; self._l = l
            def info(self, msg, *a, **kw): self._l.info(msg, *a, **kw)
            def debug(self, msg, *a, **kw): self._l.debug(msg, *a, **kw)
            def warn(self, msg, *a, **kw): self._l.warning(msg, *a, **kw)
            def error(self, msg, *a, **kw): self._l.error(msg, *a, **kw)
            def fatal(self, msg, *a, **kw): self._l.critical(msg, *a, **kw)
    """

    @abstractmethod
    def debug(self, msg: str, *args, **kwargs) -> None: ...

    @abstractmethod
    def info(self, msg: str, *args, **kwargs) -> None: ...

    @abstractmethod
    def warn(self, msg: str, *args, **kwargs) -> None: ...

    @abstractmethod
    def error(self, msg: str, *args, **kwargs) -> None: ...

    @abstractmethod
    def fatal(self, msg: str, *args, **kwargs) -> None: ...


class DefaultLogger(Logger):
    """默认日志器：控制台 + 按大小滚动文件 + 按天滚动文件。

    参数:
        level:  日志级别
        log_dir: 日志目录，None=只控制台
        name:   logger 名称（server / client）
        max_bytes: 单文件最大字节
        backup_count: 按大小滚动保留文件数
        max_days: 按天滚动保留天数
    """

    _FMT = "%(asctime)s [%(levelname)-5s] %(name)-12s | %(message)s"
    _DATE_FMT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        level: LogLevel = LogLevel.INFO,
        log_dir: str = "logs",
        name: str = "asyrq",
        max_bytes: int = 10 * 1024 * 1024,
        backup_count: int = 5,
        max_days: int = 30,
    ):
        self._logger = _logging.getLogger(name)
        self._logger.setLevel(int(level))
        self._logger.handlers.clear()
        self._logger.propagate = False

        formatter = _logging.Formatter(self._FMT, datefmt=self._DATE_FMT)

        # 控制台
        console = _logging.StreamHandler()
        console.setFormatter(formatter)
        console.setLevel(int(level))
        self._logger.addHandler(console)

        if log_dir:
            _os.makedirs(log_dir, exist_ok=True)

            # 按大小滚动：全量日志
            size_handler = RotatingFileHandler(
                _os.path.join(log_dir, f"{name}.log"),
                maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8",
            )
            size_handler.setLevel(_logging.DEBUG)
            size_handler.setFormatter(formatter)
            self._logger.addHandler(size_handler)

            # 按大小滚动：错误日志
            err_handler = RotatingFileHandler(
                _os.path.join(log_dir, f"{name}-error.log"),
                maxBytes=max(1, max_bytes // 2), backupCount=backup_count, encoding="utf-8",
            )
            err_handler.setLevel(_logging.ERROR)
            err_handler.setFormatter(formatter)
            self._logger.addHandler(err_handler)

            # 按天滚动：全量日志（保留 N 天）
            day_handler = TimedRotatingFileHandler(
                _os.path.join(log_dir, f"{name}-daily.log"),
                when="midnight", interval=1, backupCount=max_days, encoding="utf-8",
            )
            day_handler.setLevel(_logging.DEBUG)
            day_handler.setFormatter(formatter)
            self._logger.addHandler(day_handler)

    def debug(self, msg: str, *args, **kwargs) -> None:
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        self._logger.info(msg, *args, **kwargs)

    def warn(self, msg: str, *args, **kwargs) -> None:
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self._logger.error(msg, *args, **kwargs)

    def fatal(self, msg: str, *args, **kwargs) -> None:
        self._logger.critical(msg, *args, **kwargs)


class TaskLogger:
    """任务级日志 — 自动附加 [task:{id}] 前缀。"""

    def __init__(self, logger: Logger, task_id: str):
        self._logger = logger
        self._prefix = f"[task:{task_id[:12]}] "

    def debug(self, msg: str, *args, **kwargs) -> None:
        self._logger.debug(self._prefix + msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        self._logger.info(self._prefix + msg, *args, **kwargs)

    def warn(self, msg: str, *args, **kwargs) -> None:
        self._logger.warn(self._prefix + msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self._logger.error(self._prefix + msg, *args, **kwargs)

    def fatal(self, msg: str, *args, **kwargs) -> None:
        self._logger.fatal(self._prefix + msg, *args, **kwargs)
