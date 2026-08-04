# connection.py — Redis 连接配置模块
# 定义 Redis 连接选项，1:1 对应 Go asynq 的 RedisConnOpt 及三个实现类型

from __future__ import annotations
from dataclasses import dataclass, field

from urllib.parse import urlparse, parse_qs
from typing import List, Optional
import ssl as _ssl


@dataclass
class RedisClientOpt:
    """直接 Redis 连接配置。

    1:1 对应 Go asynq 的 RedisClientOpt 结构体。
    封装了连接 Redis 独立实例所需的所有参数。
    """
    # Redis 服务器地址，host:port 格式
    addr: str = "127.0.0.1:6379"
    # 网络类型: "tcp" 或 "unix"
    network: str = "tcp"
    # Redis 6+ ACL 用户名
    username: str = ""
    # Redis 认证密码
    password: str = ""
    # Redis 数据库编号 (0-15)
    db: int = 0
    # 连接超时时间（秒）
    dial_timeout: int = 5
    # 读超时时间（秒）
    read_timeout: int = 3
    # 写超时时间（秒）
    write_timeout: int = 3
    # 连接池大小
    pool_size: int = 10
    # 最小空闲连接数
    min_idle_conns: int = 0
    # TLS 配置（可选）
    tls_config: _ssl.SSLContext | None = None

    def make_redis_client(self):
        """根据当前配置创建一个 redis.asyncio.Redis 客户端实例。

        Returns:
            redis.asyncio.Redis: 异步 Redis 客户端
        """
        import redis.asyncio as _redis  # 使用异步 Redis 客户端
        # 解析 addr 获取主机和端口
        if ":" in self.addr:
            host, port = self.addr.rsplit(":", 1)  # 分离 host 和 port
            port = int(port)  # 端口转为整数
        else:
            host = self.addr  # 只有主机名
            port = 6379  # 默认 Redis 端口

        # 创建并返回异步 Redis 客户端
        kwargs = {
            "host": host, "port": port,
            "username": self.username or None,
            "password": self.password or None,
            "db": self.db,
            "socket_connect_timeout": self.dial_timeout,
            "socket_timeout": self.read_timeout,
            "socket_keepalive": True,
            "max_connections": self.pool_size,
            "retry_on_timeout": True,
            "decode_responses": False,
        }
        # 如果配置了 TLS，传递 SSL context
        if self.tls_config:
            kwargs["ssl"] = self.tls_config
        return _redis.Redis(**kwargs)

    def make_sync_redis_client(self):
        """创建同步 redis.Redis 客户端（用于 asyrq.sync）。"""
        import redis as _redis
        if ":" in self.addr:
            host, port = self.addr.rsplit(":", 1)
            port = int(port)
        else:
            host = self.addr
            port = 6379

        kwargs = {
            "host": host, "port": port,
            "username": self.username or None,
            "password": self.password or None,
            "db": self.db,
            "socket_connect_timeout": self.dial_timeout,
            "socket_timeout": self.read_timeout,
            "socket_keepalive": True,
            "max_connections": self.pool_size,
            "retry_on_timeout": True,
            "decode_responses": False,
        }
        if self.tls_config:
            kwargs["ssl"] = self.tls_config
        return _redis.Redis(**kwargs)


@dataclass
class RedisFailoverClientOpt:
    """Redis Sentinel 故障转移连接配置。

    1:1 对应 Go asynq 的 RedisFailoverClientOpt 结构体。
    通过 Redis Sentinel 提供自动故障转移能力。
    """
    # Sentinel 主节点名称
    master_name: str = ""
    # Sentinel 节点地址列表
    sentinel_addrs: List[str] = field(default_factory=list)
    # Sentinel ACL 用户名
    sentinel_username: str = ""
    # Sentinel ACL 密码
    sentinel_password: str = ""
    # Redis ACL 用户名
    username: str = ""
    # Redis 认证密码
    password: str = ""
    # Redis 数据库编号
    db: int = 0
    # 连接超时时间（秒）
    dial_timeout: int = 5
    # 读超时时间（秒）
    read_timeout: int = 3
    # 写超时时间（秒）
    write_timeout: int = 3
    # 连接池大小
    pool_size: int = 10
    # TLS 配置
    tls_config: Optional[_ssl.SSLContext] = None

    def make_redis_client(self):
        """根据 Sentinel 配置创建一个 redis.asyncio.Sentinel 客户端。

        Returns:
            redis.asyncio.Sentinel: 异步 Redis Sentinel 客户端
        """
        import redis.asyncio as _redis
        import redis.asyncio.sentinel as _sentinel

        # 创建 Sentinel 实例，连接到 Sentinel 节点
        sentinel = _sentinel.Sentinel(
            [(addr.rsplit(":", 1)[0], int(addr.rsplit(":", 1)[1]))
             for addr in self.sentinel_addrs],
            username=self.sentinel_username or None,
            password=self.sentinel_password or None,
            socket_connect_timeout=self.dial_timeout,
            socket_timeout=self.read_timeout,
        )
        # 通过 Sentinel 获取主节点的 Redis 客户端
        return sentinel.master_for(
            self.master_name,
            username=self.username or None,
            password=self.password or None,
            db=self.db,
            socket_connect_timeout=self.dial_timeout,
            socket_timeout=self.read_timeout,
            max_connections=self.pool_size,
            decode_responses=False,
        )


@dataclass
class RedisClusterClientOpt:
    """Redis Cluster 连接配置。

    1:1 对应 Go asynq 的 RedisClusterClientOpt 结构体。
    连接到 Redis Cluster，自动处理分片和重定向。
    """
    # 集群节点地址列表
    addrs: List[str] = field(default_factory=lambda: ["127.0.0.1:6379"])
    # 最大重定向次数
    max_redirects: int = 8
    # Redis ACL 用户名
    username: str = ""
    # Redis 认证密码
    password: str = ""
    # 连接超时时间（秒）
    dial_timeout: int = 5
    # 读超时时间（秒）
    read_timeout: int = 3
    # 写超时时间（秒）
    write_timeout: int = 3
    # TLS 配置
    tls_config: Optional[_ssl.SSLContext] = None

    def make_redis_client(self):
        """根据集群配置创建一个 redis.asyncio.RedisCluster 客户端。

        Returns:
            redis.asyncio.RedisCluster: 异步 Redis Cluster 客户端
        """
        import redis.asyncio as _redis

        # 创建 Redis Cluster 客户端
        return _redis.RedisCluster(
            host=self.addrs[0].rsplit(":", 1)[0] if self.addrs else "127.0.0.1",
            port=int(self.addrs[0].rsplit(":", 1)[1]) if self.addrs and ":" in self.addrs[0] else 6379,
            username=self.username or None,
            password=self.password or None,
            socket_connect_timeout=self.dial_timeout,
            socket_timeout=self.read_timeout,
            max_redirects=self.max_redirects,
            decode_responses=False,
        )


def parse_redis_uri(uri: str) -> RedisClientOpt:
    """从 Redis URI 解析连接配置。

    1:1 对应 Go asynq 的 ParseRedisURI 函数。
    支持以下格式:
        redis://[:password@]host:port[/db]
        rediss://[:password@]host:port[/db]  (TLS)
        redis-socket:///path/to/socket
        redis-sentinel://[:password@]host1:port1[,host2:port2]/master_name[/db]

    Args:
        uri: Redis 连接 URI

    Returns:
        RedisClientOpt: 解析后的连接配置

    Raises:
        ValueError: URI 格式不支持
    """
    parsed = urlparse(uri)  # 解析 URI
    opt = RedisClientOpt()  # 创建默认配置

    # 解析密码
    if parsed.password:
        opt.password = parsed.password  # 设置密码

    # 解析用户名（如果有）
    if parsed.username:
        opt.username = parsed.username  # 设置用户名

    # 根据 scheme 解析
    if parsed.scheme == "rediss":
        # rediss:// 表示使用 TLS 连接
        opt.tls_config = _ssl.create_default_context()
        opt.addr = f"{parsed.hostname}:{parsed.port or 6379}"
        # 解析数据库编号
        if parsed.path and parsed.path.lstrip("/"):
            opt.db = int(parsed.path.lstrip("/"))
    elif parsed.scheme in ("redis", ""):
        # redis:// 不指定 scheme 时默认
        opt.addr = f"{parsed.hostname}:{parsed.port or 6379}"
        if parsed.path and parsed.path.lstrip("/"):
            opt.db = int(parsed.path.lstrip("/"))
    elif parsed.scheme == "redis-socket":
        # redis-socket:// 使用 Unix socket
        opt.network = "unix"
        opt.addr = parsed.path  # socket 路径
    else:
        # 不支持的 scheme
        raise ValueError(f"不支持的 Redis URI scheme: {parsed.scheme}")

    # 解析查询参数
    query = parse_qs(parsed.query)
    if "dial_timeout" in query:
        opt.dial_timeout = int(query["dial_timeout"][0])  # 拨号超时
    if "read_timeout" in query:
        opt.read_timeout = int(query["read_timeout"][0])  # 读取超时
    if "write_timeout" in query:
        opt.write_timeout = int(query["write_timeout"][0])  # 写入超时
    if "pool_size" in query:
        opt.pool_size = int(query["pool_size"][0])  # 连接池大小

    return opt  # 返回解析后的配置
