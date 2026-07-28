# tests/test_connection.py — 连接配置的单元测试

import pytest
from asyrq.connection import (
    RedisClientOpt,
    RedisFailoverClientOpt,
    RedisClusterClientOpt,
    parse_redis_uri,
)


class TestRedisClientOpt:
    """RedisClientOpt 的单元测试。"""

    def test_default_values(self):
        """测试默认值。"""
        opt = RedisClientOpt()
        assert opt.addr == "127.0.0.1:6379"
        assert opt.network == "tcp"
        assert opt.db == 0
        assert opt.dial_timeout == 5
        assert opt.pool_size == 10

    def test_custom_values(self):
        """测试自定义配置。"""
        opt = RedisClientOpt(
            addr="redis.example.com:6380",
            password="secret",
            db=3,
            pool_size=20,
        )
        assert opt.addr == "redis.example.com:6380"
        assert opt.password == "secret"
        assert opt.db == 3
        assert opt.pool_size == 20


class TestParseRedisURI:
    """parse_redis_uri 的单元测试。"""

    def test_parse_basic_redis_uri(self):
        """测试基本 redis:// URI。"""
        opt = parse_redis_uri("redis://localhost:6379")
        assert opt.addr == "localhost:6379"

    def test_parse_redis_uri_with_password(self):
        """测试带密码的 URI。"""
        opt = parse_redis_uri("redis://:mypassword@localhost:6379")
        assert opt.password == "mypassword"

    def test_parse_redis_uri_with_db(self):
        """测试带数据库编号的 URI。"""
        opt = parse_redis_uri("redis://localhost:6379/5")
        assert opt.db == 5

    def test_parse_redis_uri_with_password_and_db(self):
        """测试带密码和数据库的 URI。"""
        opt = parse_redis_uri("redis://:pwd@localhost:6379/3")
        assert opt.password == "pwd"
        assert opt.db == 3

    def test_parse_rediss_uri(self):
        """测试 rediss:// URI。"""
        opt = parse_redis_uri("rediss://localhost:6380")
        assert opt.addr == "localhost:6380"
        assert opt.tls_config is not None
