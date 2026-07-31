-- write_server_state.lua — 写入服务器状态脚本
-- 将服务器信息和活跃 worker 信息写入 Redis（心跳）
-- KEYS[1]: servers ZSet key (asynq:servers)
-- KEYS[2]: workers ZSet key (asynq:workers:{host:pid:sid})
-- ARGV[1]: 服务器 ID
-- ARGV[2]: 服务器信息 JSON 字符串
-- ARGV[3]: 当前时间（纳秒时间戳）
-- ARGV[4]: TTL（秒）

-- 写入（或更新）服务器心跳到 ZSet
redis.call("ZADD", KEYS[1], ARGV[3], ARGV[1])

-- 清理 workers ZSet 中旧的条目
redis.call("DEL", KEYS[2])

-- 设置服务器状态 Hash
local server_key = "asynq:servers:" .. ARGV[1]
redis.call("SET", server_key, ARGV[2], "EX", ARGV[4])

return 1  -- 成功
