-- clear_server_state.lua — 清除服务器状态脚本
-- 服务器关闭时清除其注册信息
-- KEYS[1]: servers ZSet key
-- KEYS[2]: workers ZSet key
-- ARGV[1]: 服务器 ID

-- 从 servers ZSet 移除
redis.call("ZREM", KEYS[1], ARGV[1])

-- 删除 workers 相关 key
redis.call("DEL", KEYS[2])

-- 删除服务器状态 Hash
local server_key = "asynq:servers:{" .. ARGV[1] .. "}"
redis.call("DEL", server_key)

return 1  -- 成功
