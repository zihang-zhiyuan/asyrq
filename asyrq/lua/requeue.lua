-- requeue.lua — 任务重新入队脚本
-- 将 active 任务重新放回 pending 列表（用于优雅关闭时）
-- KEYS[1]: active list key
-- KEYS[2]: pending list key
-- KEYS[3]: lease hash key
-- KEYS[4]: 任务数据 Hash key
-- ARGV[1]: 任务 ID

-- 从 active 列表移除
local removed = redis.call("LREM", KEYS[1], 0, ARGV[1])
if removed == 0 then
    return redis.error_reply("NOT FOUND")
end

-- 从 lease hash 移除
redis.call("HDEL", KEYS[3], ARGV[1])

-- 重新添加到 pending 列表
redis.call("LPUSH", KEYS[2], ARGV[1])

-- 更新状态为 pending
redis.call("HSET", KEYS[4], "state", "pending")

return 1  -- 成功
