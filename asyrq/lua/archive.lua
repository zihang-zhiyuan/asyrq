-- archive.lua — 任务归档脚本
-- 将任务从 active 列表移到 archived ZSet（重试次数耗尽）
-- KEYS[1]: active list key
-- KEYS[2]: lease hash key
-- KEYS[3]: archived ZSet key (asyrq:{queue}:archived)
-- KEYS[4]: 任务数据 Hash key
-- KEYS[5]: failed 计数器 key
-- ARGV[1]: 任务 ID
-- ARGV[2]: 错误消息
-- ARGV[3]: 已重试次数
-- ARGV[4]: 失败时间（纳秒时间戳）

-- 从 active 列表移除
local removed = redis.call("LREM", KEYS[1], 0, ARGV[1])
if removed == 0 then
    return redis.error_reply("NOT FOUND")
end

-- 从 lease hash 移除
redis.call("HDEL", KEYS[2], ARGV[1])

-- 更新任务数据状态为 archived
redis.call("HSET", KEYS[4],
    "state", "archived",
    "error_msg", ARGV[2],
    "retried", ARGV[3],
    "last_failed_at", ARGV[4])

-- 添加到 archived ZSet
redis.call("ZADD", KEYS[3], ARGV[4], ARGV[1])

-- 增加失败计数器
redis.call("INCR", KEYS[5])

return 1  -- 成功
