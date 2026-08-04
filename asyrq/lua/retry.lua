-- retry.lua — 任务重试脚本
-- 将失败的任务从 active 移到 retry ZSet 中等待重试
-- KEYS[1]: active list key
-- KEYS[2]: lease hash key
-- KEYS[3]: retry ZSet key (asyrq:{queue}:retry)
-- KEYS[4]: 任务数据 Hash key
-- KEYS[5]: failed 计数器 key (asyrq:{queue}:failed)
-- ARGV[1]: 任务 ID
-- ARGV[2]: 下次重试时间（纳秒时间戳，作为 ZSet score）
-- ARGV[3]: 错误消息
-- ARGV[4]: 已重试次数
-- ARGV[5]: 失败时间（纳秒时间戳）

-- 从 active 列表移除
local removed = redis.call("LREM", KEYS[1], 0, ARGV[1])
if removed == 0 then
    return redis.error_reply("NOT FOUND")
end

-- 从 lease hash 移除
redis.call("HDEL", KEYS[2], ARGV[1])

-- 更新任务数据状态为重试
redis.call("HSET", KEYS[4],
    "state", "retry",
    "error_msg", ARGV[3],
    "retried", ARGV[4],
    "last_failed_at", ARGV[5])

-- 添加到 retry ZSet（按下次重试时间排序）
redis.call("ZADD", KEYS[3], ARGV[2], ARGV[1])

-- 增加失败计数器
redis.call("INCR", KEYS[5])

return 1  -- 成功
