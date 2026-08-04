-- delete_expired_completed.lua — 删除过期已完成任务脚本
-- 定期清理已完成但已过保留期的任务数据
-- KEYS[1]: completed ZSet key (asyrq:{queue}:completed)
-- ARGV[1]: 当前时间（纳秒时间戳）
-- ARGV[2]: 批量删除最大数量

local current_time = tonumber(ARGV[1])  -- 当前时间
local batch_size = tonumber(ARGV[2])    -- 批量大小

-- 查找过期的已完成任务（score < 当前时间的）
local expired_ids = redis.call(
    "ZRANGEBYSCORE", KEYS[1], "-inf", current_time,
    "LIMIT", 0, batch_size
)

-- 逐个删除过期任务的数据
for _, task_id in ipairs(expired_ids) do
    redis.call("ZREM", KEYS[1], task_id)  -- 从 completed ZSet 移除
end

return #expired_ids  -- 返回删除的任务数量
