-- done.lua — 任务完成处理脚本
-- 将任务从 active 列表移除，根据配置写入 completed ZSet 或直接删除
-- KEYS[1]: active list key (asynq:{queue}:active)
-- KEYS[2]: lease hash key (asynq:{queue}:lease)
-- KEYS[3]: 任务数据 Hash key (asynq:{queue}:t:{task_id})
-- KEYS[4]: completed ZSet key (asynq:{queue}:completed)
-- KEYS[5]: processed 计数器 key (asynq:{queue}:processed)
-- ARGV[1]: 任务 ID
-- ARGV[2]: 完成时间（纳秒时间戳）
-- ARGV[3]: 保留时间（秒，0 = 不保留）
-- ARGV[4]: 结果数据（JSON 字符串，可选）

-- 从 active 列表移除任务
local removed = redis.call("LREM", KEYS[1], 0, ARGV[1])
if removed == 0 then
    return redis.error_reply("NOT FOUND")  -- 任务不在 active 列表中
end

-- 从 lease hash 移除租约
redis.call("HDEL", KEYS[2], ARGV[1])

-- 更新任务数据状态
redis.call("HSET", KEYS[3],
    "state", "completed",
    "completed_at", ARGV[2])

-- 如果有结果数据，存储到任务 Hash
if ARGV[4] ~= "" then
    redis.call("HSET", KEYS[3], "result", ARGV[4])
end

-- 增加已完成计数器
redis.call("INCR", KEYS[5])

-- 根据保留时间决定是否保留完成记录
local retention = tonumber(ARGV[3])
if retention > 0 then
    -- 计算过期时间并写入 completed ZSet
    local expire_at = tonumber(ARGV[2]) + retention * 1000000000
    redis.call("ZADD", KEYS[4], expire_at, ARGV[1])
else
    -- 不保留，直接删除任务数据
    redis.call("DEL", KEYS[3])
end

return 1  -- 成功
