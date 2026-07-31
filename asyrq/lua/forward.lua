-- forward.lua — 定时任务前移脚本
-- 将到期的 scheduled 和 retry 任务前移到 pending 列表
-- KEYS[1]: scheduled ZSet key
-- KEYS[2]: retry ZSet key
-- KEYS[3]: pending list key
-- KEYS[4]: 队列名（用于构建任务数据 key）
-- ARGV[1]: 当前时间（纳秒时间戳，用于 ZRANGEBYSCORE 上限）
-- ARGV[2]: 每个 ZSet 每次前移的最大数量

local current_time = tonumber(ARGV[1])  -- 当前时间
local batch_size = tonumber(ARGV[2])    -- 批量大小
local moved_count = 0                    -- 已前移计数

-- 前移 scheduled 中的到期任务
local scheduled_ids = redis.call(
    "ZRANGEBYSCORE", KEYS[1], "-inf", current_time,
    "LIMIT", 0, batch_size - moved_count
)
for _, task_id in ipairs(scheduled_ids) do
    -- 从 scheduled 移除
    redis.call("ZREM", KEYS[1], task_id)
    -- 添加到 pending
    redis.call("LPUSH", KEYS[3], task_id)
    -- 更新任务数据状态
    local task_key = "asynq:" .. KEYS[4] .. ":t:" .. task_id
    redis.call("HSET", task_key, "state", "pending")
    moved_count = moved_count + 1
end

-- 前移 retry 中的到期任务
if moved_count < batch_size then
    local retry_ids = redis.call(
        "ZRANGEBYSCORE", KEYS[2], "-inf", current_time,
        "LIMIT", 0, batch_size - moved_count
    )
    for _, task_id in ipairs(retry_ids) do
        -- 从 retry 移除
        redis.call("ZREM", KEYS[2], task_id)
        -- 添加到 pending
        redis.call("LPUSH", KEYS[3], task_id)
        -- 更新任务数据状态
        local task_key = "asynq:" .. KEYS[4] .. ":t:" .. task_id
        redis.call("HSET", task_key, "state", "pending")
        moved_count = moved_count + 1
    end
end

return moved_count  -- 返回前移的任务总数
