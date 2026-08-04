-- dequeue.lua — 原子出队脚本
-- 从单个队列的 pending 列表中取出任务并移到 active 状态
-- KEYS[1]: pending list key (asyrq:{queue}:pending)
-- KEYS[2]: active list key  (asyrq:{queue}:active)
-- KEYS[3]: lease hash key   (asyrq:{queue}:lease)
-- KEYS[4]: paused key       (asyrq:{queue}:paused)
-- ARGV[1]: 当前纳秒时间戳（用于计算租约）
-- ARGV[2]: 租约秒数
-- ARGV[3]: 严格优先级标志（兼容保留，"1"/"0"）

-- 队列已暂停时不消费
if redis.call("EXISTS", KEYS[4]) == 1 then
    return ""
end

local now = tonumber(ARGV[1])
local lease_secs = tonumber(ARGV[2])
local lease_until = now + lease_secs * 1000000000

-- 从 pending 列表的尾部（最旧的）取出一个任务 ID
local task_id = redis.call("RPOP", KEYS[1])
if task_id then
    -- 添加到 active 列表
    redis.call("LPUSH", KEYS[2], task_id)
    -- 设置租约到期时间（Hash: task_id → lease_until）
    redis.call("HSET", KEYS[3], task_id, lease_until)
    return task_id
end

return nil  -- 队列为空
