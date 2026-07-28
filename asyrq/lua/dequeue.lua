-- dequeue.lua — 原子出队脚本
-- 从多个队列的 pending 列表中取出任务并移到 active 状态
-- 支持加权优先级和严格优先级两种模式

-- KEYS 布局（每个队列一组 key）:
--   对于 N 个队列，KEYS 序列为:
--     KEYS[i*3+1]: pending list key
--     KEYS[i*3+2]: active list key
--     KEYS[i*3+3]: lease hash key
-- ARGV[1]: 当前纳秒时间戳（用于计算租约）
-- ARGV[2]: 租约秒数
-- ARGV[3]: 严格优先级标志（"1" = 严格, "0" = 加权）

local strict_priority = (ARGV[3] == "1")  -- 严格优先级模式
local queue_count = #KEYS / 3             -- 队列数量
local now = tonumber(ARGV[1])             -- 当前时间戳
local lease_secs = tonumber(ARGV[2])      -- 租约秒数
local lease_until = now + lease_secs * 1000000000  -- 租约到期时间

-- 遍历每个队列尝试出队
for i = 0, queue_count - 1 do
    local pending_key = KEYS[i * 3 + 1]   -- pending 列表 key
    local active_key = KEYS[i * 3 + 2]    -- active 列表 key
    local lease_key = KEYS[i * 3 + 3]     -- lease hash key

    -- 从 pending 列表的尾部（最旧的）取出一个任务 ID
    local task_id = redis.call("RPOP", pending_key)
    if task_id then
        -- 添加到 active 列表
        redis.call("LPUSH", active_key, task_id)
        -- 设置租约到期时间（Hash: task_id → lease_until）
        redis.call("HSET", lease_key, task_id, lease_until)
        return task_id  -- 返回出队的任务 ID
    end

    -- 严格优先级模式下，如果高优先级队列为空，检查下一个队列
    -- 加权优先级模式下，每轮只检查一个队列
    if strict_priority then
        -- 继续检查下一个优先级的队列
    else
        -- 加权模式下每轮只尝试一个队列
        break
    end
end

return nil  -- 所有队列都为空
