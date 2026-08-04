-- aggregation_check.lua — 任务组聚合检查脚本
-- 检查聚合组是否满足触发条件，将任务从组中取出准备聚合
-- KEYS[1]: 聚合组 ZSet key (asyrq:{queue}:g:{group_name})
-- ARGV[1]: 当前时间（纳秒时间戳）
-- ARGV[2]: 聚合组名
-- ARGV[3]: 最大延迟（秒）
-- ARGV[4]: 最大任务数
-- ARGV[5]: 宽限期（秒）

local current_time = tonumber(ARGV[1])  -- 当前时间
local gname = ARGV[2]                   -- 组名
local max_delay = tonumber(ARGV[3])     -- 最大延迟
local max_size = tonumber(ARGV[4])      -- 最大任务数
local grace_period = tonumber(ARGV[5])  -- 宽限期

-- 获取组中所有任务（按入队时间排序）
local tasks = redis.call("ZRANGE", KEYS[1], 0, -1, "WITHSCORES")
local task_count = #tasks / 2  -- 任务数量（每对是 id + score）

-- 空组不需要处理
if task_count == 0 then
    return nil
end

-- 获取最早入队的任务时间
local oldest_score = tonumber(tasks[2])  -- 第一个任务的 score（入队时间）

-- 检查触发条件:
-- 1. 任务数量达到最大值
-- 2. 最早任务已超过最大延迟
local should_flush = false
if max_size > 0 and task_count >= max_size then
    should_flush = true        -- 数量触发
end
if max_delay > 0 and (current_time - oldest_score) > max_delay * 1000000000 then
    should_flush = true        -- 时间触发
end

if should_flush then
    -- 从组中删除所有任务
    redis.call("DEL", KEYS[1])
    -- 收集所有任务 ID
    local task_ids = {}
    for i = 1, #tasks, 2 do
        table.insert(task_ids, tasks[i])
    end
    return task_ids  -- 返回任务 ID 列表
end

return nil  -- 不需要触发
