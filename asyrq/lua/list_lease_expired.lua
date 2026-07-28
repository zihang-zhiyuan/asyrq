-- list_lease_expired.lua — 列出过期租约脚本
-- 查找所有租约到期的 active 任务（孤儿任务检测）
-- KEYS[1]: lease hash key (asynq:{queue}:lease)
-- ARGV[1]: 当前时间（纳秒时间戳）
-- ARGV[2]: 最大返回数量

local current_time = tonumber(ARGV[1])  -- 当前时间
local max_count = tonumber(ARGV[2])       -- 最大返回数
local expired = {}                        -- 过期任务列表
local count = 0                           -- 已收集计数

-- 遍历 lease hash 中所有条目
local all_leases = redis.call("HGETALL", KEYS[1])
for i = 1, #all_leases, 2 do
    local task_id = all_leases[i]         -- 任务 ID
    local lease_until = tonumber(all_leases[i + 1])  -- 租约到期时间

    -- 检查租约是否已过期
    if lease_until < current_time then
        table.insert(expired, task_id)  -- 收集过期任务 ID
        count = count + 1
        if count >= max_count then
            break  -- 达到最大数量，停止收集
        end
    end
end

return expired  -- 返回过期任务 ID 列表
