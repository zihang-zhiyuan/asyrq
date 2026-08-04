-- enqueue.lua — 基本任务入队脚本
-- 将任务消息写入 Redis Hash 并添加到 pending 列表
-- KEYS[1]: 任务数据 Hash key (asyrq:{queue}:t:{task_id})
-- KEYS[2]: pending 列表 key (asyrq:{queue}:pending)
-- ARGV[1]: 序列化的 TaskMessage JSON 字符串
-- ARGV[2]: 任务 ID
-- ARGV[3]: 入队时间（纳秒时间戳）

-- 检查任务是否已存在（幂等性保证）
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0  -- 任务已存在，不重复创建
end

-- 写入任务数据到 Hash
redis.call("HSET", KEYS[1],
    "msg", ARGV[1],          -- 任务消息体
    "state", "pending",       -- 任务状态
    "pending_since", ARGV[3]) -- 入队时间戳

-- 将任务 ID 推到 pending 列表头部
redis.call("LPUSH", KEYS[2], ARGV[2])

return 1  -- 成功
