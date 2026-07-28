-- schedule.lua — 定时任务调度脚本
-- 将任务写入 scheduled ZSet，等待未来某个时间点执行
-- KEYS[1]: 任务数据 Hash key
-- KEYS[2]: scheduled ZSet key (asynq:{queue}:scheduled)
-- ARGV[1]: 序列化的 TaskMessage JSON
-- ARGV[2]: 任务 ID
-- ARGV[3]: 计划执行时间（纳秒时间戳，作为 ZSet score）

-- 检查任务是否已存在
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0  -- 任务已存在
end

-- 写入任务数据，状态为 scheduled
redis.call("HSET", KEYS[1],
    "msg", ARGV[1],
    "state", "scheduled")

-- 添加到 scheduled ZSet（score 为计划执行时间）
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])

return 1  -- 成功
