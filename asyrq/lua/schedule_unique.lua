-- schedule_unique.lua — 唯一定时任务调度脚本
-- KEYS[1]: 任务数据 Hash key
-- KEYS[2]: scheduled ZSet key
-- KEYS[3]: 唯一锁 key
-- ARGV[1]: 序列化的 TaskMessage JSON
-- ARGV[2]: 任务 ID
-- ARGV[3]: 计划执行时间（纳秒时间戳）
-- ARGV[4]: 唯一锁 TTL（秒）

-- 检查唯一锁
if redis.call("EXISTS", KEYS[3]) == 1 then
    return 0  -- 唯一锁存在
end

-- 检查任务数据
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end

-- 设置唯一锁
redis.call("SET", KEYS[3], ARGV[2], "EX", ARGV[4])

-- 写入任务数据
redis.call("HSET", KEYS[1],
    "msg", ARGV[1],
    "state", "scheduled",
    "unique_key", KEYS[3])

-- 添加到 scheduled ZSet
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])

return 1  -- 成功
