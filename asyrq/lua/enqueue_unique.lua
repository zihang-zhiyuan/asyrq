-- enqueue_unique.lua — 唯一任务入队脚本
-- 在入队前检查唯一锁，防止重复创建相同任务
-- KEYS[1]: 任务数据 Hash key
-- KEYS[2]: pending 列表 key
-- KEYS[3]: 唯一锁 key (asynq:{queue}:unique:{type}:{payload_hash})
-- ARGV[1]: 序列化的 TaskMessage JSON
-- ARGV[2]: 任务 ID
-- ARGV[3]: 入队时间（纳秒时间戳）
-- ARGV[4]: 唯一锁 TTL（秒）

-- 检查唯一锁是否已存在
if redis.call("EXISTS", KEYS[3]) == 1 then
    return 0  -- 唯一锁存在，任务不重复创建
end

-- 检查任务数据是否已存在
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0  -- 任务已存在
end

-- 设置唯一锁（带 TTL，到期自动释放）
redis.call("SET", KEYS[3], ARGV[2], "EX", ARGV[4])

-- 写入任务数据
redis.call("HSET", KEYS[1],
    "msg", ARGV[1],
    "state", "pending",
    "pending_since", ARGV[3],
    "unique_key", KEYS[3])  -- 记录唯一键，用于完成时释放

-- 添加到 pending 列表
redis.call("LPUSH", KEYS[2], ARGV[2])

return 1  -- 成功
