-- add_to_group_unique.lua — 原子地将任务唯一添加到聚合组
-- KEYS[1]: 任务数据 Hash key (asyrq:{queue}:t:{task_id})
-- KEYS[2]: 聚合组 ZSet key (asyrq:{queue}:aggregation:{gname})
-- KEYS[3]: 全局组集合 key (asyrq:{queue}:groups)
-- KEYS[4]: 唯一键 key (asyrq:{queue}:unique:{type}:{hash})
-- ARGV[1]: 任务消息 JSON
-- ARGV[2]: 任务 ID
-- ARGV[3]: 组名
-- ARGV[4]: 当前时间戳（纳秒）
-- ARGV[5]: 唯一键 TTL（秒）

-- 检查唯一键是否已存在
if redis.call("EXISTS", KEYS[4]) == 1 then
    return ""  -- 任务已存在（重复）
end

-- 设置唯一键
redis.call("SET", KEYS[4], ARGV[2], "EX", ARGV[5])

-- 写入任务数据到 Hash
redis.call("HSET", KEYS[1],
    "msg", ARGV[1],
    "state", "aggregating",
    "group_key", ARGV[3])

-- 添加到聚合组 ZSet
redis.call("ZADD", KEYS[2], ARGV[4], ARGV[2])

-- 将组名加入全局组集合
redis.call("SADD", KEYS[3], ARGV[3])

return ARGV[2]  -- 返回任务 ID
