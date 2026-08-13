-- requeue_delayed.lua — 把当前执行中的任务原封不动地放回延迟队列
-- 与内置 retry 的区别：进入 scheduled ZSet（延迟队列），不修改 retried/failed 计数
-- KEYS[1]: active list key (asyrq:tasks:{type}:{route}:{queue}:active)
-- KEYS[2]: lease hash key  (asyrq:tasks:{type}:{route}:{queue}:lease)
-- KEYS[3]: scheduled ZSet key (asyrq:tasks:{type}:{route}:{queue}:scheduled)
-- KEYS[4]: 任务数据 Hash key (asyrq:tasks:{type}:{route}:{queue}:{task_id})
-- ARGV[1]: task_id
-- ARGV[2]: 计划执行时间（纳秒时间戳，作为 ZSet score）

local removed = redis.call("LREM", KEYS[1], 0, ARGV[1])
if removed == 0 then
    return 0  -- 任务不在 active，可能已被其他流程处理，避免重复入队
end

redis.call("HDEL", KEYS[2], ARGV[1])
redis.call("HSET", KEYS[4], "state", "scheduled")
redis.call("ZADD", KEYS[3], ARGV[2], ARGV[1])

return 1
