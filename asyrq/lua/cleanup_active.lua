-- cleanup_active.lua — 清理"有 active/lease 条目但任务数据已丢失"的孤儿任务
-- 出队后发现任务 Hash 不存在时调用，避免孤儿条目在 Redis 中永久残留。
-- KEYS[1]: active list key (asyrq:tasks:{type}:{route}:{queue}:active)
-- KEYS[2]: lease hash key  (asyrq:tasks:{type}:{route}:{queue}:lease)
-- ARGV[1]: task_id

redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("HDEL", KEYS[2], ARGV[1])

return 1
