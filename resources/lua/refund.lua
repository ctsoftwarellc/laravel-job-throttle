-- Give a released job back the attempt and the deadline its throttle wait used.
--
-- KEYS[1]  the queue's delayed set
-- ARGV[1]  the copy Laravel just released into it
-- ARGV[2]  the refunded copy that replaces it at the same score
--
-- Returns 1 when replaced, 0 when a worker already took the released copy.

local score = redis.call('ZSCORE', KEYS[1], ARGV[1])

if not score then
    return 0
end

redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[1], score, ARGV[2])

return 1
