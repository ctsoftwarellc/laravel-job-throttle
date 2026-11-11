-- GCRA rate limit and provider pause for one throttle key, timed by Redis TIME.
--
-- KEYS[1]  theoretical arrival time of the next free slot, in ms
-- KEYS[2]  provider pause deadline, in ms (present only while paused)
--
-- ARGV[1] = 'pause':   ARGV[2] pause length in ms. The later deadline wins. Returns 0.
-- ARGV[1] = 'acquire': ARGV[2] emission interval in ms (0 = no rate, pause only),
--                      ARGV[3] slot this job already reserved (0 = none),
--                      ARGV[4] wait budget left in ms,
--                      ARGV[5] 1 to reserve the slot, 0 to only look (the job cannot keep a reservation).
--                      Returns {wait ms, slot ms}: wait 0 runs now, -1 is over budget, slot 0 is not reserved.

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local pause = tonumber(redis.call('GET', KEYS[2]) or 0)

if ARGV[1] == 'pause' then
    local deadline = now + tonumber(ARGV[2])

    if deadline > pause then
        redis.call('SET', KEYS[2], deadline, 'PX', deadline - now)
    end

    return 0
end

local emission = tonumber(ARGV[2])
local reserved = tonumber(ARGV[3])
local budget = tonumber(ARGV[4])
local reserve = ARGV[5] == '1'

-- A reservation stands unless a pause now ends after it.
if reserved > 0 and pause <= reserved then
    return {math.max(0, reserved - now), reserved}
end

local floor = math.max(now, pause)

if emission == 0 then
    if floor - now > budget then
        return {-1, 0}
    end

    return {floor - now, 0}
end

local slot = math.max(tonumber(redis.call('GET', KEYS[1]) or 0), floor)

if slot - now > budget then
    return {-1, 0}
end

if slot > now and not reserve then
    return {slot - now, 0}
end

redis.call('SET', KEYS[1], slot + emission, 'PX', slot + emission - now)

return {slot - now, slot}
