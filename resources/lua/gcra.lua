-- GCRA rate limit and provider pause for one throttle key, timed by Redis TIME.
--
-- KEYS[1]  hash of two times in ms: `tat`, the next free slot, and `start`, the next
--          start that keeps the rate, counted per Redis clock second (jobs can start late)
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
local floor = math.max(now, pause)

if emission == 0 then
    if floor - now > budget then
        return {-1, 0}
    end

    return {floor - now, 0}
end

-- Released jobs come back in whole seconds, so starts are counted per clock second
-- and the slots of one second may start together.
local second = now - now % 1000
local batch = (math.ceil(1000 / emission) - 1) * emission
local state = redis.call('HMGET', KEYS[1], 'tat', 'start')
local tat = tonumber(state[1]) or 0
local start = tonumber(state[2]) or 0

-- The earliest start that keeps the rate: now while this second has room, else the next second that has.
local ready = start - batch

if ready > second then
    ready = math.ceil(ready / 1000) * 1000
end

local function save(next_tat, next_start)
    redis.call('HSET', KEYS[1], 'tat', next_tat, 'start', next_start)
    redis.call('PEXPIRE', KEYS[1], math.max(next_tat, next_start) - now)
end

-- A reservation stands unless a pause now ends after it.
if reserved > 0 and pause <= reserved then
    if reserved > now then
        return {reserved - now, reserved}
    end

    -- Due or late. Jobs that come back late together (a worker outage) must not all start
    -- at once: start only if the rate allows it now, else take a fresh slot below.
    if ready <= now then
        save(tat, math.max(start, second) + emission)

        return {0, reserved}
    end
end

local slot = math.max(tat, floor, ready)

if slot - now > budget then
    return {-1, 0}
end

if slot > now and not reserve then
    return {slot - now, 0}
end

if slot > now then
    save(slot + emission, start)
else
    save(slot + emission, math.max(start, second) + emission)
end

return {slot - now, slot}
