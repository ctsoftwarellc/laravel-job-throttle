-- GCRA rate limit and provider pause for one throttle key, timed by Redis TIME.
--
-- KEYS[1]  hash of `tat`, the next free slot, and `starts`, recent admission times in ms
-- KEYS[2]  provider pause deadline, in ms (present only while paused)
--
-- ARGV[1] = 'pause':   ARGV[2] pause length in ms. The later deadline wins. Returns 0.
-- ARGV[1] = 'acquire': ARGV[2] emission interval in ms (0 = no rate, pause only),
--                      ARGV[3] slot this job already reserved (0 = none),
--                      ARGV[4] wait budget left in ms,
--                      ARGV[5] 1 to reserve the slot, 0 to only look (the job cannot keep a reservation),
--                      ARGV[6] maximum starts, ARGV[7] sliding window in ms.
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
local allow = tonumber(ARGV[6])
local window = tonumber(ARGV[7])

if emission == 0 then
    if floor - now > budget then
        return {-1, 0}
    end

    return {floor - now, 0}
end

-- Track actual starts, not clock-second buckets: a sliding window can straddle
-- any second boundary and late reservations can all return together.
local state = redis.call('HMGET', KEYS[1], 'tat', 'starts')
local tat = tonumber(state[1]) or 0
local starts = state[2] and cjson.decode(state[2]) or {}
local recent = {}
for _, started in ipairs(starts) do
    if started > now - window then
        table.insert(recent, started)
    end
end
local ready = #recent >= allow and recent[1] + window or now

local function save(next_tat, did_start)
    if did_start then
        table.insert(recent, now)
    end
    redis.call('HSET', KEYS[1], 'tat', next_tat, 'starts', cjson.encode(recent))
    local window_end = #recent > 0 and recent[#recent] + window or now
    redis.call('PEXPIRE', KEYS[1], math.max(1, next_tat - now, window_end - now))
end

if reserved > 0 and pause <= reserved then
    if reserved > now then
        return {reserved - now, reserved}
    end
    if ready <= now then
        save(tat, true)
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
save(slot + emission, slot <= now)
return {slot - now, slot}
