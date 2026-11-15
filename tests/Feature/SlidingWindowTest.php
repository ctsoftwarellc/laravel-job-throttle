<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Redis;

it('keeps actual starts within every sliding window after an outage', function () {
    $lua = file_get_contents(__DIR__.'/../../resources/lua/gcra.lua');
    $lua = str_replace("local time = redis.call('TIME')\nlocal now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)", 'local now = tonumber(ARGV[8])', $lua);
    $redis = Redis::connection();
    $now = 1_000_900;
    $starts = [];
    for ($i = 0; $i < 32; $i++) {
        $result = $redis->eval($lua, 2, 'property:rate', 'property:pause', 'acquire', 125, $now - 5000 - $i, 3600000, 1, 8, 1000, $now);
        if ($result[0] === 0) {
            $starts[] = $now;
        }
    }
    expect(count($starts))->toBeLessThanOrEqual(8);
});

it('keeps sub-millisecond admissions inside every real one-second window', function () {
    $lua = file_get_contents(__DIR__.'/../../resources/lua/gcra.lua');
    $lua = str_replace("local time = redis.call('TIME')\nlocal now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)", 'local now = math.floor(tonumber(ARGV[8]) / 1000)', $lua);
    $redis = Redis::connection();

    foreach ([1, 500, 999] as $offset) {
        $key = "edge:{$offset}";
        $starts = [];
        $first = 1_000_000_000 + $offset;
        foreach (array_fill(0, 8, $first) as $time) {
            [$wait] = $redis->eval($lua, 2, $key, "{$key}:pause", 'acquire', 125, 1, 10000, 1, 8, 1000, $time);
            expect($wait)->toBe(0);
            $starts[] = $time;
        }
        $edge = 1_001_000_000;
        [$wait] = $redis->eval($lua, 2, $key, "{$key}:pause", 'acquire', 125, 1, 10000, 1, 8, 1000, $edge);
        if ($wait === 0) {
            $starts[] = $edge;
        }
        expect(count(array_filter($starts, fn (int $start) => $edge - $start < 1_000_000)))->toBeLessThanOrEqual(8)
            ->and($wait)->toBe(1);
        [$wait] = $redis->eval($lua, 2, $key, "{$key}:pause", 'acquire', 125, 1, 10000, 1, 8, 1000, $edge + 1000);
        expect($wait)->toBe(0);
    }
});

it('keeps every sliding window within the rate and eventually runs each job', function (int $allow, int $every, string $pattern, int $seed) {
    $source = file_get_contents(__DIR__.'/../../resources/lua/gcra.lua');
    $source = str_replace("local time = redis.call('TIME')\nlocal now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)", 'local now = tonumber(ARGV[8])', $source);
    $redis = Redis::connection();
    $emission = (int) ceil($every * 1000 / $allow);
    $window = $every * 1000;
    $base = 1_000_000 + $seed * 137;
    $events = [];
    for ($i = 0; $i < 36; $i++) {
        $arrival = match ($pattern) {
            'burst' => $base,
            'steady' => $base + $i * $emission,
            'outage' => $base,
            'interleaved' => $base + $i * 113,
        };
        $events[] = ['at' => $arrival, 'slot' => 0, 'id' => $i];
    }
    $starts = [];
    $expires = null;
    $iterations = 0;
    while ($events !== [] && $iterations++ < 5000) {
        usort($events, fn ($a, $b) => [$a['at'], $a['id']] <=> [$b['at'], $b['id']]);
        $event = array_shift($events);
        $now = $event['at'];
        if ($expires !== null && $now >= $expires) {
            $redis->del('property:rate');
            $expires = null;
        }
        [$wait, $slot] = $redis->eval($source, 2, 'property:rate', 'property:pause', 'acquire', $emission, $event['slot'], 100_000_000, 1, $allow, $window, $now);
        $ttl = $redis->pttl('property:rate');
        if ($ttl > 0) {
            $expires = $now + $ttl;
        }
        expect($wait)->toBeGreaterThanOrEqual(0);
        if ($wait === 0) {
            $starts[] = $now;
            $inWindow = array_filter($starts, fn (int $start) => $start > $now - $window);
            expect(count($inWindow))->toBeLessThanOrEqual($allow);
        } else {
            $return = (int) (ceil(($now + $wait) / 1000) * 1000) + ($seed * 97 % 900);
            if ($pattern === 'outage') {
                $return = max($return, $base + 100_000 + $seed * 97);
            }
            $events[] = ['at' => $return, 'slot' => $slot, 'id' => $event['id']];
        }
    }
    expect($events)->toBeEmpty()->and($starts)->toHaveCount(36);
})->with(function () {
    foreach ([[1, 1], [3, 1], [8, 1], [7, 2], [10, 5], [1, 3]] as [$allow, $every]) {
        foreach (['burst', 'steady', 'outage', 'interleaved'] as $pattern) {
            foreach ([1, 2, 3, 4] as $seed) {
                yield "{$allow}/{$every} {$pattern} seed {$seed}" => [$allow, $every, $pattern, $seed];
            }
        }
    }
});

it('finishes a burst close to its allotted rate without excess releases', function () {
    $source = file_get_contents(__DIR__.'/../../resources/lua/gcra.lua');
    $source = str_replace("local time = redis.call('TIME')\nlocal now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)", 'local now = tonumber(ARGV[8])', $source);
    $redis = Redis::connection();
    $base = 1_000_000;
    $events = [];
    for ($i = 0; $i < 200; $i++) {
        $events[] = ['at' => $base, 'slot' => 0, 'id' => $i];
    }
    $starts = [];
    $releases = 0;
    $iterations = 0;
    while ($events !== [] && $iterations++ < 2000) {
        usort($events, fn ($a, $b) => [$a['at'], $a['id']] <=> [$b['at'], $b['id']]);
        $event = array_shift($events);
        $now = $event['at'];
        [$wait, $slot] = $redis->eval($source, 2, 'throughput:rate', 'throughput:pause', 'acquire', 125, $event['slot'], 100_000_000, 1, 8, 1000, $now);
        if ($wait === 0) {
            $starts[] = $now;
            expect(count(array_filter($starts, fn (int $start) => $start > $now - 1000)))->toBeLessThanOrEqual(8);
        } else {
            $return = $event['slot'] > 0 && $slot === $event['slot'] && $wait < 1000
                ? $now + $wait
                : (int) (ceil(($now + $wait) / 1000) * 1000) + 5 + ($event['id'] * 37 % 100);
            if ($return !== $now + $wait) {
                $releases++;
            }
            $events[] = ['at' => $return, 'slot' => $slot, 'id' => $event['id']];
        }
    }
    expect($starts)->toHaveCount(200)
        ->and(max($starts) - $base)->toBeLessThanOrEqual(25_125)
        ->and($releases)->toBeLessThanOrEqual(400);
});

it('keeps a due reservation without moving the tail and rejects a wait over budget', function () {
    $source = file_get_contents(__DIR__.'/../../resources/lua/gcra.lua');
    $source = str_replace("local time = redis.call('TIME')\nlocal now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)", 'local now = tonumber(ARGV[8])', $source);
    $redis = Redis::connection();
    $redis->hmset('budget:rate', ['tat' => 1_100_000, 'starts' => '[1000000]']);

    expect($redis->eval($source, 2, 'budget:rate', 'budget:pause', 'acquire', 1000, 999_000, 1000, 1, 1, 1000, 1_000_000))->toBe([-1, 0])
        ->and($redis->eval($source, 2, 'budget:rate', 'budget:pause', 'acquire', 1000, 999_000, 1001, 1, 1, 1000, 1_000_000))->toBe([1001, 999_000])
        ->and((int) $redis->hget('budget:rate', 'tat'))->toBe(1_100_000);
});
