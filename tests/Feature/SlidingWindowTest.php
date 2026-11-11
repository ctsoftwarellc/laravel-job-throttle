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
