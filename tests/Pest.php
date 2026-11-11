<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Tests\TestCase;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Support\Facades\Redis;

pest()->extend(TestCase::class)->in('Feature');

/**
 * Run the next ready job the way one queue worker loop does.
 */
function work(int $tries = 0): void
{
    app('queue.worker')->runNextJob('redis', 'default', new WorkerOptions(timeout: 0, sleep: 0, maxTries: $tries));
}

/**
 * Work every ready job once.
 */
function workAll(): void
{
    while (Redis::llen('queues:default') > 0) {
        work();
    }
}

/**
 * The released jobs: their decoded payloads and the second each becomes available.
 *
 * @return list<array{payload: array<string, mixed>, availableAt: int}>
 */
function delayedJobs(): array
{
    $jobs = [];

    foreach (Redis::zrange('queues:default:delayed', 0, -1, ['withscores' => true]) as $raw => $score) {
        $jobs[] = ['payload' => json_decode($raw, true), 'availableAt' => (int) $score];
    }

    return $jobs;
}

/**
 * Make every released job ready now, as if its delay had passed.
 */
function releaseDelayedJobs(): void
{
    foreach (array_keys(Redis::zrange('queues:default:delayed', 0, -1, ['withscores' => true])) as $raw) {
        Redis::zrem('queues:default:delayed', $raw);
        Redis::rpush('queues:default', $raw);
    }
}

/**
 * Milliseconds on the Redis clock, the clock the throttle uses.
 */
function redisNowMs(): int
{
    [$seconds, $micros] = Redis::time();

    return (int) $seconds * 1000 + intdiv((int) $micros, 1000);
}

/**
 * Point the throttle at a Redis that refuses connections. The queue keeps working.
 */
function throttleRedisIsDown(bool $failOpen = true): void
{
    config(['job-throttle.connection' => 'down', 'job-throttle.fail_open' => $failOpen]);
}

/**
 * Rewrite the payload of the job waiting at the head of the queue.
 *
 * @param  Closure(array<string, mixed>): array<string, mixed>  $rewrite
 */
function rewriteReadyJob(Closure $rewrite): void
{
    $raw = Redis::lpop('queues:default');

    Redis::lpush('queues:default', json_encode($rewrite(json_decode($raw, true))));
}
