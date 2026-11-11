<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Support;

use Closure;
use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Illuminate\Contracts\Redis\Factory as Redis;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Str;

/**
 * GCRA slots, provider pauses and concurrency slots in Redis, shared by every worker.
 */
final class RedisLimiter implements Limiter
{
    public function __construct(private readonly Redis $redis) {}

    public function acquire(string $key, ?Rate $rate, int $reservedSlotMs, int $budgetMs, bool $reserve = true): Decision
    {
        /** @var array{int, int} $result */
        $result = $this->evaluate($key, ['acquire', $rate->emissionMs ?? 0, $reservedSlotMs, $budgetMs, (int) $reserve]);

        return new Decision(waitMs: $result[0], slotMs: $result[1]);
    }

    public function pause(string $key, int $seconds): void
    {
        $this->evaluate($key, ['pause', $seconds * 1000]);
    }

    public function enter(string $key, int $limit, int $ttlSeconds): ?Closure
    {
        $funnel = new Funnel($this->connection(), $this->name($key).':funnel:', $limit, $ttlSeconds);
        $holder = Str::random(20);
        $slot = $funnel->take($holder);

        return $slot === null ? null : fn () => $funnel->free($slot, $holder);
    }

    /**
     * @param  list<string|int>  $arguments
     */
    private function evaluate(string $key, array $arguments): mixed
    {
        $name = $this->name($key);

        return Lua::run($this->connection(), 'gcra', ["{$name}:tat", "{$name}:pause"], $arguments);
    }

    private function connection(): Connection
    {
        $name = config('job-throttle.connection');

        return $this->redis->connection(is_string($name) ? $name : null);
    }

    /**
     * Both keys of one throttle key share a hash tag, so they live in one Redis Cluster slot.
     */
    private function name(string $key): string
    {
        return '{'.$this->prefix().':job-throttle:'.$key.'}';
    }

    private function prefix(): string
    {
        foreach (['job-throttle.key_prefix', 'cache.prefix', 'app.name'] as $option) {
            $prefix = config($option);

            if (is_string($prefix) && $prefix !== '') {
                return $prefix;
            }
        }

        return 'laravel';
    }
}
