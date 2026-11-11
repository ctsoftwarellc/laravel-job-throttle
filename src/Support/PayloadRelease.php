<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Support;

use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\Jobs\RedisJob;
use Illuminate\Support\Facades\Date;
use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * Releases a throttled job so that its wait spends neither a try nor time from retryUntil.
 */
final class PayloadRelease
{
    /**
     * Release the job until its wait has passed, then rewrite the released copy on a
     * Redis queue: attempts back to before this pop, retryUntil moved by the delay and
     * the throttle state saved. Other queues keep Laravel's plain release.
     */
    public static function release(Job $job, int $waitMs, ThrottleState $state): void
    {
        $delay = self::secondsUntil($waitMs);

        $job->release($delay);

        if (! $job instanceof RedisJob) {
            return;
        }

        try {
            self::refund($job, $delay, $state);
        } catch (Throwable $e) {
            Log::warning('Job throttle could not refund the released job, so this wait spends one try.', [
                'job' => $job->resolveName(),
                'exception' => $e,
            ]);
        }
    }

    /**
     * Whole seconds of delay that keep the job out of the queue until the wait has passed.
     */
    private static function secondsUntil(int $waitMs): int
    {
        $nowMs = Date::now()->getTimestampMs();

        return intdiv($nowMs + $waitMs + 999, 1000) - intdiv($nowMs, 1000);
    }

    private static function refund(RedisJob $job, int $delay, ThrottleState $state): void
    {
        $payload = $job->payload();

        if (is_int($payload['retryUntil'] ?? null)) {
            $payload['retryUntil'] += $delay;
        }

        $payload['jobThrottle'] = $state->toArray();

        $queue = $job->getRedisQueue();

        Lua::run($queue->getConnection(), 'refund', [$queue->getQueue($job->getQueue()).':delayed'], [
            $job->getReservedJob(),
            json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
        ]);
    }
}
