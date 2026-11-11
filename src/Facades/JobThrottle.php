<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Facades;

use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleMail;
use Ctsoftwarellc\JobThrottle\Testing\FakeLimiter;
use Illuminate\Support\Facades\Facade;

/**
 * The testing entry point: swap the limiter for a fake and assert on its decisions.
 *
 * @method static void assertThrottled(string|null $key = null)
 * @method static void assertNotThrottled(string|null $key = null)
 * @method static void assertPaused(string $key, int|null $seconds = null)
 * @method static void assertNotPaused(string|null $key = null)
 *
 * @see FakeLimiter
 */
final class JobThrottle extends Facade
{
    /**
     * Replace the Redis limiter with an in-memory fake that records every decision.
     */
    public static function fake(): FakeLimiter
    {
        $fake = new FakeLimiter;

        self::swap($fake);

        return $fake;
    }

    /**
     * The throttle keys the job's middleware uses, resolved the way the queue runs them.
     *
     * @return list<string>
     */
    public static function keysFor(object $job): array
    {
        $middleware = [
            ...(method_exists($job, 'middleware') ? (array) $job->middleware() : []),
            ...(property_exists($job, 'middleware') ? (array) $job->middleware : []),
        ];

        $keys = [];

        foreach ($middleware as $throttle) {
            if ($throttle instanceof ThrottleMail) {
                $throttle = $throttle->throttleFor($job);
            }

            if ($throttle instanceof ThrottleJob) {
                $keys[] = $throttle->keyFor($job);
            }
        }

        return $keys;
    }

    protected static function getFacadeAccessor(): string
    {
        return Limiter::class;
    }
}
