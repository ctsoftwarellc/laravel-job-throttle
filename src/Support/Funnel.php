<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Support;

use Illuminate\Redis\Limiters\ConcurrencyLimiter;

/**
 * Laravel's concurrency limiter with the slot taken and freed by the caller.
 *
 * ConcurrencyLimiter::block() frees the slot right after the job's callback, so a Redis
 * error there would throw after the job's side effects. The middleware frees it instead.
 */
final class Funnel extends ConcurrencyLimiter
{
    public function take(string $holder): ?string
    {
        $slot = $this->acquire($holder);

        return is_string($slot) ? $slot : null;
    }

    public function free(string $slot, string $holder): void
    {
        $this->release($slot, $holder);
    }
}
