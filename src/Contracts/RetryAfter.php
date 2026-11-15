<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Contracts;

use Throwable;

/**
 * Thrown by a job when the provider refused the call with a rate limit (HTTP 429).
 *
 * The job must throw it before any side effect. The throttle pauses the job's key
 * for the given seconds (clamped to 1 second through 24 hours) and releases the job.
 */
interface RetryAfter extends Throwable
{
    public function retryAfterSeconds(): int;
}
