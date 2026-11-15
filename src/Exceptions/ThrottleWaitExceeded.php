<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Exceptions;

use RuntimeException;

/**
 * The job failed because its throttle waits would pass its maxWait.
 *
 * The middleware fails the job with it instead of throwing, so the job's tries are not spent on it.
 */
final class ThrottleWaitExceeded extends RuntimeException
{
    /**
     * @param  int  $waitedMs  the time the job has already waited, in ms
     */
    public static function forKey(string $key, int $waitedMs, int $maxWaitSeconds): self
    {
        $waited = intdiv($waitedMs, 1000);

        return new self("The job would wait past its {$maxWaitSeconds} second limit on throttle key [{$key}]. It has already waited {$waited} seconds.");
    }
}
