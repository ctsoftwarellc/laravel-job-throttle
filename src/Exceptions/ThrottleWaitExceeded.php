<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Exceptions;

use RuntimeException;

final class ThrottleWaitExceeded extends RuntimeException
{
    public static function forKey(string $key, int $waitedMs, int $maxWaitSeconds): self
    {
        $waited = intdiv($waitedMs, 1000);

        return new self("The job would wait past its {$maxWaitSeconds} second limit on throttle key [{$key}]. It has already waited {$waited} seconds.");
    }
}
