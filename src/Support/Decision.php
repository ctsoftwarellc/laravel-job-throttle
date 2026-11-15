<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Support;

/**
 * A limiter's answer: run now, wait for a reserved slot, or give up.
 */
final readonly class Decision
{
    /**
     * @param  int  $waitMs  0 runs now, more is the wait in ms, -1 is over the wait budget
     * @param  int  $slotMs  the reserved slot as a Redis timestamp in ms, 0 when none
     */
    public function __construct(public int $waitMs, public int $slotMs = 0) {}

    public function runsNow(): bool
    {
        return $this->waitMs === 0;
    }
}
