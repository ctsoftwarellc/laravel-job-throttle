<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Support;

use InvalidArgumentException;

/**
 * A rate of `allow` jobs per `every` seconds, spread evenly one slot apart.
 */
final readonly class Rate
{
    /** Milliseconds between two slots, rounded up so the rate is never exceeded. */
    public int $emissionMs;

    public function __construct(public int $allow, public int $every = 1)
    {
        if ($allow < 1 || $every < 1) {
            throw new InvalidArgumentException("A throttle rate needs at least 1 job per 1 second, [{$allow}] per [{$every}] given.");
        }

        $this->emissionMs = intdiv($every * 1000 + $allow - 1, $allow);
    }
}
