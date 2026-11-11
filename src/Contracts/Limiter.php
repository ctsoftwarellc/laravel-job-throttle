<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Contracts;

use Closure;
use Ctsoftwarellc\JobThrottle\Support\Decision;
use Ctsoftwarellc\JobThrottle\Support\Rate;

interface Limiter
{
    /**
     * Reserve the next slot on the key, or confirm the slot the job already holds.
     *
     * A null rate applies only the provider pause.
     */
    public function acquire(string $key, ?Rate $rate, int $reservedSlotMs, int $budgetMs): Decision;

    /**
     * Hold every job on the key for the given seconds. A shorter pause never cuts a longer one.
     */
    public function pause(string $key, int $seconds): void;

    /**
     * Take one of the key's concurrency slots.
     *
     * @return (Closure(): void)|null frees the slot, or null when every slot is taken
     */
    public function enter(string $key, int $limit, int $ttlSeconds): ?Closure;
}
