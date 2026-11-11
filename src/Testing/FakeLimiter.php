<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Testing;

use Closure;
use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Ctsoftwarellc\JobThrottle\Support\Decision;
use Ctsoftwarellc\JobThrottle\Support\Rate;
use Ctsoftwarellc\JobThrottle\Support\ThrottleState;
use PHPUnit\Framework\Assert as PHPUnit;

/**
 * Records throttle decisions in memory. Every key is free until a test says otherwise.
 */
final class FakeLimiter implements Limiter
{
    /** @var array<string, int> seconds a job on the key has to wait */
    private array $waits = [];

    /** @var array<string, true> keys whose concurrency slots are all taken */
    private array $full = [];

    /** @var list<string> the key of every wait, in order */
    private array $throttled = [];

    /** @var list<array{key: string, seconds: int}> */
    private array $pauses = [];

    /** @var array<string, ThrottleState> by job uuid */
    private array $states = [];

    /**
     * Use up the key's rate: a job on it waits the given seconds for its slot.
     */
    public function rateLimited(string $key, int $for): self
    {
        return $this->waitOn($key, $for);
    }

    /**
     * Pause the key, as a provider 429 would: a job on it waits the given seconds.
     */
    public function paused(string $key, int $for): self
    {
        return $this->waitOn($key, $for);
    }

    /**
     * Take every concurrency slot on the key: a job on it waits concurrency_retry seconds.
     */
    public function concurrencyFull(string $key): self
    {
        $this->full[$key] = true;

        return $this;
    }

    public function acquire(string $key, ?Rate $rate, int $reservedSlotMs, int $budgetMs, bool $reserve = true): Decision
    {
        $waitMs = ($this->waits[$key] ?? 0) * 1000;

        if ($waitMs > 0) {
            $this->throttled[] = $key;
        }

        return new Decision($waitMs > $budgetMs ? -1 : $waitMs);
    }

    public function pause(string $key, int $seconds): void
    {
        $this->pauses[] = ['key' => $key, 'seconds' => $seconds];

        $this->paused($key, $seconds);
    }

    public function enter(string $key, int $limit, int $ttlSeconds): ?Closure
    {
        if (isset($this->full[$key])) {
            $this->throttled[] = $key;

            return null;
        }

        return static function (): void {};
    }

    public function state(string $jobUuid): ThrottleState
    {
        return $this->states[$jobUuid] ?? new ThrottleState;
    }

    public function remember(string $jobUuid, ThrottleState $state, int $ttlSeconds): void
    {
        $this->states[$jobUuid] = $state;
    }

    public function assertThrottled(?string $key = null): void
    {
        PHPUnit::assertNotEmpty(
            $this->throttledOn($key),
            $key === null ? 'No job was throttled.' : "No job was throttled on key [{$key}].",
        );
    }

    public function assertNotThrottled(?string $key = null): void
    {
        $keys = $this->throttledOn($key);

        PHPUnit::assertEmpty($keys, 'Jobs were throttled on keys ['.implode(', ', array_unique($keys)).'].');
    }

    public function assertPaused(string $key, ?int $seconds = null): void
    {
        $reported = array_column(array_filter($this->pauses, fn (array $pause) => $pause['key'] === $key), 'seconds');

        PHPUnit::assertNotEmpty($reported, "The key [{$key}] was not paused.");

        if ($seconds !== null) {
            PHPUnit::assertContains($seconds, $reported, "The key [{$key}] was not paused for {$seconds} seconds. It was paused for [".implode(', ', $reported).'] seconds.');
        }
    }

    public function assertNotPaused(?string $key = null): void
    {
        $keys = array_column(array_filter($this->pauses, fn (array $pause) => $key === null || $pause['key'] === $key), 'key');

        PHPUnit::assertEmpty($keys, 'The keys ['.implode(', ', array_unique($keys)).'] were paused.');
    }

    private function waitOn(string $key, int $seconds): self
    {
        $this->waits[$key] = max($this->waits[$key] ?? 0, $seconds);

        return $this;
    }

    /**
     * @return list<string>
     */
    private function throttledOn(?string $key): array
    {
        return array_values(array_filter($this->throttled, fn (string $throttled) => $key === null || $throttled === $key));
    }
}
