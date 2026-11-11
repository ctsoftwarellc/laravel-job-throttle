<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Testing;

use Closure;
use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Ctsoftwarellc\JobThrottle\Support\Decision;
use Ctsoftwarellc\JobThrottle\Support\Rate;
use PHPUnit\Framework\Assert as PHPUnit;

/**
 * Records throttle decisions in memory. Every key is free until a test says otherwise.
 */
final class FakeLimiter implements Limiter
{
    /** @var array<string, int> seconds a job on the key has to wait */
    private array $waits = [];

    /** @var list<array{key: string, waitMs: int}> */
    private array $decisions = [];

    /** @var list<array{key: string, seconds: int}> */
    private array $pauses = [];

    /**
     * Use up the key's rate: a job on it waits the given seconds.
     */
    public function full(string $key, int $for): self
    {
        $this->waits[$key] = $for;

        return $this;
    }

    /**
     * Pause the key, as a provider 429 would: a job on it waits the given seconds.
     */
    public function paused(string $key, int $for): self
    {
        $this->waits[$key] = max($this->waits[$key] ?? 0, $for);

        return $this;
    }

    public function acquire(string $key, ?Rate $rate, int $reservedSlotMs, int $budgetMs, bool $reserve = true): Decision
    {
        $waitMs = ($this->waits[$key] ?? 0) * 1000;
        $decision = new Decision($waitMs > $budgetMs ? -1 : $waitMs);

        $this->decisions[] = ['key' => $key, 'waitMs' => $decision->waitMs];

        return $decision;
    }

    public function pause(string $key, int $seconds): void
    {
        $this->pauses[] = ['key' => $key, 'seconds' => $seconds];

        $this->paused($key, $seconds);
    }

    /**
     * @return Closure(): void
     */
    public function enter(string $key, int $limit, int $ttlSeconds): Closure
    {
        return fn () => null;
    }

    public function assertThrottled(?string $key = null): void
    {
        PHPUnit::assertNotEmpty(
            $this->throttled($key),
            $key === null ? 'No job was throttled.' : "No job was throttled on key [{$key}].",
        );
    }

    public function assertNotThrottled(?string $key = null): void
    {
        $keys = array_column($this->throttled($key), 'key');

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

    /**
     * @return list<array{key: string, waitMs: int}>
     */
    private function throttled(?string $key): array
    {
        return array_values(array_filter(
            $this->decisions,
            fn (array $decision) => $decision['waitMs'] !== 0 && ($key === null || $decision['key'] === $key),
        ));
    }
}
