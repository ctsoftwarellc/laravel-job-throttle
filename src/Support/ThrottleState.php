<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Support;

use Illuminate\Contracts\Queue\Job;

/**
 * What a job's throttle waits left in its payload: the attempt they belong to,
 * the total wait so far and the slot reserved on each throttle key.
 */
final readonly class ThrottleState
{
    /**
     * @param  array<array-key, int>  $slots  reserved slot in ms, by sha1 of the throttle key
     */
    public function __construct(
        public int $attempt = 0,
        public int $waitedMs = 0,
        public array $slots = [],
    ) {}

    public static function of(Job $job): self
    {
        $state = $job->payload()['jobThrottle'] ?? null;

        if (! is_array($state)) {
            return new self;
        }

        return new self(
            attempt: is_int($state['attempt'] ?? null) ? $state['attempt'] : 0,
            waitedMs: is_int($state['waited'] ?? null) ? $state['waited'] : 0,
            slots: is_array($state['slots'] ?? null) ? array_filter($state['slots'], is_int(...)) : [],
        );
    }

    /**
     * The slot this attempt reserved on the key. A new attempt (a real retry) has none.
     */
    public function slotFor(string $key, int $attempt): int
    {
        return $attempt === $this->attempt ? ($this->slots[sha1($key)] ?? 0) : 0;
    }

    public function after(string $key, int $attempt, int $slotMs, int $waitMs): self
    {
        $slots = $attempt === $this->attempt ? $this->slots : [];
        $slots[sha1($key)] = $slotMs;

        return new self($attempt, $this->waitedMs + $waitMs, $slots);
    }

    /**
     * @return array{attempt: int, waited: int, slots: array<array-key, int>}
     */
    public function toArray(): array
    {
        return ['attempt' => $this->attempt, 'waited' => $this->waitedMs, 'slots' => $this->slots];
    }
}
