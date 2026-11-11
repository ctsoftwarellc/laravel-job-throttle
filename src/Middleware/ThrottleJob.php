<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Middleware;

use Closure;
use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Ctsoftwarellc\JobThrottle\Contracts\RetryAfter;
use Ctsoftwarellc\JobThrottle\Exceptions\ThrottleWaitExceeded;
use Ctsoftwarellc\JobThrottle\Support\Decision;
use Ctsoftwarellc\JobThrottle\Support\PayloadRelease;
use Ctsoftwarellc\JobThrottle\Support\Rate;
use Ctsoftwarellc\JobThrottle\Support\ThrottleState;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\Jobs\RedisJob;
use Illuminate\Queue\Jobs\SyncJob;
use Illuminate\Support\Facades\Log;
use InvalidArgumentException;
use Throwable;

final class ThrottleJob
{
    private readonly ?Rate $rate;

    /**
     * @param  string|Closure(object): string  $key  the throttle key, or a closure that builds it from the job
     * @param  int|null  $allow  jobs per `every` seconds on the key; null applies only provider pauses and concurrency
     * @param  int|null  $concurrency  the most jobs on the key that may run at once
     * @param  int|null  $maxWait  the most seconds a job may wait in total; null uses config('job-throttle.max_wait')
     */
    public function __construct(
        private readonly string|Closure $key,
        ?int $allow = null,
        int $every = 1,
        private readonly ?int $concurrency = null,
        private readonly ?int $maxWait = null,
    ) {
        if ($concurrency !== null && $concurrency < 1) {
            throw new InvalidArgumentException("A throttle concurrency limit must be at least 1, [{$concurrency}] given.");
        }

        $this->rate = $allow === null ? null : new Rate($allow, $every);
    }

    /**
     * Apply only the provider pause: no rate and no concurrency limit.
     *
     * @param  string|Closure(object): string  $key
     */
    public static function pauseOnly(string|Closure $key): self
    {
        return new self($key);
    }

    public function handle(object $job, Closure $next): mixed
    {
        $queueJob = $this->queueJobOf($job);

        // Run inline: releasing a sync or unqueued job would drop it.
        if ($queueJob === null) {
            return $next($job);
        }

        $key = $this->keyFor($job);
        $state = ThrottleState::of($queueJob);
        $limiter = app(Limiter::class);

        try {
            [$decision, $leave] = $this->admit($limiter, $key, $queueJob, $state);
        } catch (Throwable $e) {
            $this->failOpen($e, $key);
            $decision = new Decision(0);
            $leave = null;
        }

        if (! $decision->runsNow()) {
            $this->wait($queueJob, $key, $decision, $state);

            return null;
        }

        try {
            return $next($job);
        } catch (RetryAfter $e) {
            $this->pause($limiter, $queueJob, $key, $e, $state);

            return null;
        } finally {
            $this->leave($leave);
        }
    }

    public function keyFor(object $job): string
    {
        return $this->key instanceof Closure ? ($this->key)($job) : $this->key;
    }

    private function queueJobOf(object $job): ?Job
    {
        $queueJob = property_exists($job, 'job') ? $job->job : null;

        return $queueJob instanceof Job && ! $queueJob instanceof SyncJob ? $queueJob : null;
    }

    /**
     * Take a concurrency slot, then a rate slot. A job that has to wait holds no concurrency slot.
     *
     * @return array{Decision, (Closure(): void)|null}
     */
    private function admit(Limiter $limiter, string $key, Job $queueJob, ThrottleState $state): array
    {
        $reservedSlot = $state->slotFor($key, $queueJob->attempts());
        $leave = null;

        if ($this->concurrency !== null) {
            $leave = $limiter->enter($key, $this->concurrency, ($queueJob->timeout() ?? 60) + 60);

            // ponytail: a full funnel polls every concurrency_retry seconds; a Lua wait list would end the polling.
            if ($leave === null) {
                return [$this->decisionFor(config()->integer('job-throttle.concurrency_retry') * 1000, $reservedSlot, $state), null];
            }
        }

        try {
            $decision = $limiter->acquire(
                $key,
                $this->rate,
                $reservedSlot,
                max(0, $this->maxWaitMs() - $state->waitedMs),
                reserve: $queueJob instanceof RedisJob,
            );
        } catch (Throwable $e) {
            $this->leave($leave);

            throw $e;
        }

        if ($decision->runsNow()) {
            return [$decision, $leave];
        }

        $this->leave($leave);

        return [$decision, null];
    }

    /**
     * A wait decided here rather than by the limiter, or -1 when it would pass maxWait.
     */
    private function decisionFor(int $waitMs, int $slotMs, ThrottleState $state): Decision
    {
        return new Decision($state->waitedMs + $waitMs > $this->maxWaitMs() ? -1 : $waitMs, $slotMs);
    }

    /**
     * Release the job for the wait without spending a try, or fail it once its waits pass maxWait.
     */
    private function wait(Job $queueJob, string $key, Decision $decision, ThrottleState $state): void
    {
        if ($decision->waitMs < 0) {
            $queueJob->fail(ThrottleWaitExceeded::forKey($key, $state->waitedMs, $this->maxWaitSeconds()));

            return;
        }

        $attempt = $queueJob->attempts();

        PayloadRelease::release($queueJob, $decision->waitMs, $state->after($key, $attempt, $decision->slotMs, $decision->waitMs));
    }

    /**
     * The provider answered 429: hold every job on the key, then release this one for as long.
     */
    private function pause(Limiter $limiter, Job $queueJob, string $key, RetryAfter $retryAfter, ThrottleState $state): void
    {
        $seconds = min(max($retryAfter->retryAfterSeconds(), 1), 86_400);

        try {
            $limiter->pause($key, $seconds);
        } catch (Throwable $e) {
            Log::warning('Job throttle could not pause the key, so only this job waits.', ['key' => $key, 'exception' => $e]);
        }

        $this->wait($queueJob, $key, $this->decisionFor($seconds * 1000, 0, $state), $state);
    }

    /**
     * Let the job run unthrottled when Redis fails, or rethrow when the throttle fails closed.
     */
    private function failOpen(Throwable $e, string $key): void
    {
        if (! config()->boolean('job-throttle.fail_open')) {
            throw $e;
        }

        Log::warning('Job throttle could not reach Redis, so the job runs unthrottled.', ['key' => $key, 'exception' => $e]);
    }

    /**
     * @param  (Closure(): void)|null  $leave
     */
    private function leave(?Closure $leave): void
    {
        if ($leave === null) {
            return;
        }

        try {
            $leave();
        } catch (Throwable $e) {
            Log::warning('Job throttle could not free a concurrency slot. The slot expires on its own.', ['exception' => $e]);
        }
    }

    private function maxWaitSeconds(): int
    {
        return $this->maxWait ?? config()->integer('job-throttle.max_wait');
    }

    private function maxWaitMs(): int
    {
        return $this->maxWaitSeconds() * 1000;
    }
}
