<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Ctsoftwarellc\JobThrottle\Support\Decision;
use Ctsoftwarellc\JobThrottle\Support\Funnel;
use Ctsoftwarellc\JobThrottle\Support\PayloadRelease;
use Ctsoftwarellc\JobThrottle\Support\Rate;
use Ctsoftwarellc\JobThrottle\Support\RedisLimiter;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob;
use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Redis\Limiters\ConcurrencyLimiter;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;

mutates(RedisLimiter::class, ThrottleJob::class, PayloadRelease::class);

/**
 * Take every concurrency slot on the key, as jobs running on other workers would.
 */
function fillConcurrencySlots(int $limit, string $key = 'provider'): void
{
    foreach (range(1, $limit) as $slot) {
        app(Limiter::class)->enter($key, $limit, 60);
    }
}

function hasFreeConcurrencySlot(int $limit, string $key = 'provider'): bool
{
    $leave = app(Limiter::class)->enter($key, $limit, 60);
    $leave?->__invoke();

    return $leave !== null;
}

it('runs no more than K jobs on a key at once', function () {
    fillConcurrencySlots(2);
    ProviderJob::dispatch('third', 'provider', null, concurrency: 2);

    work();

    [$released] = delayedJobs();

    expect(ProviderJob::$runs)->toBeEmpty()
        ->and($released['availableAt'] - time())->toBeBetween(2, 3)
        ->and($released['payload']['attempts'])->toBe(0)
        ->and($released['payload']['jobThrottle']['waited'])->toBe(2000);
});

it('lets a job wait exactly its maxWait', function () {
    fillConcurrencySlots(1);
    ProviderJob::dispatch('waiting', 'provider', null, concurrency: 1, maxWait: 2);

    work();

    expect(delayedJobs())->toHaveCount(1)
        ->and($this->failures)->toBeEmpty();
});

it('holds a slot while the job runs and frees it afterwards', function () {
    ProviderJob::dispatch('running', 'provider', null, concurrency: 1);
    ProviderJob::$during = function () use (&$freeWhileRunning) {
        $freeWhileRunning = hasFreeConcurrencySlot(1);
    };

    work();

    expect(ProviderJob::$runs)->toBe(['running'])
        ->and($freeWhileRunning)->toBeFalse()
        ->and(hasFreeConcurrencySlot(1))->toBeTrue();
});

it('frees the slot when the job throws', function () {
    ProviderJob::dispatch('failing', 'provider', null, concurrency: 1);
    ProviderJob::$failures = [new RuntimeException('Provider is down')];

    work();

    expect(hasFreeConcurrencySlot(1))->toBeTrue();
});

it('frees the slot while the job waits for its rate slot', function () {
    ProviderJob::dispatch('first', 'provider', 1, 10, concurrency: 1);
    ProviderJob::dispatch('waiting', 'provider', 1, 10, concurrency: 1);

    workAll();

    expect(ProviderJob::$runs)->toBe(['first'])
        ->and(hasFreeConcurrencySlot(1))->toBeTrue();
});

it('keeps an unfreed slot past any worker timeout before it expires', function (?int $timeout, ?int $lockFor, int $locked) {
    $lockedFor = null;
    app()->instance(Limiter::class, new class($lockedFor) implements Limiter
    {
        public function __construct(private ?int &$lockedFor) {}

        public function acquire(string $key, ?Rate $rate, int $reservedSlotMs, int $budgetMs, bool $reserve = true): Decision
        {
            return new Decision(0);
        }

        public function pause(string $key, int $seconds): void {}

        public function enter(string $key, int $limit, int $ttlSeconds): ?Closure
        {
            $this->lockedFor = $ttlSeconds;

            return fn () => null;
        }
    });
    $job = new ProviderJob('running', 'provider', null, concurrency: 1, lockFor: $lockFor);
    $job->timeout = $timeout;
    dispatch($job);

    work();

    expect($lockedFor)->toBe($locked);
})->with([
    'no timeout, so the worker --timeout applies' => [null, null, 660],
    'no time limit at all' => [0, null, 660],
    'a short timeout' => [30, null, 660],
    'a timeout past the default' => [900, null, 960],
    'an explicit lockFor' => [900, 120, 120],
]);

it('stores a concurrency slot that expires after the configured lock', function () {
    config(['job-throttle.concurrency_lock_for' => 3600]);
    ProviderJob::dispatch('running', 'provider', null, concurrency: 1);
    ProviderJob::$during = function () use (&$slotTtl) {
        $slotTtl = Redis::ttl('{tests:job-throttle:provider}:funnel:1');
    };

    work();

    expect($slotTtl)->toBe(3600);
});

it('rejects a lock shorter than a second', function () {
    new ThrottleJob('provider', concurrency: 1, lockFor: 0);
})->throws(InvalidArgumentException::class, 'A concurrency slot must be locked for at least 1 second, [0] given.');

it('frees the slot when the rate limiter fails after the slot was taken', function () {
    Log::spy();
    $freed = false;
    app()->instance(Limiter::class, new class($freed) implements Limiter
    {
        public function __construct(private bool &$freed) {}

        public function acquire(string $key, ?Rate $rate, int $reservedSlotMs, int $budgetMs, bool $reserve = true): Decision
        {
            throw new RedisException('Connection lost');
        }

        public function pause(string $key, int $seconds): void {}

        public function enter(string $key, int $limit, int $ttlSeconds): ?Closure
        {
            return function () {
                $this->freed = true;
            };
        }
    });
    ProviderJob::dispatch('unthrottled', 'provider', 1, concurrency: 1);

    work();

    expect($freed)->toBeTrue()
        ->and(ProviderJob::$runs)->toBe(['unthrottled']);
});

it('counts concurrency waits toward maxWait', function () {
    fillConcurrencySlots(1);
    ProviderJob::dispatch('waiting', 'provider', null, concurrency: 1, maxWait: 1);

    work();

    expect($this->failures[0]->getMessage())->toBe('The job would wait past its 1 second limit on throttle key [provider]. It has already waited 0 seconds.');
});

it('logs a slot that cannot be freed and keeps the job\'s own exception', function () {
    Log::spy();
    $exceptions = [];
    Event::listen(function (JobExceptionOccurred $event) use (&$exceptions) {
        $exceptions[] = $event->exception->getMessage();
    });
    app()->instance(Limiter::class, new class implements Limiter
    {
        public function acquire(string $key, ?Rate $rate, int $reservedSlotMs, int $budgetMs, bool $reserve = true): Decision
        {
            return new Decision(0);
        }

        public function pause(string $key, int $seconds): void {}

        public function enter(string $key, int $limit, int $ttlSeconds): ?Closure
        {
            return fn () => throw new RedisException('Connection lost');
        }
    });
    ProviderJob::dispatch('failing', 'provider', null, concurrency: 1);
    ProviderJob::$failures = [new RuntimeException('Provider is down')];

    work();

    expect(ProviderJob::$runs)->toBe(['failing'])
        ->and($exceptions)->toBe(['Provider is down']);
    Log::shouldHaveReceived('warning')->withArgs(fn (string $message, array $context) => $message === 'Job throttle could not free a concurrency slot. The slot expires on its own.'
        && $context['exception']->getMessage() === 'Connection lost')->once();
});

it('rejects a concurrency limit below one', function () {
    new ThrottleJob('provider', concurrency: 0);
})->throws(InvalidArgumentException::class, 'A throttle concurrency limit must be at least 1, [0] given.');

it('relies on the two protected ConcurrencyLimiter methods Funnel exposes', function (string $method, array $parameters) {
    $reflection = new ReflectionMethod(ConcurrencyLimiter::class, $method);

    expect($reflection->isProtected())->toBeTrue()
        ->and(array_map(fn (ReflectionParameter $parameter) => $parameter->getName(), $reflection->getParameters()))->toBe($parameters)
        ->and(get_parent_class(Funnel::class))->toBe(ConcurrencyLimiter::class);
})->with([
    'acquire' => ['acquire', ['id']],
    'release' => ['release', ['key', 'id']],
]);
