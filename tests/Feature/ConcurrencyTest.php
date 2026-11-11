<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Ctsoftwarellc\JobThrottle\Support\Decision;
use Ctsoftwarellc\JobThrottle\Support\Funnel;
use Ctsoftwarellc\JobThrottle\Support\Rate;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob;
use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Redis\Limiters\ConcurrencyLimiter;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;

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
        ->and($released['payload']['attempts'])->toBe(0);
});

it('holds a slot while the job runs and frees it afterwards', function () {
    ProviderJob::dispatch('running', 'provider', null, concurrency: 1);
    ProviderJob::$during = fn () => expect(hasFreeConcurrencySlot(1))->toBeFalse();

    work();

    expect(ProviderJob::$runs)->toBe(['running'])
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

it('lets an unfreed slot expire 60 seconds after the job timeout', function () {
    $job = new ProviderJob('running', 'provider', null, concurrency: 1);
    $job->timeout = 30;
    dispatch($job);
    ProviderJob::$during = fn () => expect(Redis::ttl('{tests:job-throttle:provider}:funnel:1'))->toBe(90);

    work();

    expect(ProviderJob::$runs)->toBe(['running']);
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
    Log::shouldHaveReceived('warning')->withArgs(fn (string $message) => $message === 'Job throttle could not free a concurrency slot. The slot expires on its own.')->once();
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
