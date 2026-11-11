<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Ctsoftwarellc\JobThrottle\Contracts\RetryAfter;
use Ctsoftwarellc\JobThrottle\Exceptions\ProviderRateLimited;
use Ctsoftwarellc\JobThrottle\Support\Rate;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob;
use GuzzleHttp\Psr7\Response as Psr7Response;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Facades\Date;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;

function pauseTtl(string $key = 'provider'): int
{
    return Redis::pttl("{tests:job-throttle:{$key}}:pause");
}

function rateLimitedResponse(?string $retryAfter): Response
{
    return new Response(new Psr7Response(429, $retryAfter === null ? [] : ['Retry-After' => $retryAfter]));
}

it('pauses the key and releases the reporting job for Retry-After seconds', function () {
    ProviderJob::dispatch('reporter', 'provider', 10);
    ProviderJob::$failures = [ProviderRateLimited::after(30)];

    work();

    [$released] = delayedJobs();

    expect(pauseTtl())->toBeBetween(29_000, 30_000)
        ->and($released['availableAt'] - time())->toBeBetween(30, 31)
        ->and($released['payload']['attempts'])->toBe(0)
        ->and($this->failures)->toBeEmpty();
});

it('holds every other job on the key until the pause ends', function () {
    ProviderJob::dispatch('reporter', 'provider', 10);
    ProviderJob::dispatch('next', 'provider', 10);
    ProviderJob::dispatch('other key', 'other', 10);
    ProviderJob::$failures = [ProviderRateLimited::after(30)];

    workAll();

    expect(ProviderJob::$runs)->toBe(['reporter', 'other key'])
        ->and(array_column(delayedJobs(), 'availableAt'))->each->toBeGreaterThanOrEqual(time() + 30);
});

it('gives no slot on the key before the pause ends, then keeps the rate', function () {
    $limiter = app(Limiter::class);
    $limiter->pause('provider', 2);
    $pauseEnds = redisNowMs() + 2000;

    $first = $limiter->acquire('provider', new Rate(allow: 4), 0, 60_000);
    $second = $limiter->acquire('provider', new Rate(allow: 4), 0, 60_000);

    expect($first->slotMs)->toBeBetween($pauseEnds - 5, $pauseEnds)
        ->and($second->slotMs - $first->slotMs)->toBe(250);
});

it('moves a reserved slot that falls inside a later pause to after the pause', function () {
    $limiter = app(Limiter::class);
    [, $reserved] = [$limiter->acquire('provider', new Rate(allow: 1), 0, 60_000), $limiter->acquire('provider', new Rate(allow: 1), 0, 60_000)];

    $limiter->pause('provider', 5);
    $again = $limiter->acquire('provider', new Rate(allow: 1), $reserved->slotMs, 60_000);

    expect($again->slotMs)->toBeGreaterThanOrEqual($reserved->slotMs + 3_900)
        ->and($again->waitMs)->toBeBetween(4_900, 5_000);
});

it('never lets a shorter report cut a longer pause', function () {
    app(Limiter::class)->pause('provider', 30);
    app(Limiter::class)->pause('provider', 5);

    expect(pauseTtl())->toBeBetween(29_000, 30_000);
});

it('lets a longer report extend a pause', function () {
    app(Limiter::class)->pause('provider', 5);
    app(Limiter::class)->pause('provider', 30);

    expect(pauseTtl())->toBeBetween(29_000, 30_000);
});

it('applies only the pause to a pause-only key', function () {
    $limiter = app(Limiter::class);

    $free = array_map(fn () => $limiter->acquire('airtable', null, 0, 60_000), range(1, 5));
    $limiter->pause('airtable', 3);
    $paused = $limiter->acquire('airtable', null, 0, 60_000);

    expect($free)->each(fn ($decision) => $decision->runsNow()->toBeTrue())
        ->and($paused->waitMs)->toBeBetween(2_900, 3_000)
        ->and(Redis::exists('{tests:job-throttle:airtable}:tat'))->toBe(0);
});

it('still releases the reporting job when the pause cannot be saved', function () {
    Log::spy();
    throttleRedisIsDown();
    ProviderJob::dispatch('reporter', 'provider', 10);
    ProviderJob::$failures = [ProviderRateLimited::after(30)];

    work();

    expect(delayedJobs())->toHaveCount(1)
        ->and($this->failures)->toBeEmpty();
    Log::shouldHaveReceived('warning')->withArgs(fn (string $message) => $message === 'Job throttle could not pause the key, so only this job waits.')->once();
});

it('fails a job whose pause would pass maxWait', function () {
    ProviderJob::dispatch('reporter', 'provider', 10, maxWait: 60);
    ProviderJob::$failures = [ProviderRateLimited::after(120)];

    work();

    expect(delayedJobs())->toBeEmpty()
        ->and($this->failures[0]->getMessage())->toBe('The job would wait past its 60 second limit on throttle key [provider]. It has already waited 0 seconds.');
});

it('pauses for any exception that implements RetryAfter, clamped to 1 second through 24 hours', function (int $asked, int $paused) {
    ProviderJob::dispatch('reporter', 'provider', 10, maxWait: 100_000);
    ProviderJob::$failures = [new class($asked) extends RuntimeException implements RetryAfter
    {
        public function __construct(private readonly int $seconds)
        {
            parent::__construct('Too many requests');
        }

        public function retryAfterSeconds(): int
        {
            return $this->seconds;
        }
    }];

    work();

    expect(pauseTtl())->toBeBetween($paused * 1000 - 1000, $paused * 1000);
})->with([
    'zero' => [0, 1],
    'negative' => [-5, 1],
    'over a day' => [200_000, 86_400],
]);

it('rethrows a RetryAfter from a job that runs without a queue', function () {
    ProviderJob::$failures = [ProviderRateLimited::after(30)];

    ProviderJob::dispatchSync('sync');
})->throws(ProviderRateLimited::class, 'The provider rate limited the request. Retry after 30 seconds.');

it('reads Retry-After from a response', function (?string $header, int $seconds) {
    Date::setTestNow('2026-11-10 12:00:00');

    expect(ProviderRateLimited::fromResponse(rateLimitedResponse($header), default: 60)->retryAfterSeconds())->toBe($seconds);
})->with([
    'seconds' => ['30', 30],
    'an HTTP date' => ['Tue, 10 Nov 2026 12:02:00 GMT', 120],
    'missing' => [null, 60],
    'unreadable' => ['soon', 60],
]);
