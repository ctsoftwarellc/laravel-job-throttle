<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Exceptions\ThrottleWaitExceeded;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Ctsoftwarellc\JobThrottle\Support\PayloadRelease;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\UniqueProviderJob;
use Illuminate\Bus\UniqueLock;
use Illuminate\Queue\LuaScripts;
use Illuminate\Redis\Events\CommandExecuted;
use Illuminate\Support\Facades\Bus;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;

mutates(ThrottleJob::class, PayloadRelease::class);

/**
 * Dispatch a job that runs at once, so the next job on the key has to wait.
 */
function takeTheNextSlot(int $allow = 1, int $every = 10): void
{
    ProviderJob::dispatch('first', 'provider', $allow, $every);
    work();
}

/**
 * Run a callback right after Laravel's release script moves a job to the delayed set.
 */
function afterRelease(Closure $callback): void
{
    Redis::connection()->setEventDispatcher(app('events'));

    Event::listen(function (CommandExecuted $event) use ($callback) {
        if ($event->command === 'eval' && $event->parameters[0] === LuaScripts::release()) {
            $callback();
        }
    });
}

it('keeps every try and the whole retryUntil after many waits', function () {
    takeTheNextSlot();
    $job = new ProviderJob('waiting', 'provider', 1, 10);
    $job->retryUntil = now()->addMinutes(5);
    dispatch($job);
    $deadline = now()->addMinutes(5)->getTimestamp();

    foreach (range(1, 5) as $wait) {
        work();
        releaseDelayedJobs();
    }

    $payload = json_decode(Redis::lindex('queues:default', 0), true);

    expect(ProviderJob::$runs)->toBe(['first'])
        ->and($payload['attempts'])->toBe(0)
        ->and($payload['jobThrottle']['attempt'])->toBe(1)
        ->and($payload['retryUntil'] - $deadline)->toBeBetween(50, 55)
        ->and($payload['jobThrottle']['waited'])->toBeBetween(49_000, 50_000);
});

it('still gives a job its full tries for real failures after it waited', function () {
    takeTheNextSlot(allow: 2, every: 1);
    $job = new ProviderJob('flaky', 'provider', 2, 1);
    $job->tries = 2;
    dispatch($job);
    ProviderJob::$failures = [new RuntimeException('Provider is down'), new RuntimeException('Provider is down')];

    do {
        work(tries: 2);
        releaseDelayedJobs();
        usleep(20_000);
    } while (Redis::llen('queues:default') > 0);

    expect(ProviderJob::$runs)->toBe(['first', 'flaky', 'flaky'])
        ->and($this->failures)->toHaveCount(1)
        ->and($this->failures[0]->getMessage())->toBe('Provider is down');
});

it('makes a real retry take a fresh slot instead of reusing the old one', function () {
    takeTheNextSlot();
    ProviderJob::dispatch('retried', 'provider', 1, 10);
    rewriteReadyJob(fn (array $payload) => [...$payload, 'attempts' => 1, 'jobThrottle' => [
        'attempt' => 1, 'waited' => 0, 'slots' => [sha1('provider') => redisNowMs() - 1000],
    ]]);

    work(tries: 3);

    expect(ProviderJob::$runs)->toBe(['first'])
        ->and(delayedJobs()[0]['payload']['jobThrottle']['attempt'])->toBe(2);
});

it('fails a job that would wait past maxWait with a clear reason', function () {
    takeTheNextSlot();
    ProviderJob::dispatch('patient', 'provider', 1, 10, maxWait: 5);

    work();

    expect(ProviderJob::$runs)->toBe(['first'])
        ->and(delayedJobs())->toBeEmpty()
        ->and($this->failures[0])->toBeInstanceOf(ThrottleWaitExceeded::class)
        ->and($this->failures[0]->getMessage())->toBe('The job would wait past its 5 second limit on throttle key [provider]. It has already waited 0 seconds.');
});

it('counts every wait toward maxWait', function () {
    takeTheNextSlot(allow: 1, every: 4);
    ProviderJob::dispatch('patient', 'provider', 1, 4, maxWait: 10);

    work();
    releaseDelayedJobs();
    work();
    releaseDelayedJobs();
    work();

    expect($this->failures[0]->getMessage())->toStartWith('The job would wait past its 10 second limit on throttle key [provider]. It has already waited ');
});

it('keeps the unique lock, holds the chain and leaves the batch open while a job waits', function () {
    $this->loadLaravelMigrations();
    takeTheNextSlot();
    dispatch(new UniqueProviderJob('unique'));
    Bus::chain([new ProviderJob('chained', 'provider', 1, 10), new ProviderJob('next in chain', 'other')])->dispatch();
    $batch = Bus::batch([new UniqueProviderJob('in batch')])->dispatch();

    workAll();

    expect(ProviderJob::$runs)->toBe(['first'])
        ->and(delayedJobs())->toHaveCount(3)
        ->and($batch->fresh()->pendingJobs)->toBe(1)
        ->and(Cache::lock(UniqueLock::getKey(new UniqueProviderJob('unique')))->get())->toBeFalse();
});

it('never runs a job twice when a worker takes it before the refund', function () {
    takeTheNextSlot();
    ProviderJob::dispatch('raced', 'provider', 1, 10);
    afterRelease(fn () => releaseDelayedJobs());

    work();

    expect(Redis::llen('queues:default') + count(delayedJobs()))->toBe(1)
        ->and(json_decode(Redis::lindex('queues:default', 0), true)['attempts'])->toBe(1);
});

it('logs a failed refund and leaves the job waiting, not failed', function () {
    Log::spy();
    takeTheNextSlot();
    ProviderJob::dispatch('waiting', 'provider', 1, 10);
    afterRelease(function () {
        Redis::rename('queues:default:delayed', 'released');
        Redis::set('queues:default:delayed', 'not a sorted set');
    });

    work();

    expect(ProviderJob::$runs)->toBe(['first'])
        ->and($this->failures)->toBeEmpty()
        ->and(Redis::zcard('released'))->toBe(1);
    Log::shouldHaveReceived('warning')->withArgs(fn (string $message) => $message === 'Job throttle could not refund the released job, so this wait spends one try.')->once();
});

it('runs a dispatchSync job once, without throttling it', function () {
    takeTheNextSlot();

    ProviderJob::dispatchSync('sync', 'provider', 1, 10);

    expect(ProviderJob::$runs)->toBe(['first', 'sync']);
});

it('runs a job that throws only once', function () {
    ProviderJob::dispatch('failing');
    ProviderJob::$failures = [new RuntimeException('Provider is down')];

    work();

    expect(ProviderJob::$runs)->toBe(['failing'])
        ->and(delayedJobs())->toHaveCount(1);
});

it('runs a job once when Redis is down and the throttle fails open', function () {
    Log::spy();
    throttleRedisIsDown();
    ProviderJob::dispatch('failing');
    ProviderJob::$failures = [new RuntimeException('Provider is down')];

    work();

    expect(ProviderJob::$runs)->toBe(['failing']);
    Log::shouldHaveReceived('warning')->withArgs(fn (string $message) => $message === 'Job throttle could not reach Redis, so the job runs unthrottled.')->once();
});

it('does not run the job when Redis is down and the throttle fails closed', function () {
    throttleRedisIsDown(failOpen: false);
    ProviderJob::dispatch('blocked');

    work();

    expect(ProviderJob::$runs)->toBeEmpty()
        ->and(delayedJobs())->toHaveCount(1);
});
