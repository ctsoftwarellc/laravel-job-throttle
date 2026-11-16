<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Exceptions\ProviderRateLimited;
use Ctsoftwarellc\JobThrottle\Facades\JobThrottle;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Ctsoftwarellc\JobThrottle\Support\PayloadRelease;
use Ctsoftwarellc\JobThrottle\Support\RedisLimiter;
use Ctsoftwarellc\JobThrottle\Testing\FakeLimiter;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Jobs\FakeJob;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;

mutates(RedisLimiter::class, ThrottleJob::class, PayloadRelease::class);

/**
 * A queue job that counts every release and its delay.
 */
function countingQueueJob(): FakeJob
{
    return new class extends FakeJob
    {
        /** @var list<int> */
        public array $releaseDelays = [];

        public function release($delay = 0): void
        {
            parent::release($delay);

            $this->releaseDelays[] = $delay;
        }
    };
}

it('finishes a blast near N/R while allowing another wait to protect the sliding window', function () {
    $pops = 0;
    Event::listen(JobProcessing::class, function () use (&$pops) {
        $pops++;
    });
    $started = time();

    foreach (range(1, 20) as $i) {
        ProviderJob::dispatch("sms-{$i}", 'provider', 10);
    }

    workAll();
    $released = delayedJobs();

    expect(ProviderJob::$runs)->toBe(['sms-1'])
        ->and($released)->toHaveCount(19)
        ->and(array_count_values(array_column($released, 'availableAt')))->each->toBeLessThanOrEqual(10);

    // A worker that polls often runs every job in the second its slot falls in.
    while (delayedJobs() !== [] && time() < $started + 10) {
        usleep(50_000);
        workAll();
    }

    expect(ProviderJob::$runs)->toHaveCount(20)
        ->and($pops)->toBeBetween(39, 79)
        ->and(time() - $started)->toBeLessThanOrEqual(5);
});

it('keeps one small hash per throttle key, which expires when the key is idle', function () {
    foreach (range(1, 3) as $i) {
        ProviderJob::dispatch("sms-{$i}", 'provider', 10);
    }

    workAll();

    expect(Redis::keys('*job-throttle*'))->toBe(['job-throttle-tests:{tests:job-throttle:provider}:tat'])
        ->and(Redis::hlen('{tests:job-throttle:provider}:tat'))->toBe(2)
        ->and(Redis::pttl('{tests:job-throttle:provider}:tat'))->toBeGreaterThan(0);

    $deadline = microtime(true) + 5;

    while (Redis::exists('{tests:job-throttle:provider}:tat') && microtime(true) < $deadline) {
        usleep(20_000);
    }

    expect(Redis::exists('{tests:job-throttle:provider}:tat'))->toBe(0);
});

it('releases exactly once for each throttle wait', function (Closure $setup, ?int $retryAfter, int $bodyRuns, array $releaseDelays) {
    $fake = JobThrottle::fake();
    $queueJob = countingQueueJob();
    $job = new ProviderJob('waiting');
    $job->setJob($queueJob);

    $setup($fake);
    if ($retryAfter !== null) {
        ProviderJob::$failures = [ProviderRateLimited::after($retryAfter)];
    }

    (new ThrottleJob('provider', allow: 1, every: 10, concurrency: 1))->handle($job, fn (ProviderJob $job) => $job->handle());

    expect(ProviderJob::$runs)->toHaveCount($bodyRuns)
        ->and($queueJob->releaseDelays)->toBe($releaseDelays)
        ->and($queueJob->failedWith)->toBeNull();
})->with([
    'rate wait' => [fn (FakeLimiter $fake) => $fake->rateLimited('provider', 10), null, 0, [10]],
    'concurrency full' => [fn (FakeLimiter $fake) => $fake->concurrencyFull('provider'), null, 0, [2]],
    'provider 429' => [fn (FakeLimiter $fake) => null, 30, 1, [30]],
    'admitted' => [fn (FakeLimiter $fake) => null, null, 1, []],
]);
