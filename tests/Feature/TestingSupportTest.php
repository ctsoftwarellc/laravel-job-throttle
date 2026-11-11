<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Ctsoftwarellc\JobThrottle\Exceptions\ProviderRateLimited;
use Ctsoftwarellc\JobThrottle\Exceptions\ThrottleWaitExceeded;
use Ctsoftwarellc\JobThrottle\Facades\JobThrottle;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Ctsoftwarellc\JobThrottle\Testing\FakeLimiter;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob;
use PHPUnit\Framework\AssertionFailedError;
use Symfony\Component\Process\Process;

/**
 * Run the job through its middleware the way the queue does, with Laravel's fake queue job.
 */
function runThrottled(ProviderJob $job): ProviderJob
{
    $job->withFakeQueueInteractions();

    foreach ($job->middleware() as $middleware) {
        $middleware->handle($job, fn (ProviderJob $job) => $job->handle());
    }

    return $job;
}

function smsJob(string $name = 'sms'): ProviderJob
{
    return new ProviderJob($name, 'quo-sms:42', allow: 8);
}

it('swaps the Redis limiter for a fake that needs no Redis', function () {
    $fake = JobThrottle::fake();
    throttleRedisIsDown(failOpen: false);

    runThrottled(smsJob())->assertNotReleased();

    expect(app(Limiter::class))->toBe($fake)
        ->and(ProviderJob::$runs)->toBe(['sms']);
    JobThrottle::assertNotThrottled();
});

it('releases a job on a full key for the wait the test sets', function () {
    JobThrottle::fake()->full('quo-sms:42', for: 3);

    runThrottled(smsJob())->assertReleased(3);

    expect(ProviderJob::$runs)->toBeEmpty();
    JobThrottle::assertThrottled('quo-sms:42');
    JobThrottle::assertThrottled();
    JobThrottle::assertNotThrottled('quo-sms:7');
});

it('releases a job on a paused key until the pause ends', function () {
    JobThrottle::fake()->paused('quo-sms:42', for: 30);

    runThrottled(smsJob())->assertReleased(30);
});

it('records the pause a provider 429 asks for and holds the next job on the key', function () {
    JobThrottle::fake();
    ProviderJob::$failures = [ProviderRateLimited::after(30)];

    runThrottled(smsJob('reporter'))->assertReleased(30);
    runThrottled(smsJob('next'))->assertReleased(30);

    expect(ProviderJob::$runs)->toBe(['reporter']);
    JobThrottle::assertPaused('quo-sms:42');
    JobThrottle::assertPaused('quo-sms:42', seconds: 30);
    JobThrottle::assertNotPaused('quo-sms:7');
});

it('fails a job whose wait would pass maxWait', function () {
    JobThrottle::fake()->full('quo-sms:42', for: 120);

    runThrottled(new ProviderJob('sms', 'quo-sms:42', allow: 8, maxWait: 60))
        ->assertFailedWith(ThrottleWaitExceeded::class)
        ->assertNotReleased();
});

it('explains a failed assertion', function (Closure $assertion, string $message) {
    $fake = JobThrottle::fake();
    $fake->full('quo-sms:42', for: 3);
    ProviderJob::$failures = [ProviderRateLimited::after(5)];
    $fake->acquire('quo-sms:42', null, 0, 60_000);
    runThrottled(new ProviderJob('sms', 'quo-sms:9', allow: 8));

    expect(fn () => $assertion($fake))->toThrow(AssertionFailedError::class, $message);
})->with([
    'throttled on a key' => [fn (FakeLimiter $fake) => $fake->assertThrottled('quo-sms:1'), 'No job was throttled on key [quo-sms:1].'],
    'not throttled' => [fn (FakeLimiter $fake) => $fake->assertNotThrottled(), 'Jobs were throttled on keys [quo-sms:42].'],
    'paused' => [fn (FakeLimiter $fake) => $fake->assertPaused('quo-sms:42'), 'The key [quo-sms:42] was not paused.'],
    'paused for' => [fn (FakeLimiter $fake) => $fake->assertPaused('quo-sms:9', seconds: 30), 'The key [quo-sms:9] was not paused for 30 seconds. It was paused for [5] seconds.'],
    'not paused' => [fn (FakeLimiter $fake) => $fake->assertNotPaused(), 'The keys [quo-sms:9] were paused.'],
]);

it('explains that nothing was throttled', function () {
    JobThrottle::fake();

    JobThrottle::assertThrottled();
})->throws(AssertionFailedError::class, 'No job was throttled.');

it('expects a job to be throttled by a key or a matching key', function () {
    $job = new ProviderJob('sms', 'quo-sms:42', allow: 8);

    expect($job)->toBeThrottledBy('quo-sms:42')
        ->and($job)->toBeThrottledBy(fn (string $key) => str_starts_with($key, 'quo-sms:'))
        ->and(JobThrottle::keysFor($job))->toBe(['quo-sms:42']);
});

it('resolves a key closure against the job', function () {
    $job = new class
    {
        public int $tenant = 42;

        /** @return list<object> */
        public function middleware(): array
        {
            return [new ThrottleJob(fn (object $job) => "quo-sms:{$job->tenant}", allow: 8)];
        }
    };

    expect($job)->toBeThrottledBy('quo-sms:42');
});

it('explains a job that is not throttled by the key', function () {
    expect(new ProviderJob('sms', 'quo-sms:42', allow: 8))->toBeThrottledBy('quo-sms:7');
})->throws(AssertionFailedError::class, 'Expected [Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob] to be throttled by [quo-sms:7], but its throttle keys are [quo-sms:42].');

it('explains a job that has no key matching the closure', function () {
    expect(new ProviderJob('sms', 'quo-sms:42', allow: 8))->toBeThrottledBy(fn (string $key) => $key === 'quo-sms:7');
})->throws(AssertionFailedError::class, 'Expected [Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob] to be throttled by a matching key, but its throttle keys are [quo-sms:42].');

it('names the problem when the value is not a job', function () {
    expect('quo-sms:42')->toBeThrottledBy('quo-sms:42');
})->throws(AssertionFailedError::class, 'toBeThrottledBy() expects a job object.');

it('finds no keys on a job without throttle middleware', function () {
    expect(JobThrottle::keysFor(new stdClass))->toBe([]);
});

it('loads the Pest expectations file safely in an app without Pest', function () {
    $process = new Process([PHP_BINARY, '-r', 'require $argv[1]; echo "loaded";', realpath(__DIR__.'/../../src/Testing/pest.php')]);

    $process->mustRun();

    expect($process->getOutput())->toBe('loaded');
});
