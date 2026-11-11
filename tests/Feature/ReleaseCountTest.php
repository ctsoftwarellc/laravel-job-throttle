<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Ctsoftwarellc\JobThrottle\Support\PayloadRelease;
use Ctsoftwarellc\JobThrottle\Support\RedisLimiter;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;

mutates(RedisLimiter::class, ThrottleJob::class, PayloadRelease::class);

it('releases each job of a blast once, to its own slot, and finishes in about N/R seconds', function () {
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
        ->and($pops)->toBe(39)
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
