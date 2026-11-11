<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob;
use Illuminate\Support\Facades\Redis;

it('releases each job of a blast once, to its own slot, and finishes in about N/R seconds', function () {
    $started = microtime(true);
    foreach (range(1, 20) as $i) {
        ProviderJob::dispatch("sms-{$i}", 'provider', 10);
    }

    workAll();
    $released = delayedJobs();
    $lastAvailable = max(array_column($released, 'availableAt'));

    expect(ProviderJob::$runs)->toBe(['sms-1'])
        ->and($released)->toHaveCount(19)
        ->and(array_count_values(array_column($released, 'availableAt')))->each->toBeLessThanOrEqual(10)
        ->and($lastAvailable - time())->toBeBetween(1, 3);

    while (time() < $lastAvailable) {
        usleep(50_000);
    }
    workAll();

    expect(ProviderJob::$runs)->toHaveCount(20)
        ->and(delayedJobs())->toBeEmpty()
        ->and(microtime(true) - $started)->toBeLessThan(3.5);
});

it('keeps one small key per throttle key, which expires when the key is idle', function () {
    foreach (range(1, 3) as $i) {
        ProviderJob::dispatch("sms-{$i}", 'provider', 10);
    }

    workAll();

    $keys = Redis::keys('*job-throttle*');
    $ttl = Redis::pttl('{tests:job-throttle:provider}:tat');

    expect($keys)->toBe(['job-throttle-tests:{tests:job-throttle:provider}:tat'])
        ->and(Redis::strlen('{tests:job-throttle:provider}:tat'))->toBe(13)
        ->and($ttl)->toBeBetween(200, 300);

    usleep(($ttl + 20) * 1000);

    expect(Redis::exists('{tests:job-throttle:provider}:tat'))->toBe(0);
});
