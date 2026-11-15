<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\JobThrottleServiceProvider;
use Illuminate\Contracts\Foundation\Application;
use Orchestra\Testbench\Foundation\Application as Testbench;

require __DIR__.'/../vendor/autoload.php';
require __DIR__.'/SendSms.php';

/**
 * A Laravel app with a Redis queue in database 15. "default" carries the queue and the
 * throttle and is the connection we count; "bench" keeps the books and is not counted.
 */
function benchmarkApp(): Application
{
    $redis = [
        'host' => getenv('REDIS_HOST') ?: '127.0.0.1',
        'port' => getenv('REDIS_PORT') ?: '6379',
        'database' => 15,
    ];

    $app = Testbench::create();

    $app['config']->set([
        'database.redis.client' => 'phpredis',
        'database.redis.options.prefix' => 'job-throttle-bench:',
        'database.redis.default' => $redis,
        'database.redis.bench' => $redis,
        'queue.default' => 'redis',
        'queue.connections.redis.block_for' => null,
        'queue.failed.driver' => 'null',
        'job-throttle.key_prefix' => 'bench',
    ]);

    $app->register(JobThrottleServiceProvider::class);

    return $app;
}
