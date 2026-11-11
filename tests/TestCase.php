<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Tests;

use Ctsoftwarellc\JobThrottle\JobThrottleServiceProvider;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Redis;
use Orchestra\Testbench\TestCase as Orchestra;
use Throwable;

abstract class TestCase extends Orchestra
{
    /** @var list<Throwable> the exceptions of jobs that failed for good */
    public array $failures = [];

    protected function setUp(): void
    {
        parent::setUp();

        Queue::failing(fn (JobFailed $event) => $this->failures[] = $event->exception);

        Redis::connection()->flushdb();
        ProviderJob::reset();
    }

    protected function getPackageProviders($app): array
    {
        return [JobThrottleServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('queue.default', 'redis');
        $app['config']->set('queue.failed.driver', 'null');
        $app['config']->set('database.default', 'sqlite');
        $app['config']->set('database.connections.sqlite.database', ':memory:');
        $app['config']->set('job-throttle.key_prefix', 'tests');
        $app['config']->set('database.redis.down', [
            'host' => '127.0.0.1',
            'port' => 1,
            'timeout' => 0.1,
            'max_retries' => 0,
        ]);
    }
}
