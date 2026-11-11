<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle;

use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Ctsoftwarellc\JobThrottle\Support\RedisLimiter;
use Illuminate\Support\ServiceProvider;

final class JobThrottleServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/job-throttle.php', 'job-throttle');

        $this->app->singleton(Limiter::class, RedisLimiter::class);
    }

    public function boot(): void
    {
        $this->publishes([
            __DIR__.'/../config/job-throttle.php' => config_path('job-throttle.php'),
        ], 'job-throttle-config');
    }
}
