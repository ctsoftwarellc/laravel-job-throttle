<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Benchmarks;

use Closure;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Support\Facades\Redis;

/**
 * A provider call that takes 150 ms, limited to 8 per second on one key.
 */
final class SendSms implements ShouldQueue
{
    use InteractsWithQueue, Queueable;

    public function __construct(public string $strategy) {}

    /**
     * @return list<object>
     */
    public function middleware(): array
    {
        if ($this->strategy === 'job-throttle') {
            return [new ThrottleJob('sms', allow: 8, every: 1)];
        }

        // Laravel's Redis::throttle, released for 1 second when the window is full.
        return [new class
        {
            public function handle(SendSms $job, Closure $next): void
            {
                Redis::throttle('bench:sms')->allow(8)->every(1)->block(0)->then(
                    fn () => $next($job),
                    fn () => $job->release(1),
                );
            }
        }];
    }

    public function handle(): void
    {
        usleep(150_000);

        Redis::connection('bench')->incr('done');
    }
}
