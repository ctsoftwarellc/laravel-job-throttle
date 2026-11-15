<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Tests\Fixtures;

use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Illuminate\Bus\Batchable;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\InteractsWithQueue;

/**
 * A unique, batchable job on the throttle key "provider" at 1 job per 10 seconds.
 */
final class UniqueProviderJob implements ShouldBeUnique, ShouldQueue
{
    use Batchable, InteractsWithQueue, Queueable;

    public function __construct(public string $name) {}

    public function uniqueId(): string
    {
        return $this->name;
    }

    /**
     * @return list<object>
     */
    public function middleware(): array
    {
        return [new ThrottleJob('provider', allow: 1, every: 10)];
    }

    public function handle(): void
    {
        ProviderJob::$runs[] = $this->name;
    }
}
