<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Tests\Fixtures;

use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;

final class TraitlessProviderJob implements ShouldQueue
{
    use Dispatchable, Queueable;

    /** @return list<ThrottleJob> */
    public function middleware(): array
    {
        return [new ThrottleJob('traitless', allow: 1)];
    }

    public function handle(): void {}
}
