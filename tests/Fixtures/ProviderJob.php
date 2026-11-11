<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Tests\Fixtures;

use Closure;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use DateTimeInterface;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Throwable;

/**
 * A queued job that calls a provider through the throttle and records each run.
 */
final class ProviderJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable;

    /** @var list<string> */
    public static array $runs = [];

    /** @var list<Throwable> thrown by the next runs, in order */
    public static array $failures = [];

    /** @var (Closure(): void)|null called inside every run */
    public static ?Closure $during = null;

    public ?int $tries = null;

    public ?int $timeout = null;

    public ?DateTimeInterface $retryUntil = null;

    public function __construct(
        public string $name = 'job',
        public string $key = 'provider',
        public ?int $allow = 1,
        public int $every = 1,
        public ?int $concurrency = null,
        public ?int $maxWait = null,
    ) {}

    public static function reset(): void
    {
        self::$runs = [];
        self::$failures = [];
        self::$during = null;
    }

    /**
     * @return list<object>
     */
    public function middleware(): array
    {
        return [new ThrottleJob($this->key, $this->allow, $this->every, $this->concurrency, $this->maxWait)];
    }

    public function handle(): void
    {
        self::$runs[] = $this->name;

        if (self::$during !== null) {
            (self::$during)();
        }

        $failure = array_shift(self::$failures);

        if ($failure !== null) {
            throw $failure;
        }
    }
}
