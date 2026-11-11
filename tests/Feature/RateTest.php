<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Ctsoftwarellc\JobThrottle\Support\Decision;
use Ctsoftwarellc\JobThrottle\Support\PayloadRelease;
use Ctsoftwarellc\JobThrottle\Support\Rate;
use Ctsoftwarellc\JobThrottle\Support\RedisLimiter;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Redis\RedisManager;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;

mutates(RedisLimiter::class, ThrottleJob::class, PayloadRelease::class);

/**
 * @return list<Decision>
 */
function acquireMany(int $times, string $key, Rate $rate): array
{
    return array_map(fn () => app(Limiter::class)->acquire($key, $rate, 0, 3_600_000), range(1, $times));
}

/**
 * @param  list<Decision>  $decisions
 * @return list<int>
 */
function gaps(array $decisions): array
{
    $slots = array_map(fn (Decision $decision) => $decision->slotMs, $decisions);

    return array_values(array_map(fn (int $slot, int $next) => $next - $slot, array_slice($slots, 0, -1), array_slice($slots, 1)));
}

it('runs the first job now and gives each next job the slot one interval later', function () {
    $decisions = acquireMany(8, 'provider', new Rate(allow: 4, every: 1));

    expect($decisions[0]->runsNow())->toBeTrue()
        ->and(gaps($decisions))->each->toBe(250)
        ->and($decisions[7]->waitMs)->toBeBetween(1000, 1750);
});

it('never starts more than allow jobs in any window of every seconds', function () {
    $slots = array_map(fn (Decision $decision) => $decision->slotMs, acquireMany(20, 'provider', new Rate(allow: 5, every: 2)));

    foreach ($slots as $start) {
        $inWindow = array_filter($slots, fn (int $slot) => $slot >= $start && $slot < $start + 2000);

        expect(count($inWindow))->toBeLessThanOrEqual(5);
    }
});

it('throttles a fractional rate with whole milliseconds and never goes over it', function () {
    $rate = new Rate(allow: 3, every: 1);

    $decisions = acquireMany(7, 'provider', $rate);

    expect($rate->emissionMs)->toBe(334)
        ->and(gaps($decisions))->each->toBe(334)
        ->and($decisions[3]->slotMs - $decisions[0]->slotMs)->toBeGreaterThanOrEqual(1000);
});

it('keeps two keys independent', function () {
    acquireMany(5, 'quo-sms:1', new Rate(allow: 1));

    $other = app(Limiter::class)->acquire('quo-sms:2', new Rate(allow: 1), 0, 3_600_000);

    expect($other->runsNow())->toBeTrue();
});

it('shares one rate across workers with their own Redis connections', function () {
    $workers = array_map(
        fn () => new RedisLimiter(new RedisManager(app(), config('database.redis.client'), config('database.redis'))),
        range(1, 3),
    );

    $decisions = array_map(
        fn (int $i) => $workers[$i % 3]->acquire('provider', new Rate(allow: 10), 0, 3_600_000),
        range(0, 8),
    );

    expect(gaps($decisions))->each->toBe(100);
});

it('lets a job keep the slot it reserved without taking another', function () {
    $rate = new Rate(allow: 2);
    [, $reserved] = acquireMany(2, 'provider', $rate);

    $again = app(Limiter::class)->acquire('provider', $rate, $reserved->slotMs, 3_600_000);
    $next = app(Limiter::class)->acquire('provider', $rate, 0, 3_600_000);

    expect($again->slotMs)->toBe($reserved->slotMs)
        ->and($again->waitMs)->toBeGreaterThan(0)
        ->and($next->slotMs - $reserved->slotMs)->toBe(500);
});

it('runs a job at once when its reserved slot has already passed', function () {
    $decision = app(Limiter::class)->acquire('provider', new Rate(allow: 1), redisNowMs() - 5000, 3_600_000);

    expect($decision->runsNow())->toBeTrue();
});

it('spaces the starts of reserved jobs that all fell due while no worker ran', function () {
    $now = redisNowMs();

    [$first, $second, $third] = array_map(
        fn (int $ago) => app(Limiter::class)->acquire('provider', new Rate(allow: 1), $now - $ago, 3_600_000),
        [3000, 2000, 1000],
    );

    expect($first->runsNow())->toBeTrue()
        ->and($second->waitMs)->toBeGreaterThan(0)
        ->and($third->slotMs - $second->slotMs)->toBe(1000);
});

it('runs a late job on its slot when starting it keeps the rate, and spaces the jobs that would not', function () {
    foreach (['a', 'b', 'c'] as $name) {
        ProviderJob::dispatch($name, 'provider', 1);
    }

    workAll();
    $due = max(array_column(delayedJobs(), 'availableAt'));

    while (time() < $due + 1) {
        usleep(50_000);
    }

    $restarted = time();
    workAll();

    expect(ProviderJob::$runs)->toBe(['a', 'b'])
        ->and(delayedJobs())->toHaveCount(1)
        ->and(delayedJobs()[0]['availableAt'])->toBeGreaterThan($restarted);
});

it('answers over budget without reserving a slot', function () {
    $rate = new Rate(allow: 1, every: 10);
    acquireMany(2, 'provider', $rate);

    $overBudget = app(Limiter::class)->acquire('provider', $rate, 0, 15_000);
    [$next] = acquireMany(1, 'provider', $rate);

    expect($overBudget->waitMs)->toBe(-1)
        ->and($overBudget->slotMs)->toBe(0)
        ->and($next->waitMs)->toBeBetween(19_000, 20_000);
});

it('counts the rate per second unless told otherwise', function () {
    foreach (range(1, 5) as $i) {
        ProviderJob::dispatch("job-{$i}", 'provider', 4);
    }
    $before = time();

    workAll();

    expect(ProviderJob::$runs)->toBe(['job-1'])
        ->and(max(array_column(delayedJobs(), 'availableAt')))->toBeLessThanOrEqual($before + 2);
});

it('throttles queued jobs on a key to the rate across the queue', function () {
    foreach (range(1, 3) as $i) {
        ProviderJob::dispatch("job-{$i}", 'provider', 2);
    }

    workAll();

    expect(ProviderJob::$runs)->toBe(['job-1'])
        ->and(delayedJobs())->toHaveCount(2);
});

it('rejects a rate below one job per second', function (int $allow, int $every) {
    new Rate($allow, $every);
})->with([[0, 1], [1, 0]])->throws(InvalidArgumentException::class, 'A throttle rate needs at least 1 job per 1 second, [');

it('lets a job on a queue without refunds wait without reserving, so a backlog cannot starve the key', function () {
    $this->loadLaravelMigrations();
    $rate = new Rate(allow: 1, every: 10);
    app(Limiter::class)->acquire('provider', $rate, 0, 3_600_000);

    ProviderJob::dispatch('on database', 'provider', 1, 10)->onConnection('database');
    app('queue.worker')->runNextJob('database', 'default', new WorkerOptions(timeout: 0, sleep: 0, maxTries: 0));
    $next = app(Limiter::class)->acquire('provider', $rate, 0, 3_600_000);

    expect(ProviderJob::$runs)->toBeEmpty()
        ->and(DB::table('jobs')->value('attempts'))->toBe(1)
        ->and($next->waitMs)->toBeBetween(9_000, 10_000);
});

it('prefixes its Redis keys with the key prefix, then the cache prefix, then the app name', function (array $config, string $prefix) {
    config(['job-throttle.key_prefix' => null, ...$config]);

    app(Limiter::class)->acquire('provider', new Rate(allow: 1), 0, 3_600_000);

    expect(Redis::keys("*{$prefix}:job-throttle:provider*"))->toHaveCount(1);
})->with([
    'key prefix' => [['job-throttle.key_prefix' => 'billing'], 'billing'],
    'cache prefix' => [['cache.prefix' => 'shop-cache'], 'shop-cache'],
    'app name' => [['cache.prefix' => '', 'app.name' => 'Shop'], 'Shop'],
    'nothing set' => [['cache.prefix' => '', 'app.name' => ''], 'laravel'],
]);
