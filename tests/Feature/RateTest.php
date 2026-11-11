<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Contracts\Limiter;
use Ctsoftwarellc\JobThrottle\Support\Decision;
use Ctsoftwarellc\JobThrottle\Support\Rate;
use Ctsoftwarellc\JobThrottle\Support\RedisLimiter;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\ProviderJob;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Redis\RedisManager;
use Illuminate\Support\Facades\DB;

mutates(RedisLimiter::class);

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
        ->and($decisions[7]->waitMs)->toBeBetween(1700, 1750);
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
        fn () => new RedisLimiter(new RedisManager(app(), 'phpredis', config('database.redis'))),
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

it('answers over budget without reserving a slot', function () {
    $rate = new Rate(allow: 1, every: 10);
    acquireMany(2, 'provider', $rate);

    $overBudget = app(Limiter::class)->acquire('provider', $rate, 0, 15_000);
    [$next] = acquireMany(1, 'provider', $rate);

    expect($overBudget->waitMs)->toBe(-1)
        ->and($overBudget->slotMs)->toBe(0)
        ->and($next->waitMs)->toBeBetween(19_900, 20_000);
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
        ->and($next->waitMs)->toBeBetween(9_900, 10_000);
});
