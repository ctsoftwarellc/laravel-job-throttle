<?php

declare(strict_types=1);

// Measures a blast of N jobs at 8 per second through ThrottleJob and through Laravel's
// Redis::throttle with a 1-second release, on 10 worker processes against a real Redis.
//
// Usage: REDIS_PORT=6379 php benchmarks/run.php [N ...]    (default N: 200 1000 3000)
// It flushes Redis database 15 before every run.

use Ctsoftwarellc\JobThrottle\Benchmarks\SendSms;
use Illuminate\Support\Facades\Redis;

require __DIR__.'/bootstrap.php';

const WORKERS = 10;

$sizes = array_map(intval(...), array_slice($argv, 1)) ?: [200, 1000, 3000];

benchmarkApp();
$books = Redis::connection('bench');
$server = $books->info('server');

printf("PHP %s, Redis %s, %d workers, 8 jobs/s, 150 ms per job\n\n", PHP_VERSION, $server['redis_version'] ?? $server['Server']['redis_version'], WORKERS);
echo "| Strategy | N | Finished (s) | Ideal N/R (s) | Job pops | Pops per job | Redis round trips | Round trips per job |\n";
echo "|---|---:|---:|---:|---:|---:|---:|---:|\n";

foreach ($sizes as $n) {
    foreach (['job-throttle' => 'ThrottleJob', 'redis-throttle' => 'Redis::throttle, 1 s release'] as $strategy => $label) {
        $books->flushdb();

        foreach (range(1, $n) as $i) {
            dispatch(new SendSms($strategy));
        }

        $started = microtime(true);
        $workers = array_map(
            fn (int $id) => proc_open([PHP_BINARY, __DIR__.'/worker.php', (string) $n, (string) $id], [], $pipes),
            range(1, WORKERS),
        );

        foreach ($workers as $worker) {
            proc_close($worker);
        }

        $seconds = microtime(true) - $started;
        $pops = $roundTrips = 0;

        foreach (range(1, WORKERS) as $id) {
            $pops += (int) $books->hget("worker:{$id}", 'pops');
            $roundTrips += (int) $books->hget("worker:{$id}", 'round_trips');
        }

        printf(
            "| %s | %d | %.1f | %.1f | %d | %.2f | %d | %.1f |\n",
            $label, $n, $seconds, $n / 8, $pops, $pops / $n, $roundTrips, $roundTrips / $n,
        );
    }
}

$books->flushdb();
