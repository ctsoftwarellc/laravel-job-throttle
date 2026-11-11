<?php

declare(strict_types=1);

// One queue worker: works jobs until the run is done, then saves what it counted.
// Usage: php benchmarks/worker.php <jobs> <id>

use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Redis\Events\CommandExecuted;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;

require __DIR__.'/bootstrap.php';

[, $jobs, $id] = $argv;

$app = benchmarkApp();
$pops = 0;
$roundTrips = 0;
$books = Redis::connection('bench');

Redis::connection()->setEventDispatcher($app['events']);
Event::listen(JobProcessing::class, function () use (&$pops) {
    $pops++;
});
Event::listen(CommandExecuted::class, function (CommandExecuted $event) use (&$roundTrips) {
    if ($event->connectionName === 'default') {
        $roundTrips++;
    }
});

$worker = $app['queue.worker'];
$options = new WorkerOptions(timeout: 0, sleep: 1, maxTries: 0);

while ((int) $books->get('done') < (int) $jobs) {
    $worker->runNextJob('redis', 'default', $options);
}

$books->hset("worker:{$id}", 'pops', $pops, 'round_trips', $roundTrips);
