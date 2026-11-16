<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Ctsoftwarellc\JobThrottle\Support\Decision;
use Ctsoftwarellc\JobThrottle\Support\PayloadRelease;
use Ctsoftwarellc\JobThrottle\Support\Rate;
use Ctsoftwarellc\JobThrottle\Support\RedisLimiter;
use Ctsoftwarellc\JobThrottle\Support\ThrottleState;
use Illuminate\Contracts\Redis\Factory;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisConnection;

mutates(RedisLimiter::class, ThrottleJob::class, PayloadRelease::class);

/**
 * A mocked Redis connection that records each command's exact wire arguments.
 *
 * @param  array<int, mixed>  $results  one return value per command, in order
 * @return array{0: Mockery\MockInterface, 1: object{commands: list<array{string, list<mixed>}>}}
 */
function recordingConnection(string $class, array &$results): array
{
    $recorder = new class
    {
        /** @var list<array{string, list<mixed>}} */
        public array $commands = [];
    };

    $connection = Mockery::mock($class);
    $connection->shouldReceive('command')->andReturnUsing(
        function (string $method, array $parameters) use ($recorder, &$results) {
            $recorder->commands[] = [$method, $parameters];

            return array_shift($results);
        },
    );

    return [$connection, $recorder];
}

/**
 * A RedisLimiter whose factory hands out the given connection for the given name.
 */
function limiterWith(Mockery\MockInterface $connection, ?string $name = null): RedisLimiter
{
    $factory = Mockery::mock(Factory::class);
    $factory->shouldReceive('connection')->with($name)->andReturn($connection);

    return new RedisLimiter($factory);
}

it('sends the acquire script with cluster-tagged keys and typed arguments over PhpRedis', function () {
    $results = [[1234, 5678]];
    [$connection, $recorder] = recordingConnection(PhpRedisConnection::class, $results);
    $limiter = limiterWith($connection);

    $decision = $limiter->acquire('provider', new Rate(3, 2), 42, 3_600_000, reserve: true);

    $commands = $recorder->commands;

    expect($decision)->toBeInstanceOf(Decision::class)
        ->and($decision->waitMs)->toBe(1234)
        ->and($decision->slotMs)->toBe(5678)
        ->and($commands)->toHaveCount(1);

    [$method, $parameters] = $commands[0];
    $script = (string) file_get_contents(__DIR__.'/../../resources/lua/gcra.lua');

    expect($method)->toBe('eval')
        ->and($parameters[0])->toBe($script)
        ->and($parameters[1])->toBe([
            '{tests:job-throttle:provider}:tat',
            '{tests:job-throttle:provider}:pause',
            'acquire',
            667,
            42,
            3_600_000,
            1,
            3,
            2000,
        ])
        ->and($parameters[2])->toBe(2);
});

it('sends a pause-only acquire with a zero rate and no reservation over Predis', function () {
    $results = [[0, 0]];
    [$connection, $recorder] = recordingConnection(PredisConnection::class, $results);
    $limiter = limiterWith($connection);

    $decision = $limiter->acquire('provider', null, 0, 3_600_000, reserve: false);

    $commands = $recorder->commands;

    expect($decision->waitMs)->toBe(0)
        ->and($decision->slotMs)->toBe(0)
        ->and($commands)->toHaveCount(1);

    [$method, $parameters] = $commands[0];
    $script = (string) file_get_contents(__DIR__.'/../../resources/lua/gcra.lua');

    expect($method)->toBe('eval')
        ->and($parameters[0])->toBe($script)
        ->and($parameters[1])->toBe(2)
        ->and($parameters[2])->toBe('{tests:job-throttle:provider}:tat')
        ->and($parameters[3])->toBe('{tests:job-throttle:provider}:pause')
        ->and(array_slice($parameters, 4))->toBe(['acquire', 0, 0, 3_600_000, 0, 0, 0]);
});

it('uses the default connection by default and the configured connection when set', function (?string $configured) {
    if ($configured !== null) {
        config(['job-throttle.connection' => $configured]);
    }

    $results = [[0]];
    [$connection, $recorder] = recordingConnection(PhpRedisConnection::class, $results);
    $limiter = limiterWith($connection, $configured);

    $limiter->pause('provider', 30);

    $commands = $recorder->commands;

    expect($commands)->toHaveCount(1)
        ->and($commands[0][0])->toBe('eval')
        ->and($commands[0][1][1][2])->toBe('pause')
        ->and($commands[0][1][1][3])->toBe(30_000);
})->with([
    'the default connection' => [null],
    'a configured connection' => ['throttle'],
]);

it('roundtrips the wait state at the prefixed state key with its exact ttl', function () {
    $stored = json_encode(['attempt' => 2, 'waited' => 1234, 'slots' => [hash('xxh128', 'provider') => 5678]]);
    $results = ['OK', $stored];
    [$connection, $recorder] = recordingConnection(PhpRedisConnection::class, $results);
    $limiter = limiterWith($connection);
    $state = new ThrottleState(attempt: 2, waitedMs: 1234, slots: [hash('xxh128', 'provider') => 5678]);

    $limiter->remember('job-uuid', $state, 123);
    $roundtrip = $limiter->state('job-uuid');

    $commands = $recorder->commands;

    expect($roundtrip->attempt)->toBe(2)
        ->and($roundtrip->waitedMs)->toBe(1234)
        ->and($roundtrip->slots)->toBe([hash('xxh128', 'provider') => 5678])
        ->and($commands)->toHaveCount(2);

    expect($commands[0][0])->toBe('setex')
        ->and($commands[0][1][0])->toBe('tests:job-throttle-state:job-uuid')
        ->and($commands[0][1][1])->toBe(123)
        ->and($commands[0][1][2])->toBe($stored);

    expect($commands[1][0])->toBe('get')
        ->and($commands[1][1])->toBe(['tests:job-throttle-state:job-uuid']);
});

it('keeps the wait state of two jobs and two app prefixes apart', function () {
    $results = [];
    [$connection, $recorder] = recordingConnection(PhpRedisConnection::class, $results);
    $limiter = limiterWith($connection);

    $limiter->remember('job-one', new ThrottleState(waitedMs: 1000), 60);
    $limiter->remember('job-two', new ThrottleState(waitedMs: 2000), 60);
    config(['job-throttle.key_prefix' => 'other-app']);
    $limiter->remember('job-one', new ThrottleState(waitedMs: 3000), 60);

    $setexKeys = array_map(
        fn (array $command) => $command[1][0],
        array_values(array_filter($recorder->commands, fn (array $command) => $command[0] === 'setex')),
    );

    expect($setexKeys[0])->toBe('tests:job-throttle-state:job-one')
        ->and($setexKeys[1])->toBe('tests:job-throttle-state:job-two')
        ->and($setexKeys[2])->toBe('other-app:job-throttle-state:job-one');
});

it('reads no state when the key holds nothing', function () {
    $results = [null];
    [$connection] = recordingConnection(PredisConnection::class, $results);
    $limiter = limiterWith($connection);

    expect($limiter->state('missing'))->toEqual(new ThrottleState);
});
