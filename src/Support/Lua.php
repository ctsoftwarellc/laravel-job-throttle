<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Support;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use RuntimeException;

/**
 * Runs the package's Lua scripts from resources/lua on a Redis connection.
 */
final class Lua
{
    /**
     * @param  list<string>  $keys
     * @param  list<string|int>  $arguments
     */
    public static function run(Connection $redis, string $script, array $keys, array $arguments): mixed
    {
        $source = (string) file_get_contents(__DIR__."/../../resources/lua/{$script}.lua");

        // PhpRedis takes the keys and arguments as one array; Predis takes them inline.
        $result = $redis->command('eval', $redis instanceof PhpRedisConnection
            ? [$source, [...$keys, ...$arguments], count($keys)]
            : [$source, count($keys), ...$keys, ...$arguments]);

        // PhpRedis answers a failed script with false instead of throwing.
        if ($result === false) {
            throw new RuntimeException("Redis rejected the job throttle {$script} script.");
        }

        return $result;
    }
}
