<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Facades\JobThrottle;
use Pest\Expectation;
use PHPUnit\Framework\Assert;

// Loaded by Composer on every request; registers the expectation only where Pest is installed.
if (! class_exists(Expectation::class)) {
    return;
}

/** @var Expectation<mixed> $expectations an extension applies to expectations of any value */
$expectations = new Expectation(null);

$expectations->extend('toBeThrottledBy', function (string|Closure $key) {
    $job = $this->value;

    Assert::assertIsObject($job, 'toBeThrottledBy() expects a job object.');

    $keys = JobThrottle::keysFor($job);
    $matches = $key instanceof Closure ? array_filter($keys, $key) : array_intersect($keys, [$key]);
    $expected = $key instanceof Closure ? 'a matching key' : "[{$key}]";

    Assert::assertNotEmpty($matches, sprintf(
        'Expected [%s] to be throttled by %s, but its throttle keys are [%s].',
        $job::class,
        $expected,
        implode(', ', $keys),
    ));

    return $this;
});
