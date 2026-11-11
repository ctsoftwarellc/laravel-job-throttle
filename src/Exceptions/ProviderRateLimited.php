<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Exceptions;

use Ctsoftwarellc\JobThrottle\Contracts\RetryAfter;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Facades\Date;
use RuntimeException;

/**
 * Throw from a throttled job when the provider answers 429, before any side effect.
 */
final class ProviderRateLimited extends RuntimeException implements RetryAfter
{
    private function __construct(private readonly int $seconds)
    {
        parent::__construct("The provider rate limited the request. Retry after {$seconds} seconds.");
    }

    public static function after(int $seconds): self
    {
        return new self($seconds);
    }

    /**
     * Read the response's Retry-After header (seconds or an HTTP date), or use the default.
     */
    public static function fromResponse(Response $response, int $default = 60): self
    {
        return new self(self::secondsFrom($response->header('Retry-After')) ?? $default);
    }

    public function retryAfterSeconds(): int
    {
        return $this->seconds;
    }

    private static function secondsFrom(string $retryAfter): ?int
    {
        $seconds = filter_var($retryAfter, FILTER_VALIDATE_INT);

        if ($seconds !== false) {
            return $seconds;
        }

        $timestamp = strtotime($retryAfter);

        return $timestamp === false ? null : $timestamp - Date::now()->getTimestamp();
    }
}
