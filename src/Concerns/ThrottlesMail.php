<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Concerns;

use Ctsoftwarellc\JobThrottle\Middleware\ThrottleMail;

/**
 * Throttles a queued notification or mailable to its mailer's rate limit in config/mail.php:
 *
 *     'resend' => ['transport' => 'resend', 'rate_limit' => 2, 'rate_limit_per' => 1],
 *
 * A mailer without `rate_limit` is not throttled.
 */
trait ThrottlesMail
{
    /**
     * Laravel passes the notifiable and the channel for each channel of a queued notification.
     *
     * @return array<int, object>
     */
    public function middleware(?object $notifiable = null, ?string $channel = null): array
    {
        return [
            new ThrottleMail(channel: $channel),
            ...$this->additionalMiddleware(),
        ];
    }

    /**
     * Override to add more middleware and keep the throttle.
     *
     * @return array<int, object>
     */
    protected function additionalMiddleware(): array
    {
        return [];
    }
}
