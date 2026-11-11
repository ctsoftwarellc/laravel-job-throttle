<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Mail;

use Closure;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleJob;
use Illuminate\Mail\Mailable;
use Illuminate\Notifications\Notification;

/**
 * Throttles queued mail to its mailer's `rate_limit` per `rate_limit_per` seconds from config/mail.php.
 */
final class ThrottleMail
{
    /**
     * @param  int|null  $maxAttempts  overrides the mailer's rate_limit
     * @param  int|null  $perSeconds  overrides the mailer's rate_limit_per
     * @param  string|null  $mailer  the mailer to throttle against, instead of the job's own
     */
    public function __construct(
        private readonly ?int $maxAttempts = null,
        private readonly ?int $perSeconds = null,
        private readonly ?string $mailer = null,
    ) {}

    public function handle(object $job, Closure $next, ?string $channel = null): mixed
    {
        // As notification middleware, only the mail channel is throttled.
        $throttle = $channel === null || $channel === 'mail' ? $this->throttleFor($job) : null;

        return $throttle === null ? $next($job) : $throttle->handle($job, $next);
    }

    /**
     * The throttle for the job's mailer, or null when the mailer has no rate limit.
     */
    public function throttleFor(object $job): ?ThrottleJob
    {
        $mailer = $this->mailerFor($job);
        $config = config()->array("mail.mailers.{$mailer}", []);
        $allow = $this->maxAttempts ?? $this->integer($config['rate_limit'] ?? null);

        if ($allow === null) {
            return null;
        }

        $every = $this->perSeconds ?? $this->integer($config['rate_limit_per'] ?? null) ?? 1;

        return new ThrottleJob("mail:{$mailer}", $allow, $every);
    }

    private function mailerFor(object $job): string
    {
        return $this->mailer
            ?? $this->mailerOf($job)
            ?? (property_exists($job, 'mailable') ? $this->mailerOf($job->mailable) : null)
            ?? (property_exists($job, 'notification') ? $this->mailerOf($job->notification) : null)
            ?? config()->string('mail.default');
    }

    private function mailerOf(mixed $message): ?string
    {
        $mailer = match (true) {
            $message instanceof Mailable => $message->mailer,
            $message instanceof Notification && property_exists($message, 'mailer') => $message->mailer,
            default => null,
        };

        return is_string($mailer) ? $mailer : null;
    }

    private function integer(mixed $value): ?int
    {
        return is_numeric($value) ? (int) $value : null;
    }
}
