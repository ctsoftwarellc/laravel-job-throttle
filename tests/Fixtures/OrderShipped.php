<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Tests\Fixtures;

use Ctsoftwarellc\JobThrottle\Concerns\ThrottlesMail;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Notifications\Messages\MailMessage;
use Illuminate\Notifications\Notification;

/**
 * A queued mail notification throttled by its mailer's rate limit.
 */
final class OrderShipped extends Notification implements ShouldQueue
{
    use Queueable, ThrottlesMail;

    /** @var list<string> */
    public static array $sent = [];

    /**
     * @param  list<string>  $channels
     */
    public function __construct(public ?string $mailer = null, public array $channels = ['mail']) {}

    /**
     * @return list<string>
     */
    public function via(object $notifiable): array
    {
        return $this->channels;
    }

    public function toMail(object $notifiable): MailMessage
    {
        self::$sent[] = $this->mailer ?? 'default';

        return (new MailMessage)->line('Your order has shipped.');
    }
}
