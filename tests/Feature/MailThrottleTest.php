<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Concerns\ThrottlesMail;
use Ctsoftwarellc\JobThrottle\Facades\JobThrottle;
use Ctsoftwarellc\JobThrottle\Middleware\ThrottleMail;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\Customer;
use Ctsoftwarellc\JobThrottle\Tests\Fixtures\OrderShipped;
use Illuminate\Mail\Mailable;
use Illuminate\Mail\SendQueuedMailable;
use Illuminate\Notifications\AnonymousNotifiable;
use Illuminate\Notifications\SendQueuedNotifications;
use Illuminate\Queue\Middleware\WithoutOverlapping;
use Illuminate\Support\Facades\Notification;

beforeEach(function () {
    OrderShipped::$sent = [];
    config(['mail.default' => 'array']);
});

function shipOrders(int $count, ?string $mailer = null): void
{
    foreach (range(1, $count) as $order) {
        Notification::route('mail', "customer-{$order}@example.com")->notify(new OrderShipped($mailer));
    }
}

it('throttles queued mail to the rate_limit of its mailer', function () {
    config(['mail.mailers.array.rate_limit' => 1]);
    shipOrders(3);

    workAll();

    expect(OrderShipped::$sent)->toHaveCount(1)
        ->and(delayedJobs())->toHaveCount(2);
});

it('spreads rate_limit evenly over rate_limit_per seconds', function () {
    config(['mail.mailers.array.rate_limit' => 2, 'mail.mailers.array.rate_limit_per' => 10]);
    shipOrders(2);

    workAll();

    expect(OrderShipped::$sent)->toHaveCount(1)
        ->and(delayedJobs()[0]['availableAt'] - time())->toBeBetween(5, 6);
});

it('sends mail unthrottled when its mailer has no rate_limit', function () {
    shipOrders(3);

    workAll();

    expect(OrderShipped::$sent)->toHaveCount(3);
});

it('throttles against the mailer the mail is sent with', function (object $job, string $key) {
    config(['mail.mailers.array.rate_limit' => 5, 'mail.mailers.ses.rate_limit' => 5, 'mail.mailers.postmark.rate_limit' => 5]);

    expect(JobThrottle::keysFor($job))->toBe([$key]);
})->with([
    'a mailable with a mailer' => fn () => [(new ThrottledMailable)->mailer('ses'), 'mail:ses'],
    'a mailable with the default mailer' => fn () => [new ThrottledMailable, 'mail:array'],
    'a queued mailable' => fn () => [
        new class((new Mailable)->mailer('ses')) extends SendQueuedMailable
        {
            public function middleware(): array
            {
                return [new ThrottleMail];
            }
        },
        'mail:ses',
    ],
    'a queued notification with a mailer' => fn () => [
        (new SendQueuedNotifications(new AnonymousNotifiable, new OrderShipped('postmark')))->through([new ThrottleMail]),
        'mail:postmark',
    ],
    'a notification with a mailer' => fn () => [new OrderShipped('postmark'), 'mail:postmark'],
    'a notification with the default mailer' => fn () => [new OrderShipped, 'mail:array'],
]);

it('takes the limit and mailer from the constructor over config', function () {
    $throttle = (new ThrottleMail(maxAttempts: 4, perSeconds: 2, mailer: 'ses'))->throttleFor(new OrderShipped);

    expect($throttle?->keyFor(new OrderShipped))->toBe('mail:ses');
});

it('throttles a mailer without config when the constructor sets a limit', function () {
    JobThrottle::fake()->full('mail:array', for: 1);
    $job = (new SendQueuedNotifications(new AnonymousNotifiable, new OrderShipped))->withFakeQueueInteractions();

    (new ThrottleMail(maxAttempts: 1))->handle($job, fn () => OrderShipped::$sent[] = 'sent');

    expect(OrderShipped::$sent)->toBeEmpty();
    $job->assertReleased(1);
});

it('throttles only the mail channel of a queued notification', function () {
    config(['mail.mailers.array.rate_limit' => 1]);
    Notification::extend('database', fn () => new class
    {
        public function send(object $notifiable, OrderShipped $notification): void
        {
            OrderShipped::$sent[] = 'database';
        }
    });

    foreach (range(1, 2) as $order) {
        (new Customer("customer-{$order}@example.com"))->notify(new OrderShipped(channels: ['mail', 'database']));
    }

    workAll();

    expect(OrderShipped::$sent)->toBe(['default', 'database', 'database'])
        ->and(delayedJobs())->toHaveCount(1);
});

it('keeps the throttle first when a notification adds middleware', function () {
    $notification = new class extends Illuminate\Notifications\Notification
    {
        use ThrottlesMail;

        protected function additionalMiddleware(): array
        {
            return [new WithoutOverlapping('orders')];
        }
    };

    expect($notification->middleware())->sequence(
        fn ($middleware) => $middleware->toBeInstanceOf(ThrottleMail::class),
        fn ($middleware) => $middleware->toBeInstanceOf(WithoutOverlapping::class),
    );
});

final class ThrottledMailable extends Mailable
{
    use ThrottlesMail;
}
