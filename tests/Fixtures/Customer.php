<?php

declare(strict_types=1);

namespace Ctsoftwarellc\JobThrottle\Tests\Fixtures;

use Illuminate\Notifications\Notifiable;

/**
 * A notifiable that is not a model, so it reaches every channel without a database.
 */
final class Customer
{
    use Notifiable;

    public function __construct(public string $email) {}
}
