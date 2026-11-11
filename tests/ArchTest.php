<?php

declare(strict_types=1);

use Ctsoftwarellc\JobThrottle\Facades\JobThrottle;

arch('the package follows the PHP preset')->preset()->php();

arch('the package follows the security preset')->preset()->security();

arch('every file declares strict types')
    ->expect('Ctsoftwarellc\JobThrottle')
    ->toUseStrictTypes();

arch('every class is final')
    ->expect('Ctsoftwarellc\JobThrottle')
    ->classes()
    ->toBeFinal();

arch('the throttle never reaches for the testing facade')
    ->expect('Ctsoftwarellc\JobThrottle')
    ->not->toUse(JobThrottle::class)
    ->ignoring(['Ctsoftwarellc\JobThrottle\Facades', 'Ctsoftwarellc\JobThrottle\Testing']);

arch('only the testing code depends on PHPUnit')
    ->expect('PHPUnit')
    ->toOnlyBeUsedIn('Ctsoftwarellc\JobThrottle\Testing');

arch('the package follows the Laravel preset')->preset()->laravel();
