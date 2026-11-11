<?php

declare(strict_types=1);

$readme = file_get_contents(__DIR__.'/../../README.md');
preg_match_all('/```php\n(.*?)\n```/s', $readme, $examples);

foreach ($examples[1] as $example) {
    eval($example);
}

it('keeps the README quick start short and executes its PHP examples', function () use ($examples) {
    expect($examples[1])->toHaveCount(2)
        ->and(count(explode("\n", $examples[1][0])))->toBeLessThanOrEqual(20);
});
