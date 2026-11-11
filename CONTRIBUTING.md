# Contributing

Thank you for helping. Bug reports, fixes and focused improvements are welcome.

## Before you start

Open an issue for anything larger than a small fix, so we can agree on the approach first. Report security issues privately, as [SECURITY.md](SECURITY.md) explains.

## Setup

You need PHP 8.2 or newer with the `redis` extension, Composer, and a Redis server you can flush. The tests use database 15.

```bash
composer install
REDIS_PORT=6379 composer test
```

Set `REDIS_HOST` and `REDIS_PORT` if your Redis is not on `127.0.0.1:6379`, and `REDIS_CLIENT=predis` to run the suite with Predis instead of PhpRedis.

## Checks

A pull request must pass every check that CI runs:

| Command | What it checks |
|---|---|
| `composer test` | The Pest suite, including the arch tests |
| `composer test:types` | 100% type coverage |
| `composer test:coverage` | At least 95% line coverage (needs PCOV or Xdebug) |
| `composer test:mutate` | At least 90% mutation score on the limiter, the middleware and the release (needs PCOV or Xdebug) |
| `composer analyse` | PHPStan at max level, with no baseline |
| `composer lint` | Pint code style (`composer format` fixes it) |

## Guidelines

- Write the test first. Each test should prove behaviour a job or a package user would notice, and read like a sentence.
- Keep classes small and `final`, fully typed, with `declare(strict_types=1)`.
- Never run a job twice. Any Redis call that can fail after a job's side effects must be caught and logged, not thrown.
- No static or singleton state that holds request, job or tenant data: workers are long-lived.
- Add a line to `CHANGELOG.md` under an Unreleased heading for changes after 1.0.0.
