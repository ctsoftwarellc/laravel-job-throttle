# Changelog

All notable changes to this package are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-11-11

### Added

- `ThrottleJob` queue middleware: a GCRA rate limit per key in one Redis script, timed by the Redis clock. Each throttled job is released once, to its own slot.
- Provider pauses: a job that throws `ProviderRateLimited` (or any `RetryAfter` exception) pauses its key for the provider's `Retry-After`, and every job on the key waits.
- Free waits on Redis queues: a throttle wait gives back the attempt and moves `retryUntil`, so `tries` and `retryUntil` count real failures only. `maxWait` fails a job that waits too long with `ThrottleWaitExceeded`.
- Concurrency limits per key.
- Fail open or fail closed when Redis is down.
- `ThrottlesMail` and `ThrottleMail` for `mail.mailers.*.rate_limit`, compatible with `ctsoftwarellc/laravel-mail-throttle`.
- `JobThrottle::fake()` with `assertThrottled`, `assertNotThrottled`, `assertPaused` and `assertNotPaused`, the Pest expectation `toBeThrottledBy()` and `JobThrottle::keysFor()`.

[1.0.0]: https://github.com/ctsoftwarellc/laravel-job-throttle/releases/tag/v1.0.0
