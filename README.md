# Laravel Job Throttle

[![tests](https://github.com/ctsoftwarellc/laravel-job-throttle/actions/workflows/tests.yml/badge.svg)](https://github.com/ctsoftwarellc/laravel-job-throttle/actions/workflows/tests.yml)
[![Latest version](https://img.shields.io/packagist/v/ctsoftwarellc/laravel-job-throttle.svg)](https://packagist.org/packages/ctsoftwarellc/laravel-job-throttle)
[![License](https://img.shields.io/packagist/l/ctsoftwarellc/laravel-job-throttle.svg)](LICENSE)

One Redis throttle for every queued job that calls an outside API. It keeps each key under the provider's rate, waits as long as the provider asks after a 429, and never spends a job's tries or `retryUntil` on waiting.

- **Exact slots, not polling.** Each job gets its own slot from a GCRA rate limit and is released once, to that slot.
- **`Retry-After` pauses the whole key.** One 429 holds every job on the key, not only the job that saw it.
- **Waits are free.** A throttled job keeps all of its tries and its `retryUntil` time for real failures.
- **Concurrency limits** per key, **fail open or closed** when Redis is down, and **one small Redis key** per throttle key that expires when idle.
- **A testing fake** with Laravel-style assertions and a Pest expectation.

## Quick start

```bash
composer require ctsoftwarellc/laravel-job-throttle
```

```php
class SendSms implements ShouldQueue
{
    use Queueable;

    public function __construct(public int $tenantId, public string $to) {}

    public function middleware(): array
    {
        return [new ThrottleJob(fn (self $job) => "sms:{$job->tenantId}", allow: 8, every: 1)];
    }

    public function handle(): void
    {
        $response = Http::post('https://sms.example.com/messages', ['to' => $this->to]);

        if ($response->tooManyRequests()) {
            throw ProviderRateLimited::fromResponse($response);
        }
    }
}
```

That job now runs at most 8 times per second per tenant, across all workers, and pauses the tenant's key for as long as the provider's `Retry-After` header asks.

## Why GCRA

Laravel's `Redis::throttle()` counts jobs in a fixed window. A job that finds the window full is released for a fixed time and then tries again, so a blast of N jobs is popped again and again until each one finds room. Every one of those pops spends a try, and the jobs at the back of the queue can run out of tries or `retryUntil` before they run.

This package uses the Generic Cell Rate Algorithm instead. One Lua script, timed by the Redis clock so every worker agrees, stores one timestamp per key: the next free slot. Each job takes the next slot, and a job whose slot is in the future is released once, to that slot. The slot stays reserved for the job, so it runs when it comes back. A blast of N jobs at R per second finishes in about N/R seconds, with about two pops per job.

Slots are spaced evenly, one every `every / allow` seconds (rounded up to whole milliseconds), so no window of `every` seconds ever holds more than `allow` slots. There is no burst: a provider that allows 8 calls per second sees at most 8 calls start in any one-second window.

### Benchmark

`benchmarks/run.php` dispatches a blast of N jobs at 8 per second on 10 worker processes. Each job takes 150 ms. It counts the jobs popped and the Redis commands the workers sent (queue and throttle together), for this package and for Laravel's `Redis::throttle()` with a 1-second release.

Measured on an Apple M4 (16 GB), macOS, PHP 8.4.27, Redis 7.4.11 on the same machine, PhpRedis, workers with `--sleep=1` and unlimited tries:

| Strategy | N | Finished (s) | Ideal N/R (s) | Job pops | Pops per job | Redis round trips | Round trips per job |
|---|---:|---:|---:|---:|---:|---:|---:|
| ThrottleJob | 200 | 26.8 | 25.0 | 400 | 2.00 | 2,875 | 14.4 |
| `Redis::throttle`, 1 s release | 200 | 25.7 | 25.0 | 2,424 | 12.12 | 12,753 | 63.8 |
| ThrottleJob | 1,000 | 127.6 | 125.0 | 1,999 | 2.00 | 14,288 | 14.3 |
| `Redis::throttle`, 1 s release | 1,000 | 129.8 | 125.0 | 55,433 | 55.43 | 280,192 | 280.2 |
| ThrottleJob | 3,000 | 377.4 | 375.0 | 5,999 | 2.00 | 42,813 | 14.3 |
| `Redis::throttle`, 1 s release | 3,000 | 377.3 | 375.0 | 391,174 | 130.39 | 1,962,479 | 654.2 |

Both finish in about N/R seconds, because both keep the rate. The difference is the work it takes: with `Redis::throttle()` every pop is an attempt, so the average job in a blast of 3,000 spends 130 tries, and any `tries` or `retryUntil` you set would fail the tail of the blast. With this package each job is popped twice (once to reserve its slot, once to run in it) and keeps all of its tries. Round trips per job include the queue's own polling.

Run it yourself against a scratch Redis (it flushes database 15): `REDIS_PORT=6379 php benchmarks/run.php 200 1000 3000`.

## Provider rate limits (429 and `Retry-After`)

Throw `ProviderRateLimited` from the job when the provider refuses the call, before any side effect:

```php
throw ProviderRateLimited::fromResponse($response);              // reads Retry-After: seconds or an HTTP date; 60 s if missing
throw ProviderRateLimited::fromResponse($response, default: 30);
throw ProviderRateLimited::after(30);
```

The middleware pauses the key for that long and releases the job until the pause ends. Every other job on the key waits for the pause too, then takes slots at the normal rate after it, so they do not all hit the provider at once. A shorter pause never cuts a longer one.

Your own exceptions can do the same: implement `Ctsoftwarellc\JobThrottle\Contracts\RetryAfter`.

```php
final class QuickBooksRateLimited extends QuickBooksException implements RetryAfter
{
    public function retryAfterSeconds(): int
    {
        return $this->retryAfter;
    }
}
```

The wait is clamped to 1 second through 24 hours. If a key needs only the pause and no rate, use `ThrottleJob::pauseOnly('airtable:'.$this->baseId)`.

## Waits do not spend tries

Laravel counts every release as an attempt. This package releases the job and then, on a Redis queue, rewrites the released copy in one atomic Lua script: the attempt count goes back, `retryUntil` moves forward by the wait, and the job keeps its slot. The job keeps the same id, uuid, unique lock, chain and batch. `tries`, `maxExceptions`, `backoff` and `retryUntil` all apply to real failures only.

A job that waits too long in total fails with `ThrottleWaitExceeded`, which names the key, the time waited and the limit. The limit is `maxWait` on the middleware, or the `max_wait` config (3600 seconds).

```php
new ThrottleJob('gusto:'.$this->locationId, allow: 60, every: 60, maxWait: 900);
```

If the job is `ShouldBeUnique`, set `uniqueFor` to at least `maxWait`, or the unique lock can expire while the job waits.

## Concurrency

```php
new ThrottleJob('reports:'.$this->accountId, allow: 10, every: 60, concurrency: 2);
```

At most 2 jobs on the key run at once. A job that finds every slot taken is released for `concurrency_retry` seconds (2 by default) without spending a try, and that wait counts toward `maxWait`. A slot is freed when the job finishes or throws. If the worker dies first, the slot stays taken for `lockFor` seconds: by default the `concurrency_lock_for` config (660, above a 600-second worker `--timeout`), or the job's own `timeout` plus 60 when that is longer. Set it above your longest worker `--timeout`, because a slot that expires while its job still runs lets one more job in:

```php
new ThrottleJob('reports:'.$this->accountId, concurrency: 2, lockFor: 1800);
```

`concurrency` works without `allow` too: `new ThrottleJob('exports', concurrency: 1)`.

## Options

| Middleware argument | Default | Meaning |
|---|---|---|
| `key` | required | The throttle key, or a closure that builds it from the job. Jobs on the same key share the limit. |
| `allow` | `null` | Jobs per `every` seconds. `null` applies no rate, only pauses and concurrency. |
| `every` | `1` | The rate window in seconds. |
| `concurrency` | `null` | The most jobs on the key that may run at once. |
| `maxWait` | `max_wait` | The most seconds a job may wait in total before it fails. |
| `lockFor` | `concurrency_lock_for` | The most seconds a concurrency slot stays taken when its worker dies. |

Publish the config with `php artisan vendor:publish --tag=job-throttle-config`.

| Config key | Default | Meaning |
|---|---|---|
| `connection` | default connection | The Redis connection for the throttle keys. |
| `key_prefix` | `cache.prefix`, then `app.name` | Keeps apps that share a Redis apart. |
| `fail_open` | `true` | When Redis is down: `true` runs the job unthrottled and logs a warning, `false` throws so the job fails this try. |
| `max_wait` | `3600` | The default `maxWait`, in seconds. |
| `concurrency_retry` | `2` | Seconds a job waits when every concurrency slot is taken. |
| `concurrency_lock_for` | `660` | Seconds a concurrency slot stays taken when its worker dies, unless the job's `timeout` plus 60 is longer. |

## Mail

Throttle queued notifications and mailables to their mailer's limit in `config/mail.php`:

```php
'resend' => [
    'transport' => 'resend',
    'rate_limit' => 2,      // 2 emails
    'rate_limit_per' => 1,  // per second
],
```

```php
use Ctsoftwarellc\JobThrottle\Concerns\ThrottlesMail;

class InvoicePaid extends Notification implements ShouldQueue
{
    use Queueable, ThrottlesMail;
}
```

A mailer without `rate_limit` is not throttled. Override `additionalMiddleware()` to add more middleware, or use the middleware directly with your own limit: `new ThrottleMail(maxAttempts: 2, perSeconds: 1, mailer: 'ses')`.

### Moving from `ctsoftwarellc/laravel-mail-throttle`

Swap the package and change the `use` line from `Ctsoftwarellc\MailThrottle\Concerns\ThrottlesMail` to `Ctsoftwarellc\JobThrottle\Concerns\ThrottlesMail`. The trait, the constructor and the `rate_limit` config work as before. The differences: sends are spaced evenly through the window instead of bursting at its start, waits no longer spend tries, and the `mail-throttle.*` release, backoff and jitter settings are gone because each email is released once, to its slot. `MAIL_THROTTLE_FAIL_OPEN` and `MAIL_THROTTLE_KEY_PREFIX` become `JOB_THROTTLE_FAIL_OPEN` and `JOB_THROTTLE_KEY_PREFIX`.

## Testing

`JobThrottle::fake()` swaps the Redis limiter for an in-memory fake, so your tests need no Redis. Every key is free until you say otherwise. Laravel's own `withFakeQueueInteractions()` assertions check the release.

```php
use Ctsoftwarellc\JobThrottle\Exceptions\ProviderRateLimited;
use Ctsoftwarellc\JobThrottle\Facades\JobThrottle;

it('waits for the next free slot', function () {
    JobThrottle::fake()->rateLimited('sms:42', for: 3);
    $job = (new SendSms(42, '+15550100'))->withFakeQueueInteractions();

    (new ThrottleJob('sms:42', allow: 8))->handle($job, fn ($job) => $job->handle());

    $job->assertReleased(3);
    JobThrottle::assertThrottled('sms:42');
});

it('pauses the tenant when the provider answers 429', function () {
    JobThrottle::fake();
    Http::fake(['*' => Http::response(status: 429, headers: ['Retry-After' => '30'])]);
    $job = (new SendSms(42, '+15550100'))->withFakeQueueInteractions();

    (new ThrottleJob('sms:42', allow: 8))->handle($job, fn ($job) => $job->handle());

    $job->assertReleased(30);
    JobThrottle::assertPaused('sms:42', seconds: 30);
});
```

| Fake | |
|---|---|
| `->rateLimited($key, for: $seconds)` | The key's rate is used up: a job on it waits `$seconds` for its slot. |
| `->paused($key, for: $seconds)` | The key is paused, as after a 429: a job on it waits `$seconds`. |
| `->concurrencyFull($key)` | Every concurrency slot on the key is taken: a job on it waits `concurrency_retry` seconds. |
| `JobThrottle::assertThrottled(?$key)` / `assertNotThrottled(?$key)` | A job had to wait, on the key or on any key. |
| `JobThrottle::assertPaused($key, ?$seconds)` / `assertNotPaused(?$key)` | A job reported a 429 on the key. |

With Pest, check which keys a job is throttled by, resolved from its own `middleware()`:

```php
expect(new SendSms(42, '+15550100'))->toBeThrottledBy('sms:42');
expect(new SendSms(42, '+15550100'))->toBeThrottledBy(fn (string $key) => str_starts_with($key, 'sms:'));
```

The expectation loads only when Pest is installed. In PHPUnit, use `JobThrottle::keysFor($job)`, which returns the same keys.

## Limits

- **Redis queues only get free waits.** On the database, SQS or another queue, a wait spends a try like a plain `release()`, and a waiting job takes no slot: it asks again when the next slot is free. `maxWait` still holds there: the throttle keeps the time each job waited in its own Redis, by the job's uuid, until the job can no longer wait.
- **Whole seconds.** Laravel releases jobs in whole seconds, so jobs whose slots fall in the same second start together. The rate over each second holds.
- **Late jobs keep the rate.** A job that comes back after its slot (a busy queue, a slow worker) runs at once if the key has room in the current second. Jobs that come back together after a worker outage do not all start at once: one second's worth starts, and the rest take new slots at the rate.
- **Poll at least once a second.** A worker that sleeps 3 seconds between empty polls finds 3 seconds of released jobs at once, and only one second's worth may start. Run enough worker processes, or a short `--sleep`, that a released job is picked up in the second it becomes due.
- **Redis Cluster** keeps the throttle keys in one slot. The free-wait rewrite needs the queue key Laravel builds outside a cluster; on a cluster a wait spends one try.
- **One try in a race.** If a worker takes a released job in the moment before the rewrite, that wait spends one try. A job never runs twice.
- **Concurrency waits poll** every `concurrency_retry` seconds; there is no waiting list.
- **Sync jobs run unthrottled.** `dispatchSync()` and jobs run without a queue are not throttled, because releasing them would drop them.
- The concurrency limit uses two protected methods of Laravel's `ConcurrencyLimiter`. A test pins them on every Laravel version in CI.

## Compatibility

| | |
|---|---|
| PHP | 8.2, 8.3, 8.4, 8.5 |
| Laravel | 12 (PHP 8.2+), 13 (PHP 8.3+) |
| Redis server | 6.2 or newer (CI runs 6.2 and 7) |
| Redis client | PhpRedis or Predis, both tested in CI |
| Queues | Redis and Horizon for free waits; any queue for throttling |

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md). Report security issues privately, as [SECURITY.md](SECURITY.md) explains.

## License

MIT. See [LICENSE](LICENSE).
