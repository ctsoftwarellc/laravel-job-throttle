<?php

declare(strict_types=1);

return [

    /*
    |--------------------------------------------------------------------------
    | Redis Connection
    |--------------------------------------------------------------------------
    |
    | The Redis connection that holds the throttle keys. Null uses the default
    | connection. Every worker that shares a limit must use the same Redis.
    |
    */

    'connection' => env('JOB_THROTTLE_REDIS_CONNECTION'),

    /*
    |--------------------------------------------------------------------------
    | Key Prefix
    |--------------------------------------------------------------------------
    |
    | Keeps the throttle keys of apps that share one Redis apart. When empty,
    | the cache prefix is used, then the app name.
    |
    */

    'key_prefix' => env('JOB_THROTTLE_KEY_PREFIX'),

    /*
    |--------------------------------------------------------------------------
    | Fail Open
    |--------------------------------------------------------------------------
    |
    | When Redis cannot be reached, true runs the job unthrottled and logs a
    | warning. False throws, so the job fails this try and is retried.
    |
    */

    'fail_open' => (bool) env('JOB_THROTTLE_FAIL_OPEN', true),

    /*
    |--------------------------------------------------------------------------
    | Maximum Total Wait
    |--------------------------------------------------------------------------
    |
    | The longest a job may wait for the throttle over its whole life, in
    | seconds. A job that would wait longer fails with ThrottleWaitExceeded.
    | Each middleware can set its own maxWait.
    |
    */

    'max_wait' => (int) env('JOB_THROTTLE_MAX_WAIT', 3600),

    /*
    |--------------------------------------------------------------------------
    | Concurrency Retry
    |--------------------------------------------------------------------------
    |
    | How many seconds a job waits before it tries again when every
    | concurrency slot on its key is taken.
    |
    */

    'concurrency_retry' => (int) env('JOB_THROTTLE_CONCURRENCY_RETRY', 2),

    /*
    |--------------------------------------------------------------------------
    | Concurrency Lock
    |--------------------------------------------------------------------------
    |
    | How many seconds a concurrency slot stays taken when a worker dies
    | before it can free it. Keep it above your longest worker --timeout:
    | a slot that expires while its job runs lets one more job in. A job's
    | own timeout plus 60 seconds is used when that is longer. Each
    | middleware can set its own lockFor.
    |
    */

    'concurrency_lock_for' => (int) env('JOB_THROTTLE_CONCURRENCY_LOCK_FOR', 660),

];
