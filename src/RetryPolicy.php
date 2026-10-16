<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore;

use Closure;

use function min;
use function mt_getrandmax;
use function mt_rand;
use function usleep;

/**
 * How often to retry and how long to wait. Which errors are retried depends on the call
 * (FirestoreClient decides); this class only holds the limits and the backoff.
 */
final readonly class RetryPolicy
{
    /**
     * @param int $maxRetries Retries per call, after the first attempt.
     * @param int $baseDelayMs Backoff base.
     * @param int $maxDelayMs Backoff cap.
     * @param int $transactionAttempts Runs of a transaction callable before ABORTED is rethrown.
     * @param Closure(int): void|null $sleep Test hook, called with milliseconds; default usleep.
     */
    public function __construct(
        public int $maxRetries = 3,
        public int $baseDelayMs = 200,
        public int $maxDelayMs = 5_000,
        public int $transactionAttempts = 5,
        private ?Closure $sleep = null,
    ) {
    }

    /**
     * Milliseconds to wait before retry number $retry (1-based), or null when the retries are spent:
     * min(maxDelayMs, baseDelayMs × 2^(retry−1)) × a jitter in [0.75, 1].
     */
    public function delayMs(int $retry): ?int
    {
        return $retry < 1 || $retry > $this->maxRetries ? null : $this->backoff($retry);
    }

    /**
     * Milliseconds to wait before re-running a transaction after run number $run (1-based) was
     * aborted, or null when that was the last allowed run.
     */
    public function transactionDelayMs(int $run): ?int
    {
        return $run < 1 || $run >= $this->transactionAttempts ? null : $this->backoff($run);
    }

    /**
     * Wait the given milliseconds.
     *
     * @internal
     */
    public function sleep(int $ms): void
    {
        if ($this->sleep !== null) {
            ($this->sleep)($ms);

            return;
        }
        usleep($ms * 1000);
    }

    private function backoff(int $retry): int
    {
        $delay = min($this->maxDelayMs, $this->baseDelayMs * 2 ** min($retry - 1, 30));
        $jitter = 0.75 + 0.25 * mt_rand() / mt_getrandmax();

        return (int) ($delay * $jitter);
    }
}
