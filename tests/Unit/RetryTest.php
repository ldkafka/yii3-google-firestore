<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Unit;

use Ldkafka\GoogleFirestore\Exception\AbortedException;
use Ldkafka\GoogleFirestore\Exception\ApiException;
use Ldkafka\GoogleFirestore\Exception\TransportException;
use Ldkafka\GoogleFirestore\RetryPolicy;
use Ldkafka\GoogleFirestore\Tests\Support\ArrayLogger;
use Ldkafka\GoogleFirestore\Tests\Support\QueueHttpClient;
use PHPUnit\Framework\TestCase;

/**
 * Which calls are retried on what, and the backoff.
 */
final class RetryTest extends TestCase
{
    private const ROOT = 'projects/demo-p/databases/(default)/documents';

    /** @var list<int> */
    private array $delays = [];

    private ArrayLogger $logger;

    protected function setUp(): void
    {
        $this->delays = [];
        $this->logger = new ArrayLogger();
    }

    public function testAReadIsRetriedOn429Then503WithGrowingWaits(): void
    {
        $http = (new QueueHttpClient())
            ->error(429, 'RESOURCE_EXHAUSTED')
            ->error(503, 'UNAVAILABLE')
            ->json(200, [['missing' => self::ROOT . '/c/a']]);

        self::assertNull($this->client($http)->get('c/a'));

        self::assertCount(3, $http->requests);
        self::assertCount(2, $this->delays);
        self::assertGreaterThanOrEqual(150, $this->delays[0]);
        self::assertLessThanOrEqual(200, $this->delays[0]);
        self::assertGreaterThanOrEqual(300, $this->delays[1]);
        self::assertLessThanOrEqual(400, $this->delays[1]);
        self::assertSame('Firestore batchGet failed with RESOURCE_EXHAUSTED; retry 1 of 3 in ' . $this->delays[0] . ' ms', $this->logger->records[0]['message']);
        self::assertSame('warning', $this->logger->records[1]['level']);
    }

    public function testABare502AndNetworkErrorsAreRetriedOnReads(): void
    {
        $http = (new QueueHttpClient())
            ->json(502, '<html>Bad Gateway</html>')
            ->networkError('Connection reset')
            ->json(200, [['missing' => self::ROOT . '/c/a']]);

        self::assertNull($this->client($http)->get('c/a'));
        self::assertCount(3, $http->requests);
        self::assertStringContainsString('NETWORK_ERROR', $this->logger->records[1]['message']);
    }

    public function testExhaustedRetriesThrow(): void
    {
        $http = (new QueueHttpClient())
            ->error(503, 'UNAVAILABLE')->error(503, 'UNAVAILABLE')->error(503, 'UNAVAILABLE')->error(503, 'UNAVAILABLE');

        try {
            $this->client($http)->query('c')->get();
            self::fail('expected exception');
        } catch (ApiException $e) {
            self::assertSame('UNAVAILABLE', $e->status);
        }
        self::assertCount(4, $http->requests, 'the first attempt and 3 retries');
        self::assertCount(3, $this->delays);
    }

    public function testACommitIsRetriedOn429AndAborted(): void
    {
        $http = (new QueueHttpClient())
            ->error(429, 'RESOURCE_EXHAUSTED')
            ->error(409, 'ABORTED')
            ->json(200, ['writeResults' => [['updateTime' => '2026-10-16T13:21:09Z']]]);

        $this->client($http)->update('c/a', ['x' => 1]);

        self::assertCount(3, $http->requests);
    }

    public function testACommitIsNotRetriedOn503OrANetworkError(): void
    {
        $http = (new QueueHttpClient())->error(503, 'UNAVAILABLE');
        try {
            $this->client($http)->update('c/a', ['x' => 1]);
            self::fail('expected exception');
        } catch (ApiException $e) {
            self::assertSame('UNAVAILABLE', $e->status);
        }
        self::assertCount(1, $http->requests);

        $http = (new QueueHttpClient())->networkError('Operation timed out');
        $this->expectException(TransportException::class);
        try {
            $this->client($http)->delete('c/a');
        } finally {
            self::assertCount(1, $http->requests);
            self::assertSame([], $this->delays);
        }
    }

    public function testATransactionCommitIsNotRetriedOnAbortedByTheCall(): void
    {
        $http = (new QueueHttpClient())
            ->json(200, ['transaction' => 'TX1'])
            ->error(409, 'ABORTED');

        $this->expectException(AbortedException::class);
        $this->client($http, new RetryPolicy(transactionAttempts: 1))->runTransaction(static fn (): int => 1);
    }

    public function testPolicyBounds(): void
    {
        $policy = new RetryPolicy(maxRetries: 2, baseDelayMs: 1000, maxDelayMs: 1500, transactionAttempts: 3);

        self::assertNull($policy->delayMs(0));
        self::assertNull($policy->delayMs(3));
        self::assertLessThanOrEqual(1500, (int) $policy->delayMs(2));
        self::assertGreaterThanOrEqual(1125, (int) $policy->delayMs(2));
        self::assertNotNull($policy->transactionDelayMs(2));
        self::assertNull($policy->transactionDelayMs(3));
    }

    private function client(QueueHttpClient $http, ?RetryPolicy $retry = null): \Ldkafka\GoogleFirestore\FirestoreClient
    {
        $retry ??= new RetryPolicy(sleep: function (int $ms): void {
            $this->delays[] = $ms;
        });

        return $http->client(logger: $this->logger, retry: $retry);
    }
}
