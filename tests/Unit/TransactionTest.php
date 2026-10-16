<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Unit;

use Ldkafka\GoogleFirestore\Exception\AbortedException;
use Ldkafka\GoogleFirestore\RetryPolicy;
use Ldkafka\GoogleFirestore\Tests\Support\ArrayLogger;
use Ldkafka\GoogleFirestore\Tests\Support\QueueHttpClient;
use Ldkafka\GoogleFirestore\Transaction;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The runTransaction() loop: begin, reads with the id, commit, re-runs on ABORTED, rollback.
 */
final class TransactionTest extends TestCase
{
    private const ROOT = 'projects/demo-p/databases/(default)/documents';

    public function testBeginReadCommit(): void
    {
        $http = (new QueueHttpClient())
            ->json(200, ['transaction' => 'TX1'])
            ->json(200, [['found' => self::doc(4), 'transaction' => 'TX1']])
            ->json(200, [['document' => self::doc(4)]])
            ->json(200, ['writeResults' => [['updateTime' => '2026-10-16T13:21:09Z']]]);
        $fs = $http->client();

        $n = $fs->runTransaction(static function (Transaction $tx) use ($fs): int {
            $counter = $tx->get('counters/messages');
            $tx->runQuery($fs->query('counters'));
            $n = self::int($counter?->get('n')) + 1;
            $tx->set('counters/messages', ['n' => $n]);

            return $n;
        });

        self::assertSame(5, $n);
        self::assertStringEndsWith(':beginTransaction', $http->path(0));
        self::assertSame('{"options":{"readWrite":{}}}', (string) $http->requests[0]->getBody());
        self::assertSame('TX1', $http->body(1)['transaction'] ?? null);
        self::assertSame('TX1', $http->body(2)['transaction'] ?? null);
        self::assertStringEndsWith(':commit', $http->path(3));
        self::assertSame('TX1', $http->body(3)['transaction'] ?? null);
        self::assertSame(['n' => ['integerValue' => '5']], QueueHttpClient::dig($http->body(3), 'writes', 0, 'update', 'fields'));
    }

    public function testAnAbortedCommitReRunsWithRetryTransactionAndNoRollback(): void
    {
        $logger = new ArrayLogger();
        $delays = [];
        $http = (new QueueHttpClient())
            ->json(200, ['transaction' => 'TX1'])
            ->json(200, [['found' => self::doc(1)]])
            ->error(409, 'ABORTED', 'Too much contention on these documents.')
            ->json(200, ['transaction' => 'TX2'])
            ->json(200, [['found' => self::doc(2)]])
            ->json(200, ['writeResults' => [['updateTime' => '2026-10-16T13:21:09Z']]]);
        $fs = $http->client(logger: $logger, retry: new RetryPolicy(sleep: static function (int $ms) use (&$delays): void {
            $delays[] = $ms;
        }));
        $runs = 0;

        $fs->runTransaction(static function (Transaction $tx) use (&$runs): void {
            $runs++;
            $n = self::int($tx->get('counters/messages')?->get('n'));
            $tx->set('counters/messages', ['n' => $n + 1]);
        });

        self::assertSame(2, $runs);
        self::assertCount(6, $http->requests, 'no rollback after ABORTED, no retry of the aborted commit itself');
        self::assertSame(['options' => ['readWrite' => ['retryTransaction' => 'TX1']]], $http->body(3));
        self::assertSame(['n' => ['integerValue' => '3']], QueueHttpClient::dig($http->body(5), 'writes', 0, 'update', 'fields'));
        self::assertCount(1, $delays);
        self::assertStringContainsString('transaction aborted; run 2 of 5', $logger->records[0]['message']);
    }

    public function testAnAbortedReadAlsoReRuns(): void
    {
        $http = (new QueueHttpClient())
            ->json(200, ['transaction' => 'TX1'])
            ->error(409, 'ABORTED')
            ->json(200, ['transaction' => 'TX2'])
            ->json(200, [['missing' => self::ROOT . '/counters/messages']])
            ->json(200, ['writeResults' => []]);

        $result = $http->client()->runTransaction(static fn (Transaction $tx): ?string => $tx->get('counters/messages')?->id);

        self::assertNull($result);
        self::assertCount(5, $http->requests);
    }

    public function testAnExceptionInTheCallableRollsBackAndIsRethrown(): void
    {
        $http = (new QueueHttpClient())
            ->json(200, ['transaction' => 'TX1'])
            ->json(200, new \stdClass());

        try {
            $http->client()->runTransaction(self::failing());
            self::fail('expected exception');
        } catch (RuntimeException $e) {
            self::assertSame('business rule', $e->getMessage());
        }

        self::assertCount(2, $http->requests);
        self::assertStringEndsWith(':rollback', $http->path(1));
        self::assertSame(['transaction' => 'TX1'], $http->body(1));
    }

    public function testAFailedRollbackIsOnlyLogged(): void
    {
        $logger = new ArrayLogger();
        $http = (new QueueHttpClient())
            ->json(200, ['transaction' => 'TX1'])
            ->error(400, 'INVALID_ARGUMENT', 'Invalid transaction.');

        try {
            $http->client(logger: $logger)->runTransaction(self::failing());
            self::fail('expected exception');
        } catch (RuntimeException $e) {
            self::assertSame('business rule', $e->getMessage());
        }
        self::assertStringContainsString('rollback failed', $logger->records[0]['message']);
    }

    public function testRunsAreCapped(): void
    {
        $http = (new QueueHttpClient())
            ->json(200, ['transaction' => 'TX1'])
            ->error(409, 'ABORTED')
            ->json(200, ['transaction' => 'TX2'])
            ->error(409, 'ABORTED');
        $runs = 0;

        try {
            $http->client(retry: new RetryPolicy(transactionAttempts: 2, sleep: static function (int $ms): void {
            }))->runTransaction(static function (Transaction $tx) use (&$runs): void {
                $runs++;
                $tx->set('c/a', ['x' => 1]);
            });
            self::fail('expected exception');
        } catch (AbortedException) {
            self::assertSame(2, $runs);
        }
        self::assertCount(4, $http->requests);
    }

    /**
     * A callable that buffers a write, then throws.
     *
     * @return callable(Transaction): void
     */
    private static function failing(): callable
    {
        return static function (Transaction $tx): void {
            $tx->set('c/a', ['x' => 1]);
            throw new RuntimeException('business rule');
        };
    }

    private static function int(mixed $value): int
    {
        return is_int($value) ? $value : 0;
    }

    /**
     * @return array<string, mixed>
     */
    private static function doc(int $n): array
    {
        return [
            'name' => self::ROOT . '/counters/messages',
            'fields' => ['n' => ['integerValue' => (string) $n]],
            'createTime' => '2026-10-16T13:21:07Z',
            'updateTime' => '2026-10-16T13:21:08Z',
        ];
    }
}
