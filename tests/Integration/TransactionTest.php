<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Integration;

use Ldkafka\GoogleFirestore\RetryPolicy;
use Ldkafka\GoogleFirestore\Tests\Support\EmulatorTestCase;
use Ldkafka\GoogleFirestore\Tests\Support\FaultInjectingClient;
use Ldkafka\GoogleFirestore\Transaction;
use RuntimeException;

use function fclose;
use function is_int;
use function is_resource;
use function microtime;
use function proc_close;
use function proc_open;
use function stream_get_contents;
use function trim;

/**
 * Transactions on the emulator: an ABORTED commit re-runs, a failure rolls back, and real
 * contention between two processes loses no increment.
 */
final class TransactionTest extends EmulatorTestCase
{
    public function testAnAbortedCommitReRunsAndTheCounterRisesByOne(): void
    {
        $this->fs->set('counters/messages', ['n' => 10]);
        $faults = new FaultInjectingClient(self::http());
        $fs = $this->client($faults);
        $runs = 0;

        $n = $fs->runTransaction(static function (Transaction $tx) use (&$runs): int {
            $runs++;
            $n = $tx->get('counters/messages')?->get('n');
            $next = (is_int($n) ? $n : 0) + 1;
            $tx->set('counters/messages', ['n' => $next]);

            return $next;
        });

        self::assertSame(1, $faults->injected);
        self::assertSame(2, $runs);
        self::assertSame(11, $n);
        self::assertSame(11, $this->fs->get('counters/messages')?->get('n'));
    }

    public function testAFailureRollsBackAndWritesNothing(): void
    {
        $this->fs->set('counters/messages', ['n' => 1]);

        try {
            $this->fs->runTransaction(self::failing());
            self::fail('expected exception');
        } catch (RuntimeException $e) {
            self::assertSame('business rule', $e->getMessage());
        }

        self::assertSame(1, $this->fs->get('counters/messages')?->get('n'));
        $this->fs->update('counters/messages', ['n' => 2]);
        self::assertSame(2, $this->fs->get('counters/messages')?->get('n'), 'the rollback released the lock');
    }

    public function testRealContentionBetweenTwoProcessesLosesNoIncrement(): void
    {
        $this->fs->set('counters/shared', ['n' => 0]);
        $startAt = (string) (microtime(true) + 1.5);
        $times = 3;

        $workers = [];
        foreach ([1, 2] as $i) {
            $process = proc_open(
                [PHP_BINARY, __DIR__ . '/../Support/claim-worker.php', 'increment', $this->host, self::PROJECT, 'counters/shared', $startAt, (string) $times],
                [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
                $pipes,
            );
            self::assertTrue(is_resource($process));
            self::assertTrue(is_resource($pipes[1]));
            $workers[$i] = [$process, $pipes[1]];
        }
        $runs = 0;
        foreach ($workers as [$process, $stdout]) {
            $runs += (int) trim((string) stream_get_contents($stdout));
            fclose($stdout);
            proc_close($process);
        }

        self::assertSame(2 * $times, $this->fs->get('counters/shared')?->get('n'));
        self::assertGreaterThanOrEqual(2 * $times, $runs);
    }

    public function testTheRunsAreCappedOnTheServerToo(): void
    {
        $this->fs->set('counters/messages', ['n' => 1]);
        $fs = $this->client(new FaultInjectingClient(self::http(), 5), new RetryPolicy(transactionAttempts: 2, baseDelayMs: 10));

        $this->expectException(\Ldkafka\GoogleFirestore\Exception\AbortedException::class);
        $fs->runTransaction(static function (Transaction $tx): void {
            $tx->set('counters/messages', ['n' => 2]);
        });
    }

    /**
     * @return callable(Transaction): void
     */
    private static function failing(): callable
    {
        return static function (Transaction $tx): void {
            $tx->get('counters/messages');
            $tx->set('counters/messages', ['n' => 99]);
            throw new RuntimeException('business rule');
        };
    }
}
