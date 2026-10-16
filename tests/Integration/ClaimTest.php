<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Integration;

use Ldkafka\GoogleFirestore\Exception\FailedPreconditionException;
use Ldkafka\GoogleFirestore\FieldValue;
use Ldkafka\GoogleFirestore\Precondition;
use Ldkafka\GoogleFirestore\Tests\Support\EmulatorTestCase;

use function fclose;
use function is_resource;
use function microtime;
use function proc_close;
use function proc_open;
use function sort;
use function stream_get_contents;
use function trim;

/**
 * Two agents claim one message: exactly one wins.
 */
final class ClaimTest extends EmulatorTestCase
{
    private const ROUNDS = 10;

    public function testTheSecondClaimWithTheOldVersionIsRefused(): void
    {
        $this->fs->create('messages', ['to' => 'infra', 'status' => 'new'], 'm1');
        $seenByA = $this->fs->get('messages/m1');
        $seenByB = $this->fs->get('messages/m1');
        self::assertNotNull($seenByA);
        self::assertNotNull($seenByB);

        $this->fs->update('messages/m1', ['status' => 'claimed', 'claimant' => 'A', 'claimedAt' => FieldValue::serverTimestamp()], Precondition::updateTime($seenByA->updateTime));
        try {
            $this->fs->update('messages/m1', ['status' => 'claimed', 'claimant' => 'B'], Precondition::updateTime($seenByB->updateTime));
            self::fail('B should lose');
        } catch (FailedPreconditionException $e) {
            self::assertSame('FAILED_PRECONDITION', $e->status);
        }

        self::assertSame('A', $this->fs->get('messages/m1')?->get('claimant'));
    }

    public function testTwoProcessesClaimingAtOnceGiveExactlyOneWinnerEachRound(): void
    {
        for ($round = 1; $round <= self::ROUNDS; $round++) {
            $path = 'messages/round-' . $round;
            $this->fs->create('messages', ['to' => 'infra', 'status' => 'new'], 'round-' . $round);
            $startAt = (string) (microtime(true) + 1.5);

            $workers = [];
            foreach (['A', 'B'] as $name) {
                $workers[$name] = $this->start(['claim', $this->host, self::PROJECT, $path, $startAt, $name]);
            }
            $results = [];
            foreach ($workers as $name => $worker) {
                $results[$name] = $this->finish($worker);
            }

            $outcomes = $results;
            sort($outcomes);
            self::assertSame(['lost', 'won'], $outcomes, 'round ' . $round . ': ' . json_encode($results));
            $winner = $results['A'] === 'won' ? 'A' : 'B';
            self::assertSame($winner, $this->fs->get($path)?->get('claimant'), 'round ' . $round);
        }
    }

    /**
     * @param list<string> $args
     * @return array{process: resource, stdout: resource}
     */
    private function start(array $args): array
    {
        $process = proc_open([PHP_BINARY, __DIR__ . '/../Support/claim-worker.php', ...$args], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        self::assertTrue(is_resource($process));
        self::assertTrue(is_resource($pipes[1]));

        return ['process' => $process, 'stdout' => $pipes[1]];
    }

    /**
     * @param array{process: resource, stdout: resource} $worker
     */
    private function finish(array $worker): string
    {
        $out = trim((string) stream_get_contents($worker['stdout']));
        fclose($worker['stdout']);
        proc_close($worker['process']);

        return $out;
    }
}
