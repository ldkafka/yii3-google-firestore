<?php

declare(strict_types=1);

/**
 * One competing process for the integration ClaimTest and TransactionTest.
 *
 *   php claim-worker.php claim <emulator host> <project> <document path> <start at, unix float> <name>
 *     Reads the document, waits for the start time, claims it with an updateTime precondition and
 *     prints "won" or "lost".
 *   php claim-worker.php increment <emulator host> <project> <document path> <start at> <times>
 *     Increments field n in a transaction <times> times and prints the number of callable runs.
 */

use HttpSoft\Message\RequestFactory;
use HttpSoft\Message\StreamFactory;
use Ldkafka\GoogleFirestore\Exception\FailedPreconditionException;
use Ldkafka\GoogleFirestore\Exception\NotFoundException;
use Ldkafka\GoogleFirestore\FieldValue;
use Ldkafka\GoogleFirestore\FirestoreClient;
use Ldkafka\GoogleFirestore\Precondition;
use Ldkafka\GoogleFirestore\RetryPolicy;
use Ldkafka\GoogleFirestore\Transaction;
use Ldkafka\GoogleFirestore\Tests\Support\EmulatorTestCase;

require __DIR__ . '/../../vendor/autoload.php';

$args = [];
$argv = $_SERVER['argv'] ?? [];
foreach (is_array($argv) ? array_slice($argv, 1) : [] as $value) {
    $args[] = is_string($value) ? $value : '';
}
[$mode, $host, $project, $path, $startAt, $arg] = $args + ['', '', '', '', '0', ''];

$fs = new FirestoreClient(
    EmulatorTestCase::http(),
    new RequestFactory(),
    new StreamFactory(),
    null,
    $project,
    emulatorHost: $host,
    retry: new RetryPolicy(transactionAttempts: 20, maxDelayMs: 1000),
);

$waitForStart = static function () use ($startAt): void {
    $delay = (float) $startAt - microtime(true);
    if ($delay > 0) {
        usleep((int) ($delay * 1_000_000));
    }
};

if ($mode === 'claim') {
    $message = $fs->get($path);
    if ($message === null) {
        echo "missing\n";
        exit(1);
    }
    $waitForStart();
    try {
        $fs->update($path, [
            'status' => 'claimed',
            'claimant' => $arg,
            'claimedAt' => FieldValue::serverTimestamp(),
        ], Precondition::updateTime($message->updateTime));
        echo "won\n";
    } catch (FailedPreconditionException | NotFoundException) {
        echo "lost\n";
    }
    exit(0);
}

if ($mode === 'increment') {
    $waitForStart();
    $runs = 0;
    for ($i = 0; $i < (int) $arg; $i++) {
        $fs->runTransaction(static function (Transaction $tx) use ($path, &$runs): void {
            $runs++;
            $counter = $tx->get($path);
            $n = $counter?->get('n');
            $tx->set($path, ['n' => (is_int($n) ? $n : 0) + 1]);
        });
    }
    echo $runs, "\n";
    exit(0);
}

echo "unknown mode\n";
exit(2);
