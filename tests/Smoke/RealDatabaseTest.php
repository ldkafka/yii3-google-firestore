<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Smoke;

use HttpSoft\Message\RequestFactory;
use HttpSoft\Message\StreamFactory;
use Ldkafka\GoogleFirestore\Auth\ServiceAccountCredentials;
use Ldkafka\GoogleFirestore\Exception\AlreadyExistsException;
use Ldkafka\GoogleFirestore\Exception\FailedPreconditionException;
use Ldkafka\GoogleFirestore\Exception\NotFoundException;
use Ldkafka\GoogleFirestore\FirestoreClient;
use Ldkafka\GoogleFirestore\Precondition;
use Ldkafka\GoogleFirestore\Tests\Support\EmulatorTestCase;
use Ldkafka\GoogleFirestore\Transaction;
use Ldkafka\GoogleFirestore\Value\Timestamp;
use PHPUnit\Framework\TestCase;

use function bin2hex;
use function getenv;
use function is_int;
use function is_string;
use function random_bytes;

/**
 * Against a real database, in a scratch collection that is emptied afterwards. Opt-in:
 * FIRESTORE_SMOKE=1, FIRESTORE_SMOKE_KEY=<key file>, optionally FIRESTORE_SMOKE_DATABASE.
 */
final class RealDatabaseTest extends TestCase
{
    private FirestoreClient $fs;

    private string $collection = '';

    /** @var list<string> */
    private array $created = [];

    protected function setUp(): void
    {
        $key = getenv('FIRESTORE_SMOKE_KEY');
        if (getenv('FIRESTORE_SMOKE') !== '1' || !is_string($key) || $key === '') {
            self::markTestSkipped('Set FIRESTORE_SMOKE=1 and FIRESTORE_SMOKE_KEY to run against a real database.');
        }
        $database = getenv('FIRESTORE_SMOKE_DATABASE');
        $http = EmulatorTestCase::http();
        $this->fs = new FirestoreClient(
            $http,
            new RequestFactory(),
            new StreamFactory(),
            new ServiceAccountCredentials($key, $http, new RequestFactory(), new StreamFactory()),
            databaseId: is_string($database) && $database !== '' ? $database : FirestoreClient::DEFAULT_DATABASE,
        );
        $this->collection = 'smoke-' . bin2hex(random_bytes(4));
    }

    protected function tearDown(): void
    {
        foreach ($this->created as $path) {
            $this->fs->delete($path);
        }
    }

    public function testCreateGetQueryDelete(): void
    {
        $result = $this->fs->create($this->collection, ['to' => 'infra', 'status' => 'new']);
        $this->created[] = $result->path;

        self::assertSame('infra', $this->fs->get($result->path)?->get('to'));
        self::assertCount(1, $this->fs->query($this->collection)->where('to', '==', 'infra')->get());
        self::assertSame(1, $this->fs->query($this->collection)->count());

        $this->fs->delete($result->path);
        self::assertNull($this->fs->get($result->path));
    }

    public function testAReadWriteTransaction(): void
    {
        $path = $this->collection . '/counter';
        $this->created[] = $path;
        $this->fs->set($path, ['n' => 1]);

        $n = $this->fs->runTransaction(static function (Transaction $tx) use ($path): int {
            $n = $tx->get($path)?->get('n');
            $next = (is_int($n) ? $n : 0) + 1;
            $tx->set($path, ['n' => $next]);

            return $next;
        });

        self::assertSame(2, $n);
        self::assertSame(2, $this->fs->get($path)?->get('n'));
    }

    public function testThePreconditionStatuses(): void
    {
        $path = $this->collection . '/m1';
        $this->created[] = $path;
        $this->fs->create($this->collection, ['a' => 1], 'm1');

        $this->assertThrows(FailedPreconditionException::class, fn () => $this->fs->update($path, ['a' => 2], Precondition::updateTime(Timestamp::parse('2020-01-01T00:00:00Z'))));
        $this->assertThrows(AlreadyExistsException::class, fn () => $this->fs->create($this->collection, ['a' => 3], 'm1'));
        $this->assertThrows(NotFoundException::class, fn () => $this->fs->update($this->collection . '/missing', ['a' => 4]));
    }

    /**
     * @param class-string<\Throwable> $class
     */
    private function assertThrows(string $class, callable $call): void
    {
        try {
            $call();
            self::fail('expected ' . $class);
        } catch (\Throwable $e) {
            self::assertInstanceOf($class, $e);
        }
    }
}
