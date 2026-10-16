<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Integration;

use HttpSoft\Message\RequestFactory;
use HttpSoft\Message\StreamFactory;
use Ldkafka\GoogleFirestore\Exception\AlreadyExistsException;
use Ldkafka\GoogleFirestore\Exception\FailedPreconditionException;
use Ldkafka\GoogleFirestore\Exception\NotFoundException;
use Ldkafka\GoogleFirestore\FirestoreClient;
use Ldkafka\GoogleFirestore\Precondition;
use Ldkafka\GoogleFirestore\Tests\Support\EmulatorTestCase;
use Ldkafka\GoogleFirestore\Value\Timestamp;

/**
 * Create, get, update, delete and the precondition statuses, on the emulator.
 */
final class DocumentsTest extends EmulatorTestCase
{
    public function testCreateGetUpdateDelete(): void
    {
        $created = $this->fs->create('messages', ['to' => 'infra', 'status' => 'new', 'meta' => ['n' => 1]]);
        self::assertNotNull($created->updateTime);

        $doc = $this->fs->get($created->path);
        self::assertNotNull($doc);
        self::assertSame('infra', $doc->get('to'));
        self::assertSame(1, $doc->get('meta.n'));
        self::assertSame((string) $created->updateTime, (string) $doc->updateTime);

        $this->fs->update($created->path, ['status' => 'seen', 'meta.by' => 'steward']);
        $updated = $this->fs->get($created->path);
        self::assertNotNull($updated);
        self::assertSame(self::sorted(['to' => 'infra', 'status' => 'seen', 'meta' => ['n' => 1, 'by' => 'steward']]), self::sorted($updated->data));

        $this->fs->set($created->path, ['status' => 'done', 'meta' => ['n' => 2]], merge: true);
        $merged = $this->fs->get($created->path);
        self::assertSame(['by' => 'steward', 'n' => 2], self::sorted((array) $merged?->get('meta')));
        self::assertSame('infra', $merged?->get('to'));

        $this->fs->set($created->path, ['only' => true]);
        self::assertSame(['only' => true], $this->fs->get($created->path)?->data);

        $this->fs->delete($created->path);
        self::assertNull($this->fs->get($created->path));
    }

    public function testCreateOnAnExistingIdFails(): void
    {
        $this->fs->create('messages', ['a' => 1], 'm1');

        $this->expectException(AlreadyExistsException::class);
        $this->fs->create('messages', ['a' => 2], 'm1');
    }

    public function testUpdateOfAMissingDocumentFails(): void
    {
        $this->expectException(NotFoundException::class);
        $this->fs->update('messages/nobody', ['a' => 1]);
    }

    public function testPreconditionStatuses(): void
    {
        $this->fs->create('messages', ['a' => 1], 'm1');
        $stale = Timestamp::parse('2020-01-01T00:00:00.123456Z');

        try {
            $this->fs->update('messages/m1', ['a' => 2], Precondition::updateTime($stale));
            self::fail('expected FailedPreconditionException');
        } catch (FailedPreconditionException $e) {
            self::assertSame(400, $e->httpStatus);
        }
        try {
            $this->fs->delete('messages/missing', Precondition::exists());
            self::fail('expected NotFoundException');
        } catch (NotFoundException $e) {
            self::assertSame(404, $e->httpStatus);
        }
        try {
            $this->fs->set('messages/m1', ['a' => 3]);
            $this->fs->update('messages/m1', ['a' => 4], Precondition::exists(false));
            self::fail('expected AlreadyExistsException');
        } catch (AlreadyExistsException $e) {
            self::assertSame(409, $e->httpStatus);
        }

        $current = $this->fs->get('messages/m1');
        self::assertNotNull($current);
        $this->fs->update('messages/m1', ['a' => 5], Precondition::updateTime($current->updateTime));
        self::assertSame(5, $this->fs->get('messages/m1')?->get('a'));
    }

    public function testGetAllAndOddIds(): void
    {
        $this->fs->create('c', ['n' => 1], 'a b é');
        $this->fs->create('c', ['n' => 2], 'x:y');

        $docs = $this->fs->getAll(['c/x:y', 'c/none', 'c/a b é']);

        self::assertSame(['c/x:y', 'c/none', 'c/a b é'], array_keys($docs));
        self::assertSame(2, $docs['c/x:y']?->get('n'));
        self::assertNull($docs['c/none']);
        self::assertSame('a b é', $docs['c/a b é']?->id);
    }

    public function testANamedDatabaseIsSeparate(): void
    {
        $named = new FirestoreClient(self::http(), new RequestFactory(), new StreamFactory(), null, self::PROJECT, 'bus-db', $this->host);
        $named->set('c/a', ['db' => 'named']);
        $this->fs->set('c/a', ['db' => 'default']);

        self::assertSame('named', $named->get('c/a')?->get('db'));
        self::assertSame('default', $this->fs->get('c/a')?->get('db'));
        $named->delete('c/a');
    }

    /**
     * @param array<mixed> $data
     * @return array<mixed>
     */
    private static function sorted(array $data): array
    {
        ksort($data);
        foreach ($data as $key => $value) {
            if (is_array($value)) {
                $data[$key] = self::sorted($value);
            }
        }

        return $data;
    }
}
