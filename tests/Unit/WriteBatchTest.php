<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Unit;

use Ldkafka\GoogleFirestore\FieldValue;
use Ldkafka\GoogleFirestore\FirestoreClient;
use Ldkafka\GoogleFirestore\Precondition;
use Ldkafka\GoogleFirestore\Tests\Support\QueueHttpClient;
use Ldkafka\GoogleFirestore\Value\Reference;
use Ldkafka\GoogleFirestore\Value\Timestamp;
use PHPUnit\Framework\TestCase;

use function json_encode;
use function preg_match;

/**
 * The Write JSON of every verb, transforms and field paths, and the commit results.
 */
final class WriteBatchTest extends TestCase
{
    private const ROOT = 'projects/demo-p/databases/(default)/documents';

    private QueueHttpClient $http;

    private FirestoreClient $fs;

    protected function setUp(): void
    {
        $this->http = new QueueHttpClient();
        $this->fs = $this->http->client();
    }

    public function testCreateWithAutoIdMustNotExist(): void
    {
        $this->http->json(200, ['writeResults' => [['updateTime' => '2026-10-16T13:21:07.221777Z']], 'commitTime' => '2026-10-16T13:21:07.221777Z']);

        $result = $this->fs->create('messages', ['to' => 'infra']);

        self::assertSame(1, preg_match('#^messages/[A-Za-z0-9]{20}$#', $result->path));
        self::assertSame('2026-10-16T13:21:07.221777Z', (string) $result->updateTime);
        self::assertStringEndsWith('/v1/' . self::ROOT . ':commit', $this->http->path(0));
        self::assertSame(json_encode(['writes' => [[
            'update' => ['name' => self::ROOT . '/' . $result->path, 'fields' => ['to' => ['stringValue' => 'infra']]],
            'currentDocument' => ['exists' => false],
        ]]], JSON_UNESCAPED_SLASHES), (string) $this->http->requests[0]->getBody());
    }

    public function testSetReplacesAndMergeMasksTheLeaves(): void
    {
        $batch = $this->fs->batch();
        $batch->set('c/a', ['x' => 1]);
        $batch->set('c/b', ['x' => 1, 'meta' => ['c' => 'z', 'x.y' => 'dot', 'l' => [1]], 'e' => []], true);
        $this->http->json(200, ['writeResults' => [['updateTime' => '2026-10-16T13:21:07Z'], ['updateTime' => '2026-10-16T13:21:07Z']]]);
        $batch->commit();

        $writes = QueueHttpClient::dig($this->http->body(0), 'writes');
        self::assertSame(['update' => ['name' => self::ROOT . '/c/a', 'fields' => ['x' => ['integerValue' => '1']]]], QueueHttpClient::dig($writes, 0));
        self::assertSame(['fieldPaths' => ['x', 'meta.c', 'meta.`x.y`', 'meta.l', 'e']], QueueHttpClient::dig($writes, 1, 'updateMask'));
        self::assertNull(QueueHttpClient::dig($writes, 1, 'currentDocument'), 'set sends no precondition');
    }

    public function testUpdateMasksTheKeysAndMustExist(): void
    {
        $this->http->json(200, ['writeResults' => [['updateTime' => '2026-10-16T13:21:07Z']]]);

        $this->fs->update('c/a', ['status' => 'claimed', 'meta.claimant' => 'infra', 'a-b.c' => 1, 'm' => ['k' => 'v']]);

        self::assertSame(json_encode(['writes' => [[
            'update' => ['name' => self::ROOT . '/c/a', 'fields' => [
                'status' => ['stringValue' => 'claimed'],
                'meta' => ['mapValue' => ['fields' => ['claimant' => ['stringValue' => 'infra']]]],
                'a-b' => ['mapValue' => ['fields' => ['c' => ['integerValue' => '1']]]],
                'm' => ['mapValue' => ['fields' => ['k' => ['stringValue' => 'v']]]],
            ]],
            'updateMask' => ['fieldPaths' => ['status', 'meta.claimant', '`a-b`.c', 'm']],
            'currentDocument' => ['exists' => true],
        ]]], JSON_UNESCAPED_SLASHES), (string) $this->http->requests[0]->getBody());
    }

    public function testPreconditions(): void
    {
        $batch = $this->fs->batch();
        $batch->update('c/a', ['s' => 1], Precondition::updateTime(Timestamp::parse('2026-10-16T13:21:52.070602Z')));
        $batch->update('c/b', ['s' => 1], Precondition::exists(false));
        $batch->delete('c/c');
        $batch->delete('c/d', Precondition::exists());
        $this->http->json(200, ['writeResults' => [[], [], ['updateTime' => '2026-10-16T13:21:07Z'], []]]);

        $results = $batch->commit();

        $writes = QueueHttpClient::dig($this->http->body(0), 'writes');
        self::assertSame(['updateTime' => '2026-10-16T13:21:52.070602Z'], QueueHttpClient::dig($writes, 0, 'currentDocument'));
        self::assertSame(['exists' => false], QueueHttpClient::dig($writes, 1, 'currentDocument'));
        self::assertSame(['delete' => self::ROOT . '/c/c'], QueueHttpClient::dig($writes, 2));
        self::assertSame(['delete' => self::ROOT . '/c/d', 'currentDocument' => ['exists' => true]], QueueHttpClient::dig($writes, 3));
        self::assertCount(4, $results);
        self::assertSame('c/c', $results[2]->path);
        self::assertNull($results[2]->updateTime, 'a delete has no new version');
    }

    public function testTransformsLeaveFieldsAndMask(): void
    {
        $this->http->json(200, ['writeResults' => [[
            'updateTime' => '2026-10-16T13:21:52.133664Z',
            'transformResults' => [
                ['timestampValue' => '2026-10-16T13:21:52.129Z'],
                ['integerValue' => '8'],
                ['nullValue' => null],
                ['nullValue' => null],
                ['timestampValue' => '2026-10-16T13:21:52.129Z'],
            ],
        ]]]);

        $result = $this->fs->update('messages/m1', [
            'status' => 'done',
            'doneAt' => FieldValue::serverTimestamp(),
            'attempts' => FieldValue::increment(1),
            'labels' => FieldValue::arrayUnion('infra', 'urgent'),
            'watchers' => FieldValue::arrayRemove('steward'),
            'meta' => ['at' => FieldValue::serverTimestamp()],
        ]);

        $write = QueueHttpClient::dig($this->http->body(0), 'writes', 0);
        self::assertSame(['status' => ['stringValue' => 'done']], QueueHttpClient::dig($write, 'update', 'fields'));
        self::assertSame(['fieldPaths' => ['status']], QueueHttpClient::dig($write, 'updateMask'));
        self::assertSame([
            ['fieldPath' => 'doneAt', 'setToServerValue' => 'REQUEST_TIME'],
            ['fieldPath' => 'attempts', 'increment' => ['integerValue' => '1']],
            ['fieldPath' => 'labels', 'appendMissingElements' => ['values' => [['stringValue' => 'infra'], ['stringValue' => 'urgent']]]],
            ['fieldPath' => 'watchers', 'removeAllFromArray' => ['values' => [['stringValue' => 'steward']]]],
            ['fieldPath' => 'meta.at', 'setToServerValue' => 'REQUEST_TIME'],
        ], QueueHttpClient::dig($write, 'updateTransforms'));
        self::assertEquals(Timestamp::parse('2026-10-16T13:21:52.129Z'), $result->transformResults['doneAt']);
        self::assertSame(8, $result->transformResults['attempts']);
        self::assertArrayHasKey('labels', $result->transformResults);
        self::assertNull($result->transformResults['labels']);
        self::assertArrayHasKey('meta.at', $result->transformResults);
    }

    public function testATransformOnlyUpdateSendsAnEmptyMask(): void
    {
        $this->http->json(200, ['writeResults' => [['updateTime' => '2026-10-16T13:21:52Z']]]);

        $this->fs->update('c/a', ['n' => FieldValue::increment(2.5)]);

        self::assertSame(
            '{"writes":[{"update":{"name":"' . self::ROOT . '/c/a","fields":{}},"updateMask":{"fieldPaths":[]},'
            . '"updateTransforms":[{"fieldPath":"n","increment":{"doubleValue":2.5}}],"currentDocument":{"exists":true}}]}',
            (string) $this->http->requests[0]->getBody(),
        );
    }

    public function testSentinelsInCreateAndSetAreTransformsToo(): void
    {
        $batch = $this->fs->batch();
        $path = $batch->create('messages', ['last' => new Reference('threads/t1'), 'at' => FieldValue::serverTimestamp()], 'given-id');
        $batch->set('c/a', ['m' => ['at' => FieldValue::serverTimestamp()], 'x' => 1], true);
        $this->http->json(200, ['writeResults' => [['updateTime' => '2026-10-16T13:21:52Z'], ['updateTime' => '2026-10-16T13:21:52Z']]]);
        $batch->commit();

        self::assertSame('messages/given-id', $path);
        $writes = QueueHttpClient::dig($this->http->body(0), 'writes');
        self::assertSame(['last' => ['referenceValue' => self::ROOT . '/threads/t1']], QueueHttpClient::dig($writes, 0, 'update', 'fields'));
        self::assertSame([['fieldPath' => 'at', 'setToServerValue' => 'REQUEST_TIME']], QueueHttpClient::dig($writes, 0, 'updateTransforms'));
        self::assertSame(['fieldPaths' => ['x']], QueueHttpClient::dig($writes, 1, 'updateMask'), 'a map left empty by its sentinels is dropped');
        self::assertSame([['fieldPath' => 'm.at', 'setToServerValue' => 'REQUEST_TIME']], QueueHttpClient::dig($writes, 1, 'updateTransforms'));
    }

    public function testBadPathsAreRefused(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->fs->batch()->set('c', ['x' => 1]);
    }
}
