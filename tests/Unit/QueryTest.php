<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Unit;

use InvalidArgumentException;
use Ldkafka\GoogleFirestore\Document;
use Ldkafka\GoogleFirestore\FieldValue;
use Ldkafka\GoogleFirestore\FirestoreClient;
use Ldkafka\GoogleFirestore\Tests\Support\QueueHttpClient;
use Ldkafka\GoogleFirestore\Value\Timestamp;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;
use function json_encode;

/**
 * The structuredQuery JSON, cursors, pagination and count.
 */
final class QueryTest extends TestCase
{
    private const ROOT = 'projects/demo-p/databases/(default)/documents';

    private QueueHttpClient $http;

    private FirestoreClient $fs;

    protected function setUp(): void
    {
        $this->http = new QueueHttpClient();
        $this->fs = $this->http->client();
    }

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function operators(): iterable
    {
        yield '==' => ['==', 'EQUAL'];
        yield '!=' => ['!=', 'NOT_EQUAL'];
        yield '<' => ['<', 'LESS_THAN'];
        yield '<=' => ['<=', 'LESS_THAN_OR_EQUAL'];
        yield '>' => ['>', 'GREATER_THAN'];
        yield '>=' => ['>=', 'GREATER_THAN_OR_EQUAL'];
        yield 'array-contains' => ['array-contains', 'ARRAY_CONTAINS'];
        yield 'array-contains-any' => ['array-contains-any', 'ARRAY_CONTAINS_ANY'];
        yield 'in' => ['in', 'IN'];
        yield 'not-in' => ['not-in', 'NOT_IN'];
    }

    #[DataProvider('operators')]
    public function testEveryOperator(string $op, string $wire): void
    {
        $query = $this->fs->query('messages')->where('to', $op, 'infra')->toArray($this->fs->codec());

        self::assertSame([
            'from' => [['collectionId' => 'messages']],
            'where' => ['fieldFilter' => ['field' => ['fieldPath' => 'to'], 'op' => $wire, 'value' => ['stringValue' => 'infra']]],
        ], $query);
    }

    public function testNullAndNanBecomeUnaryFilters(): void
    {
        $codec = $this->fs->codec();
        $q = $this->fs->query('m');

        self::assertSame(['unaryFilter' => ['op' => 'IS_NULL', 'field' => ['fieldPath' => 'a']]], $q->where('a', '==', null)->toArray($codec)['where']);
        self::assertSame(['unaryFilter' => ['op' => 'IS_NOT_NULL', 'field' => ['fieldPath' => 'a']]], $q->where('a', '!=', null)->toArray($codec)['where']);
        self::assertSame(['unaryFilter' => ['op' => 'IS_NAN', 'field' => ['fieldPath' => 'a']]], $q->where('a', '==', NAN)->toArray($codec)['where']);
        self::assertSame(['unaryFilter' => ['op' => 'IS_NOT_NAN', 'field' => ['fieldPath' => 'a']]], $q->where('a', '!=', NAN)->toArray($codec)['where']);
    }

    public function testFiltersAreAndedAndOrdersLimitOffsetApply(): void
    {
        $base = $this->fs->query('messages');
        $query = $base
            ->where('to', '==', 'infra')
            ->where('status', 'in', ['new', 'seen'])
            ->orderBy('createdAt', 'desc')
            ->orderBy('a-b')
            ->offset(5)
            ->limit(20);

        self::assertSame(json_encode([
            'from' => [['collectionId' => 'messages']],
            'where' => ['compositeFilter' => ['op' => 'AND', 'filters' => [
                ['fieldFilter' => ['field' => ['fieldPath' => 'to'], 'op' => 'EQUAL', 'value' => ['stringValue' => 'infra']]],
                ['fieldFilter' => ['field' => ['fieldPath' => 'status'], 'op' => 'IN', 'value' => ['arrayValue' => ['values' => [['stringValue' => 'new'], ['stringValue' => 'seen']]]]]],
            ]]],
            'orderBy' => [
                ['field' => ['fieldPath' => 'createdAt'], 'direction' => 'DESCENDING'],
                ['field' => ['fieldPath' => '`a-b`'], 'direction' => 'ASCENDING'],
            ],
            'offset' => 5,
            'limit' => 20,
        ]), json_encode($query->toArray($this->fs->codec())));
        self::assertSame(['from' => [['collectionId' => 'messages']]], $base->toArray($this->fs->codec()), 'the original query is unchanged');
    }

    public function testCursorsFromValues(): void
    {
        $codec = $this->fs->codec();
        $q = $this->fs->query('m')->orderBy('n');

        self::assertSame(['values' => [['integerValue' => '3']], 'before' => true], $q->startAt([3])->toArray($codec)['startAt']);
        self::assertSame(['values' => [['integerValue' => '3']], 'before' => false], $q->startAfter([3])->toArray($codec)['startAt']);
        self::assertSame(['values' => [['integerValue' => '9']], 'before' => false], $q->endAt([9])->toArray($codec)['endAt']);
        self::assertSame(['values' => [['integerValue' => '9']], 'before' => true], $q->endBefore([9])->toArray($codec)['endAt']);
        self::assertArrayNotHasKey('orderBy', $this->fs->query('m')->startAt([1])->toArray($codec), 'value cursors add no order');
    }

    public function testDocumentCursorAddsNameInTheLastDirection(): void
    {
        $doc = new Document('messages/m1', 'm1', ['createdAt' => new Timestamp(10), 'to' => 'infra'], new Timestamp(1), new Timestamp(2));
        $query = $this->fs->query('messages')->orderBy('createdAt', 'DESC')->startAfter($doc)->toArray($this->fs->codec());

        self::assertSame([
            ['field' => ['fieldPath' => 'createdAt'], 'direction' => 'DESCENDING'],
            ['field' => ['fieldPath' => '__name__'], 'direction' => 'DESCENDING'],
        ], $query['orderBy']);
        self::assertSame(['values' => [
            ['timestampValue' => '1970-01-01T00:00:10.000000Z'],
            ['referenceValue' => self::ROOT . '/messages/m1'],
        ], 'before' => false], $query['startAt']);

        $plain = $this->fs->query('messages')->endBefore($doc)->toArray($this->fs->codec());
        self::assertSame([['field' => ['fieldPath' => '__name__'], 'direction' => 'ASCENDING']], $plain['orderBy']);
    }

    public function testCollectionGroupAndSubcollection(): void
    {
        self::assertSame(
            ['from' => [['collectionId' => 'replies', 'allDescendants' => true]]],
            $this->fs->collectionGroup('replies')->toArray($this->fs->codec()),
        );

        $this->http->json(200, [['readTime' => '2026-10-16T13:21:25.328698Z', 'done' => true]]);
        self::assertSame([], $this->fs->query('threads/t 1/replies')->get());
        self::assertSame('/v1/' . self::ROOT . '/threads/t%201:runQuery', $this->http->path(0));
        self::assertSame(['from' => [['collectionId' => 'replies']]], QueueHttpClient::dig($this->http->body(0), 'structuredQuery'));
    }

    public function testGetDecodesTheStream(): void
    {
        $this->http->json(200, [
            ['document' => self::doc('m/a', 1), 'readTime' => '2026-10-16T13:21:25.291061Z'],
            ['document' => self::doc('m/b', 2), 'readTime' => '2026-10-16T13:21:25.291061Z', 'done' => true],
        ]);

        $docs = $this->fs->query('m')->get();

        self::assertCount(2, $docs);
        self::assertSame('m/b', $docs[1]->path);
        self::assertSame(2, $docs[1]->get('n'));
    }

    public function testStreamPagesWithCursorsAndStopsAtTheLimit(): void
    {
        $this->http
            ->json(200, [['document' => self::doc('m/a', 1)], ['document' => self::doc('m/b', 2)]])
            ->json(200, [['document' => self::doc('m/c', 3)], ['document' => self::doc('m/d', 4)]])
            ->json(200, [['document' => self::doc('m/e', 5)]]);

        $paths = [];
        foreach ($this->fs->query('m')->orderBy('n')->offset(1)->limit(5)->stream(2) as $doc) {
            $paths[] = $doc->path;
        }

        self::assertSame(['m/a', 'm/b', 'm/c', 'm/d', 'm/e'], $paths);
        self::assertCount(3, $this->http->requests);
        self::assertSame(2, QueueHttpClient::dig($this->http->body(0), 'structuredQuery', 'limit'));
        self::assertSame(1, QueueHttpClient::dig($this->http->body(0), 'structuredQuery', 'offset'));
        self::assertNull(QueueHttpClient::dig($this->http->body(1), 'structuredQuery', 'offset'), 'offset only on the first page');
        self::assertSame(1, QueueHttpClient::dig($this->http->body(2), 'structuredQuery', 'limit'), 'the last page asks only for what the limit leaves');
        self::assertSame('__name__', QueueHttpClient::dig($this->http->body(0), 'structuredQuery', 'orderBy', 1, 'field', 'fieldPath'));
        self::assertSame(
            ['values' => [['integerValue' => '4'], ['referenceValue' => self::ROOT . '/m/d']], 'before' => false],
            QueueHttpClient::dig($this->http->body(2), 'structuredQuery', 'startAt'),
        );
    }

    public function testStreamEndsOnAShortPage(): void
    {
        $this->http->json(200, [['document' => self::doc('m/a', 1)], ['readTime' => '2026-10-16T13:21:25Z', 'done' => true]]);

        self::assertCount(1, iterator_to_array($this->fs->query('m')->stream(2), false));
        self::assertCount(1, $this->http->requests);
    }

    public function testCount(): void
    {
        $this->http->json(200, [['result' => ['aggregateFields' => ['count' => ['integerValue' => '3']]], 'readTime' => '2026-10-16T13:21:25Z', 'done' => true]]);

        self::assertSame(3, $this->fs->query('c')->where('status', '==', 'new')->count());
        self::assertStringEndsWith(':runAggregationQuery', $this->http->path(0));
        self::assertStringContainsString('"aggregations":[{"alias":"count","count":{}}]', (string) $this->http->requests[0]->getBody());
    }

    public function testBadArgumentsAreRefused(): void
    {
        $q = $this->fs->query('m');
        foreach ([
            static fn () => $q->where('a', '=', 1),
            static fn () => $q->where('a', '==', FieldValue::increment(1)),
            static fn () => $q->orderBy('a', 'UP'),
            static fn () => $q->limit(-1),
            static fn () => $q->startAt([]),
            fn () => $this->fs->query('m/doc'),
            fn () => $this->fs->collectionGroup('a/b'),
        ] as $i => $call) {
            try {
                $call();
                self::fail('expected InvalidArgumentException for case ' . $i);
            } catch (InvalidArgumentException) {
                self::addToAssertionCount(1);
            }
        }
    }

    /**
     * @return array<string, mixed>
     */
    private static function doc(string $path, int $n): array
    {
        return [
            'name' => self::ROOT . '/' . $path,
            'fields' => ['n' => ['integerValue' => (string) $n]],
            'createTime' => '2026-10-16T13:21:07.221777Z',
            'updateTime' => '2026-10-16T13:21:07.221777Z',
        ];
    }
}
