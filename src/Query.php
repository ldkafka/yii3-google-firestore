<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore;

use Generator;
use InvalidArgumentException;
use Ldkafka\GoogleFirestore\Internal\ValueCodec;
use stdClass;

use function array_map;
use function count;
use function is_array;
use function is_float;
use function is_nan;
use function is_numeric;
use function min;
use function strtoupper;

/**
 * An immutable query on one collection or a collection group: every call returns a changed copy.
 * Firestore needs an index for most queries that combine filters and orders; the emulator doesn't.
 */
final class Query
{
    public const NAME_FIELD = '__name__';

    private const OPERATORS = [
        '==' => 'EQUAL',
        '!=' => 'NOT_EQUAL',
        '<' => 'LESS_THAN',
        '<=' => 'LESS_THAN_OR_EQUAL',
        '>' => 'GREATER_THAN',
        '>=' => 'GREATER_THAN_OR_EQUAL',
        'array-contains' => 'ARRAY_CONTAINS',
        'array-contains-any' => 'ARRAY_CONTAINS_ANY',
        'in' => 'IN',
        'not-in' => 'NOT_IN',
    ];
    private const DIRECTIONS = ['ASC' => 'ASCENDING', 'DESC' => 'DESCENDING'];
    private const COUNT_ALIAS = 'count';

    /** @var list<array{field: string, op: string, value: mixed}> */
    private array $filters = [];

    /** @var list<array{field: string, direction: string}> */
    private array $orders = [];

    private ?int $limit = null;

    private ?int $offset = null;

    /** @var array{cursor: Document|list<mixed>, before: bool}|null */
    private ?array $start = null;

    /** @var array{cursor: Document|list<mixed>, before: bool}|null */
    private ?array $end = null;

    /**
     * @internal Use FirestoreClient::query() or collectionGroup().
     * @param string $parentPath '' for root collections, or the parent document's path.
     */
    public function __construct(
        private FirestoreClient $client,
        private string $parentPath,
        private string $collectionId,
        private bool $allDescendants,
    ) {
    }

    /**
     * Add a filter; several are ANDed. $op: == != < <= > >= array-contains array-contains-any in
     * not-in. A null or NAN value with == or != becomes the IS_NULL / IS_NOT_NULL / IS_NAN /
     * IS_NOT_NAN filter.
     */
    public function where(string $fieldPath, string $op, mixed $value): self
    {
        if (!isset(self::OPERATORS[$op])) {
            throw new InvalidArgumentException('Unknown query operator: ' . $op);
        }
        if ($value instanceof FieldValue) {
            throw new InvalidArgumentException('A FieldValue sentinel cannot be a query value.');
        }
        ValueCodec::quoteFieldPath($fieldPath);
        $clone = clone $this;
        $clone->filters[] = ['field' => $fieldPath, 'op' => $op, 'value' => $value];

        return $clone;
    }

    /**
     * Order by a field, 'ASC' or 'DESC'.
     */
    public function orderBy(string $fieldPath, string $direction = 'ASC'): self
    {
        $wire = self::DIRECTIONS[strtoupper($direction)] ?? null;
        if ($wire === null) {
            throw new InvalidArgumentException('An order direction is ASC or DESC.');
        }
        ValueCodec::quoteFieldPath($fieldPath);
        $clone = clone $this;
        $clone->orders[] = ['field' => $fieldPath, 'direction' => $wire];

        return $clone;
    }

    /**
     * Return at most this many documents.
     */
    public function limit(int $limit): self
    {
        if ($limit < 0) {
            throw new InvalidArgumentException('A limit is not negative.');
        }
        $clone = clone $this;
        $clone->limit = $limit;

        return $clone;
    }

    /**
     * Skip this many documents. Firestore still reads (and bills) them: page with cursors instead.
     */
    public function offset(int $offset): self
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('An offset is not negative.');
        }
        $clone = clone $this;
        $clone->offset = $offset;

        return $clone;
    }

    /**
     * Start at a document, or at these values of the orderBy fields (inclusive).
     *
     * @param Document|list<mixed> $cursor
     */
    public function startAt(Document|array $cursor): self
    {
        return $this->withCursor('start', $cursor, true);
    }

    /**
     * Start right after a document, or after these values of the orderBy fields.
     *
     * @param Document|list<mixed> $cursor
     */
    public function startAfter(Document|array $cursor): self
    {
        return $this->withCursor('start', $cursor, false);
    }

    /**
     * End at a document, or at these values of the orderBy fields (inclusive).
     *
     * @param Document|list<mixed> $cursor
     */
    public function endAt(Document|array $cursor): self
    {
        return $this->withCursor('end', $cursor, false);
    }

    /**
     * End right before a document, or before these values of the orderBy fields.
     *
     * @param Document|list<mixed> $cursor
     */
    public function endBefore(Document|array $cursor): self
    {
        return $this->withCursor('end', $cursor, true);
    }

    /**
     * Run the query.
     *
     * @return list<Document>
     *
     * @phpstan-impure
     */
    public function get(): array
    {
        return $this->run(null);
    }

    /**
     * Every match, one page at a time through cursors (each page is the query again, after the last
     * document of the page before). Stops at limit() if one was set.
     *
     * @return Generator<int, Document>
     */
    public function stream(int $pageSize = 300): Generator
    {
        if ($pageSize < 1) {
            throw new InvalidArgumentException('A page size is at least 1.');
        }
        $query = $this->withNameOrder();
        $remaining = $this->limit;
        while (true) {
            $size = $remaining === null ? $pageSize : min($pageSize, $remaining);
            if ($size === 0) {
                return;
            }
            $page = $query->limit($size)->get();
            foreach ($page as $document) {
                yield $document;
            }
            if ($remaining !== null) {
                $remaining -= count($page);
            }
            if (count($page) < $size) {
                return;
            }
            $query = $query->startAfter($page[count($page) - 1]);
            $query->offset = null;
        }
    }

    /**
     * The number of matching documents (a count aggregation; no documents are sent).
     *
     * @phpstan-impure
     */
    public function count(): int
    {
        $body = ['structuredAggregationQuery' => [
            'structuredQuery' => $this->toArray($this->client->codec()),
            'aggregations' => [['alias' => self::COUNT_ALIAS, 'count' => new stdClass()]],
        ]];
        foreach ($this->client->call('runAggregationQuery', $this->parentPath, $body, FirestoreClient::CALL_READ) as $item) {
            $fields = is_array($item) && is_array($item['result'] ?? null) ? $item['result']['aggregateFields'] ?? null : null;
            if (is_array($fields) && isset($fields[self::COUNT_ALIAS])) {
                $count = $this->client->codec()->decode($fields[self::COUNT_ALIAS]);

                return is_numeric($count) ? (int) $count : 0;
            }
        }

        return 0;
    }

    /**
     * Run the query, optionally inside a transaction.
     *
     * @internal
     * @return list<Document>
     */
    public function run(?string $transaction): array
    {
        $codec = $this->client->codec();
        $body = ['structuredQuery' => $this->toArray($codec)];
        if ($transaction !== null) {
            $body['transaction'] = $transaction;
        }
        $documents = [];
        foreach ($this->client->call('runQuery', $this->parentPath, $body, FirestoreClient::CALL_READ) as $item) {
            if (is_array($item) && isset($item['document'])) {
                $documents[] = $codec->decodeDocument($item['document']);
            }
        }

        return $documents;
    }

    /**
     * The REST structuredQuery object.
     *
     * @internal
     * @return array<string, mixed>
     */
    public function toArray(ValueCodec $codec): array
    {
        $orders = $this->orders;
        if ($this->start !== null && $this->start['cursor'] instanceof Document
            || $this->end !== null && $this->end['cursor'] instanceof Document) {
            $orders = $this->withNameOrder()->orders;
        }

        $query = ['from' => [['collectionId' => $this->collectionId] + ($this->allDescendants ? ['allDescendants' => true] : [])]];
        $filters = array_map(fn (array $f): array => $this->filter($codec, $f['field'], $f['op'], $f['value']), $this->filters);
        if (count($filters) === 1) {
            $query['where'] = $filters[0];
        } elseif ($filters !== []) {
            $query['where'] = ['compositeFilter' => ['op' => 'AND', 'filters' => $filters]];
        }
        if ($orders !== []) {
            $query['orderBy'] = array_map(
                static fn (array $o): array => ['field' => ['fieldPath' => ValueCodec::quoteFieldPath($o['field'])], 'direction' => $o['direction']],
                $orders,
            );
        }
        if ($this->start !== null) {
            $query['startAt'] = $this->cursor($codec, $this->start, $orders);
        }
        if ($this->end !== null) {
            $query['endAt'] = $this->cursor($codec, $this->end, $orders);
        }
        if ($this->offset !== null) {
            $query['offset'] = $this->offset;
        }
        if ($this->limit !== null) {
            $query['limit'] = $this->limit;
        }

        return $query;
    }

    /**
     * @param Document|list<mixed> $cursor
     */
    private function withCursor(string $which, Document|array $cursor, bool $before): self
    {
        if (is_array($cursor) && $cursor === []) {
            throw new InvalidArgumentException('A cursor needs at least one value.');
        }
        $clone = clone $this;
        if ($which === 'start') {
            $clone->start = ['cursor' => $cursor, 'before' => $before];
        } else {
            $clone->end = ['cursor' => $cursor, 'before' => $before];
        }

        return $clone;
    }

    private function withNameOrder(): self
    {
        foreach ($this->orders as $order) {
            if ($order['field'] === self::NAME_FIELD) {
                return $this;
            }
        }
        $last = $this->orders === [] ? 'ASCENDING' : $this->orders[count($this->orders) - 1]['direction'];
        $clone = clone $this;
        $clone->orders[] = ['field' => self::NAME_FIELD, 'direction' => $last];

        return $clone;
    }

    /**
     * @return array<string, mixed>
     */
    private function filter(ValueCodec $codec, string $field, string $op, mixed $value): array
    {
        $fieldRef = ['fieldPath' => ValueCodec::quoteFieldPath($field)];
        if ($op === '==' || $op === '!=') {
            $unary = match (true) {
                $value === null => $op === '==' ? 'IS_NULL' : 'IS_NOT_NULL',
                is_float($value) && is_nan($value) => $op === '==' ? 'IS_NAN' : 'IS_NOT_NAN',
                default => null,
            };
            if ($unary !== null) {
                return ['unaryFilter' => ['op' => $unary, 'field' => $fieldRef]];
            }
        }

        return ['fieldFilter' => ['field' => $fieldRef, 'op' => self::OPERATORS[$op], 'value' => $codec->encode($value)]];
    }

    /**
     * @param array{cursor: Document|list<mixed>, before: bool} $cursor
     * @param list<array{field: string, direction: string}> $orders
     * @return array{values: list<array<string, mixed>>, before: bool}
     */
    private function cursor(ValueCodec $codec, array $cursor, array $orders): array
    {
        $document = $cursor['cursor'];
        if (!$document instanceof Document) {
            return ['values' => array_map($codec->encode(...), $document), 'before' => $cursor['before']];
        }
        $values = [];
        foreach ($orders as $order) {
            $values[] = $order['field'] === self::NAME_FIELD
                ? ['referenceValue' => $codec->documentName($document->path)]
                : $codec->encode($document->get($order['field']));
        }

        return ['values' => $values, 'before' => $cursor['before']];
    }
}
