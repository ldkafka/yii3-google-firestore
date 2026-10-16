<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore;

use InvalidArgumentException;
use Ldkafka\GoogleFirestore\Internal\ValueCodec;
use Ldkafka\GoogleFirestore\Value\Timestamp;

use function array_is_list;
use function array_map;
use function count;
use function explode;
use function implode;
use function is_array;
use function is_string;
use function random_int;
use function strlen;

/**
 * Buffered writes that commit together, all or nothing, in order. The server enforces the limits.
 */
final class WriteBatch
{
    private const ID_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    private const ID_LENGTH = 20;

    /** @var list<array<string, mixed>> REST Write objects. */
    private array $writes = [];

    /** @var list<array{path: string, delete: bool, transforms: list<string>}> */
    private array $meta = [];

    /**
     * @internal Use FirestoreClient::batch().
     */
    public function __construct(private FirestoreClient $client)
    {
    }

    /**
     * Create a document (it must not exist yet).
     *
     * @param array<string, mixed> $data Field values; FieldValue sentinels allowed.
     * @param string|null $id Null = a random 20-character id, made here so it is known before the commit.
     * @return string The new document's path.
     */
    public function create(string $collection, array $data, ?string $id = null): string
    {
        $path = $collection . '/' . ($id ?? self::autoId());
        $transforms = [];
        $fields = self::extract($data, [], $transforms);
        $this->add($path, ['update' => $this->document($path, $fields)], $transforms, ['exists' => false]);

        return $path;
    }

    /**
     * Replace a document, or with $merge write only the given (nested) fields. Creates it if missing.
     *
     * @param array<string, mixed> $data Field values; FieldValue sentinels allowed.
     */
    public function set(string $path, array $data, bool $merge = false): void
    {
        $transforms = [];
        $fields = self::extract($data, [], $transforms);
        $write = ['update' => $this->document($path, $fields)];
        if ($merge) {
            $write['updateMask'] = ['fieldPaths' => array_map(ValueCodec::quoteSegments(...), self::leafPaths($fields, []))];
        }
        $this->add($path, $write, $transforms, null);
    }

    /**
     * Update the named fields only. Keys are dotted field paths; the update mask is the keys.
     * Without a precondition the document must exist.
     *
     * @param array<string, mixed> $fields Dotted field path => value; FieldValue sentinels allowed.
     */
    public function update(string $path, array $fields, ?Precondition $precondition = null): void
    {
        $nested = [];
        $mask = [];
        $transforms = [];
        foreach ($fields as $fieldPath => $value) {
            $segments = explode('.', (string) $fieldPath);
            if ($value instanceof FieldValue) {
                $transforms[] = [$segments, $value];
                continue;
            }
            if (is_array($value) && $value !== [] && !array_is_list($value)) {
                $value = self::extract($value, $segments, $transforms);
                if ($value === []) {
                    continue;
                }
            }
            $mask[] = ValueCodec::quoteSegments($segments);
            self::place($nested, $segments, $value);
        }
        $write = ['update' => $this->document($path, $nested), 'updateMask' => ['fieldPaths' => $mask]];
        $this->add($path, $write, $transforms, ($precondition ?? Precondition::exists())->toArray());
    }

    /**
     * Delete a document.
     */
    public function delete(string $path, ?Precondition $precondition = null): void
    {
        FirestoreClient::documentSegments($path);
        $this->writes[] = ['delete' => $this->client->codec()->documentName($path)]
            + ($precondition !== null ? ['currentDocument' => $precondition->toArray()] : []);
        $this->meta[] = ['path' => $path, 'delete' => true, 'transforms' => []];
    }

    /**
     * Commit every buffered write atomically.
     *
     * @return list<WriteResult> One per write, in order.
     */
    public function commit(): array
    {
        return $this->commitIn(null);
    }

    /**
     * Commit, ending the given transaction when one is passed.
     *
     * @internal
     * @return list<WriteResult>
     */
    public function commitIn(?string $transaction): array
    {
        $answer = $this->client->commit($this->writes, $transaction);
        $codec = $this->client->codec();
        $results = is_array($answer['writeResults'] ?? null) ? $answer['writeResults'] : [];
        $out = [];
        foreach ($this->meta as $i => $meta) {
            $result = is_array($results[$i] ?? null) ? $results[$i] : [];
            $updateTime = $result['updateTime'] ?? null;
            $values = is_array($result['transformResults'] ?? null) ? $result['transformResults'] : [];
            $transformResults = [];
            foreach ($meta['transforms'] as $j => $fieldPath) {
                $transformResults[$fieldPath] = isset($values[$j]) ? $codec->decode($values[$j]) : null;
            }
            $out[] = new WriteResult(
                $meta['path'],
                !$meta['delete'] && is_string($updateTime) ? Timestamp::parse($updateTime) : null,
                $transformResults,
            );
        }

        return $out;
    }

    /**
     * @param array<string, mixed> $write
     * @param list<array{0: list<string>, 1: FieldValue}> $transforms
     * @param array<string, mixed>|null $currentDocument
     */
    private function add(string $path, array $write, array $transforms, ?array $currentDocument): void
    {
        $codec = $this->client->codec();
        if ($transforms !== []) {
            $write['updateTransforms'] = array_map(
                static fn (array $t): array => self::transform($codec, $t[0], $t[1]),
                $transforms,
            );
        }
        if ($currentDocument !== null) {
            $write['currentDocument'] = $currentDocument;
        }
        $this->writes[] = $write;
        $this->meta[] = [
            'path' => $path,
            'delete' => false,
            'transforms' => array_map(static fn (array $t): string => implode('.', $t[0]), $transforms),
        ];
    }

    /**
     * @param array<array-key, mixed> $fields
     * @return array<string, mixed>
     */
    private function document(string $path, array $fields): array
    {
        FirestoreClient::documentSegments($path);
        $codec = $this->client->codec();

        return ['name' => $codec->documentName($path), 'fields' => $codec->encodeFields($fields)];
    }

    /**
     * @param list<string> $segments
     * @return array<string, mixed>
     */
    private static function transform(ValueCodec $codec, array $segments, FieldValue $value): array
    {
        $out = ['fieldPath' => ValueCodec::quoteSegments($segments)];

        return $out + match ($value->kind) {
            FieldValue::SERVER_TIMESTAMP => ['setToServerValue' => 'REQUEST_TIME'],
            FieldValue::INCREMENT => ['increment' => $codec->encode($value->by)],
            FieldValue::ARRAY_UNION => ['appendMissingElements' => ['values' => array_map($codec->encode(...), $value->values)]],
            default => ['removeAllFromArray' => ['values' => array_map($codec->encode(...), $value->values)]],
        };
    }

    /**
     * Pull FieldValue sentinels out of the data, at any depth. A map left empty by that is dropped.
     *
     * @param array<array-key, mixed> $data
     * @param list<string> $prefix
     * @param list<array{0: list<string>, 1: FieldValue}> $transforms
     * @return array<array-key, mixed>
     */
    private static function extract(array $data, array $prefix, array &$transforms): array
    {
        $out = [];
        foreach ($data as $name => $value) {
            $segments = [...$prefix, (string) $name];
            if ($value instanceof FieldValue) {
                $transforms[] = [$segments, $value];
                continue;
            }
            if (is_array($value) && $value !== [] && !array_is_list($value)) {
                $value = self::extract($value, $segments, $transforms);
                if ($value === []) {
                    continue;
                }
            }
            $out[$name] = $value;
        }

        return $out;
    }

    /**
     * The leaf field paths of nested data (a non-empty map is descended, anything else is a leaf).
     *
     * @param array<array-key, mixed> $data
     * @param list<string> $prefix
     * @return list<list<string>>
     */
    private static function leafPaths(array $data, array $prefix): array
    {
        $paths = [];
        foreach ($data as $name => $value) {
            $segments = [...$prefix, (string) $name];
            if (is_array($value) && $value !== [] && !array_is_list($value)) {
                $paths = [...$paths, ...self::leafPaths($value, $segments)];
            } else {
                $paths[] = $segments;
            }
        }

        return $paths;
    }

    /**
     * @param array<array-key, mixed> $nested
     * @param list<string> $segments
     */
    private static function place(array &$nested, array $segments, mixed $value): void
    {
        $last = count($segments) - 1;
        $node = &$nested;
        foreach ($segments as $i => $segment) {
            if ($segment === '') {
                throw new InvalidArgumentException('A field path has an empty segment.');
            }
            if ($i === $last) {
                $node[$segment] = $value;
                break;
            }
            if (!isset($node[$segment]) || !is_array($node[$segment])) {
                $node[$segment] = [];
            }
            $node = &$node[$segment];
        }
    }

    private static function autoId(): string
    {
        $id = '';
        $max = strlen(self::ID_ALPHABET) - 1;
        for ($i = 0; $i < self::ID_LENGTH; $i++) {
            $id .= self::ID_ALPHABET[random_int(0, $max)];
        }

        return $id;
    }
}
