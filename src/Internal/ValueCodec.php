<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Internal;

use DateTimeInterface;
use InvalidArgumentException;
use Ldkafka\GoogleFirestore\Document;
use Ldkafka\GoogleFirestore\FieldValue;
use Ldkafka\GoogleFirestore\Value\Bytes;
use Ldkafka\GoogleFirestore\Value\GeoPoint;
use Ldkafka\GoogleFirestore\Value\Reference;
use Ldkafka\GoogleFirestore\Value\Timestamp;
use stdClass;

use function array_is_list;
use function array_key_exists;
use function array_map;
use function base64_decode;
use function base64_encode;
use function explode;
use function get_debug_type;
use function implode;
use function is_array;
use function is_bool;
use function is_float;
use function is_infinite;
use function is_int;
use function is_nan;
use function is_numeric;
use function is_string;
use function preg_match;
use function preg_replace;
use function str_replace;
use function strrpos;
use function substr;

/**
 * PHP values to Firestore REST Values and back. Bound to one database, for references.
 *
 * @internal
 */
final class ValueCodec
{
    private const SIMPLE_SEGMENT = '/^[A-Za-z_][A-Za-z0-9_]*$/';
    private const ANY_DATABASE_ROOT = '#^projects/[^/]+/databases/[^/]+/documents/#';

    /**
     * @param string $documentsRoot 'projects/{p}/databases/{db}/documents'.
     */
    public function __construct(private string $documentsRoot)
    {
    }

    /**
     * One PHP value as a Firestore Value.
     *
     * @return array<string, mixed>
     */
    public function encode(mixed $value): array
    {
        return match (true) {
            $value === null => ['nullValue' => null],
            is_bool($value) => ['booleanValue' => $value],
            is_int($value) => ['integerValue' => (string) $value],
            is_float($value) => ['doubleValue' => self::encodeDouble($value)],
            is_string($value) => ['stringValue' => self::utf8($value)],
            $value instanceof Timestamp => ['timestampValue' => (string) $value],
            $value instanceof DateTimeInterface => ['timestampValue' => (string) Timestamp::fromDateTime($value)],
            $value instanceof Bytes => ['bytesValue' => base64_encode($value->value)],
            $value instanceof Reference => ['referenceValue' => $this->documentsRoot . '/' . $value->path],
            $value instanceof GeoPoint => ['geoPointValue' => ['latitude' => $value->latitude, 'longitude' => $value->longitude]],
            $value instanceof stdClass => ['mapValue' => ['fields' => $this->encodeFields((array) $value)]],
            is_array($value) && $value !== [] && !array_is_list($value) => ['mapValue' => ['fields' => $this->encodeFields($value)]],
            is_array($value) => $value === [] ? ['arrayValue' => new stdClass()] : ['arrayValue' => ['values' => array_map($this->encode(...), $value)]],
            $value instanceof FieldValue => throw new InvalidArgumentException('A FieldValue sentinel is only allowed as a field value in a write, not here.'),
            default => throw new InvalidArgumentException('A ' . get_debug_type($value) . ' has no Firestore type.'),
        };
    }

    /**
     * A map of PHP values as Firestore fields; an empty map becomes a JSON object.
     *
     * @param array<array-key, mixed> $data
     * @return array<string, array<string, mixed>>|stdClass
     */
    public function encodeFields(array $data): array|stdClass
    {
        $fields = [];
        foreach ($data as $name => $value) {
            $fields[(string) $name] = $this->encode($value);
        }

        return $fields === [] ? new stdClass() : $fields;
    }

    /**
     * A Firestore Value as a PHP value.
     */
    public function decode(mixed $value): mixed
    {
        if (!is_array($value)) {
            throw new InvalidArgumentException('Not a Firestore Value.');
        }
        foreach ($value as $type => $inner) {
            return match ($type) {
                'nullValue' => null,
                'booleanValue' => (bool) $inner,
                'integerValue' => is_numeric($inner) ? (int) $inner : 0,
                'doubleValue' => self::decodeDouble($inner),
                'stringValue' => is_string($inner) ? $inner : '',
                'timestampValue' => Timestamp::parse(is_string($inner) ? $inner : ''),
                'bytesValue' => new Bytes((string) base64_decode(is_string($inner) ? $inner : '', true)),
                'referenceValue' => new Reference((string) preg_replace(self::ANY_DATABASE_ROOT, '', is_string($inner) ? $inner : '')),
                'geoPointValue' => new GeoPoint(self::number($inner, 'latitude'), self::number($inner, 'longitude')),
                'arrayValue' => array_map($this->decode(...), self::listAt($inner, 'values')),
                'mapValue' => $this->decodeFields(is_array($inner) ? $inner['fields'] ?? [] : []),
                default => throw new InvalidArgumentException('Unknown Firestore value type.'),
            };
        }

        throw new InvalidArgumentException('Empty Firestore Value.');
    }

    /**
     * Firestore fields as a PHP map.
     *
     * @return array<string, mixed>
     */
    public function decodeFields(mixed $fields): array
    {
        $data = [];
        if (is_array($fields)) {
            foreach ($fields as $name => $value) {
                $data[(string) $name] = $this->decode($value);
            }
        }

        return $data;
    }

    /**
     * A REST Document object as a Document.
     */
    public function decodeDocument(mixed $document): Document
    {
        if (!is_array($document) || !is_string($document['name'] ?? null)) {
            throw new InvalidArgumentException('Not a Firestore Document.');
        }
        $path = $this->relativePath($document['name']);
        $createTime = $document['createTime'] ?? null;
        $updateTime = $document['updateTime'] ?? null;

        return new Document(
            $path,
            substr($path, (int) strrpos($path, '/') + 1),
            $this->decodeFields($document['fields'] ?? []),
            Timestamp::parse(is_string($createTime) ? $createTime : ''),
            Timestamp::parse(is_string($updateTime) ? $updateTime : ''),
        );
    }

    /**
     * 'projects/{p}/databases/{db}/documents'.
     */
    public function documentsRoot(): string
    {
        return $this->documentsRoot;
    }

    /**
     * 'messages/abc' as 'projects/{p}/databases/{db}/documents/messages/abc'.
     */
    public function documentName(string $path): string
    {
        return $this->documentsRoot . '/' . $path;
    }

    /**
     * A full document name as its path relative to the database.
     */
    public function relativePath(string $name): string
    {
        return (string) preg_replace(self::ANY_DATABASE_ROOT, '', $name);
    }

    /**
     * A dotted field path ('meta.claimant') in wire form: each segment that is not a simple name is
     * backtick-quoted ('`a-b`.c').
     */
    public static function quoteFieldPath(string $dotted): string
    {
        return self::quoteSegments(explode('.', $dotted));
    }

    /**
     * Field-name segments as one wire field path, quoting each segment that is not a simple name.
     *
     * @param list<string> $segments
     */
    public static function quoteSegments(array $segments): string
    {
        $quoted = [];
        foreach ($segments as $segment) {
            if ($segment === '') {
                throw new InvalidArgumentException('A field path has an empty segment.');
            }
            $quoted[] = preg_match(self::SIMPLE_SEGMENT, $segment) === 1
                ? $segment
                : '`' . str_replace(['\\', '`'], ['\\\\', '\\`'], $segment) . '`';
        }

        return implode('.', $quoted);
    }

    private static function encodeDouble(float $value): float|string
    {
        if (is_nan($value)) {
            return 'NaN';
        }
        if (is_infinite($value)) {
            return $value > 0 ? 'Infinity' : '-Infinity';
        }

        return $value;
    }

    private static function decodeDouble(mixed $value): float
    {
        return match ($value) {
            'NaN' => NAN,
            'Infinity' => INF,
            '-Infinity' => -INF,
            default => is_numeric($value) ? (float) $value : 0.0,
        };
    }

    private static function utf8(string $value): string
    {
        if (preg_match('//u', $value) !== 1) {
            throw new InvalidArgumentException('A string value is not valid UTF-8; wrap binary data in Bytes.');
        }

        return $value;
    }

    private static function number(mixed $object, string $key): float
    {
        $value = is_array($object) && array_key_exists($key, $object) ? $object[$key] : 0;

        return is_numeric($value) ? (float) $value : 0.0;
    }

    /**
     * @return array<mixed>
     */
    private static function listAt(mixed $object, string $key): array
    {
        $value = is_array($object) ? $object[$key] ?? [] : [];

        return is_array($value) ? $value : [];
    }
}
