<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore;

use function array_values;

/**
 * Sentinels for server-side field transforms. Put one where a value would go in create(), set() or
 * update() data; the server applies it at commit time.
 */
final readonly class FieldValue
{
    public const SERVER_TIMESTAMP = 'serverTimestamp';
    public const INCREMENT = 'increment';
    public const ARRAY_UNION = 'arrayUnion';
    public const ARRAY_REMOVE = 'arrayRemove';

    /**
     * @param list<mixed> $values
     */
    private function __construct(public string $kind, public int|float $by = 0, public array $values = [])
    {
    }

    /**
     * The commit time on the server (millisecond precision).
     */
    public static function serverTimestamp(): self
    {
        return new self(self::SERVER_TIMESTAMP);
    }

    /**
     * Add to the stored number (a missing field counts as 0).
     */
    public static function increment(int|float $by): self
    {
        return new self(self::INCREMENT, $by);
    }

    /**
     * Append each value the stored array doesn't hold yet (a missing field becomes the array).
     */
    public static function arrayUnion(mixed ...$values): self
    {
        return new self(self::ARRAY_UNION, values: array_values($values));
    }

    /**
     * Remove every copy of each value from the stored array (a missing field becomes an empty array).
     */
    public static function arrayRemove(mixed ...$values): self
    {
        return new self(self::ARRAY_REMOVE, values: array_values($values));
    }
}
