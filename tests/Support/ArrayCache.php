<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Support;

use DateInterval;
use Psr\SimpleCache\CacheInterface;

/**
 * Minimal PSR-16 cache for the tests.
 */
final class ArrayCache implements CacheInterface
{
    /** @var array<string, mixed> */
    public array $data = [];

    /**
     * The value, or the default.
     */
    public function get(string $key, mixed $default = null): mixed
    {
        return $this->data[$key] ?? $default;
    }

    /**
     * Store a value (the TTL is ignored).
     */
    public function set(string $key, mixed $value, null|int|DateInterval $ttl = null): bool
    {
        $this->data[$key] = $value;

        return true;
    }

    /**
     * Remove a value.
     */
    public function delete(string $key): bool
    {
        unset($this->data[$key]);

        return true;
    }

    /**
     * Remove everything.
     */
    public function clear(): bool
    {
        $this->data = [];

        return true;
    }

    /**
     * Several values.
     *
     * @param iterable<string> $keys
     * @return array<string, mixed>
     */
    public function getMultiple(iterable $keys, mixed $default = null): iterable
    {
        $out = [];
        foreach ($keys as $key) {
            $out[$key] = $this->get($key, $default);
        }

        return $out;
    }

    /**
     * Store several values.
     *
     * @param iterable<string, mixed> $values
     */
    public function setMultiple(iterable $values, null|int|DateInterval $ttl = null): bool
    {
        foreach ($values as $key => $value) {
            $this->set($key, $value);
        }

        return true;
    }

    /**
     * Remove several values.
     *
     * @param iterable<string> $keys
     */
    public function deleteMultiple(iterable $keys): bool
    {
        foreach ($keys as $key) {
            $this->delete($key);
        }

        return true;
    }

    /**
     * Whether a value is stored.
     */
    public function has(string $key): bool
    {
        return isset($this->data[$key]);
    }
}
