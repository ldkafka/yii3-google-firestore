<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Value;

/**
 * Raw binary data (Firestore bytesValue). Plain PHP strings are written as text and must be UTF-8.
 */
final readonly class Bytes
{
    /**
     * @param string $value The raw bytes, not base64.
     */
    public function __construct(public string $value)
    {
    }
}
