<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore;

use Ldkafka\GoogleFirestore\Value\Timestamp;

/**
 * A condition the stored document must meet when a write is committed, or the write fails.
 */
final readonly class Precondition
{
    private function __construct(private ?bool $exists, private ?Timestamp $updateTime)
    {
    }

    /**
     * The document must exist (true) or must not exist (false).
     */
    public static function exists(bool $exists = true): self
    {
        return new self($exists, null);
    }

    /**
     * The document must still be this version: its updateTime as last read. Fails with
     * FailedPreconditionException when another write changed it (or it was deleted).
     */
    public static function updateTime(Timestamp $updateTime): self
    {
        return new self(null, $updateTime);
    }

    /**
     * The REST `currentDocument` object.
     *
     * @internal
     * @return array{exists: bool}|array{updateTime: string}
     */
    public function toArray(): array
    {
        return $this->updateTime !== null
            ? ['updateTime' => (string) $this->updateTime]
            : ['exists' => (bool) $this->exists];
    }
}
