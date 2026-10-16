<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Value;

/**
 * A reference to a document of the same database (Firestore referenceValue).
 */
final readonly class Reference
{
    /**
     * @param string $path The document path relative to the database, e.g. 'messages/abc'.
     */
    public function __construct(public string $path)
    {
    }
}
