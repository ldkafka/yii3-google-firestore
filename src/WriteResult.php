<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore;

use Ldkafka\GoogleFirestore\Value\Timestamp;

/**
 * The outcome of one committed write.
 */
final readonly class WriteResult
{
    /**
     * @param string $path The written document's path.
     * @param Timestamp|null $updateTime Its new version (null for a delete).
     * @param array<string, mixed> $transformResults Dotted field path => the value the server set
     *     (array transforms give null).
     */
    public function __construct(public string $path, public ?Timestamp $updateTime, public array $transformResults = [])
    {
    }
}
