<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore;

use Ldkafka\GoogleFirestore\Value\Timestamp;

use function array_key_exists;
use function explode;
use function is_array;

/**
 * A document as read: its path, id, decoded fields and versions.
 */
final readonly class Document
{
    /**
     * @param string $path Relative to the database, e.g. 'messages/abc'.
     * @param string $id The last segment of the path.
     * @param array<string, mixed> $data The fields, decoded to PHP values.
     * @param Timestamp $createTime When it was created.
     * @param Timestamp $updateTime Its version: pass it to Precondition::updateTime() to write only if unchanged.
     */
    public function __construct(
        public string $path,
        public string $id,
        public array $data,
        public Timestamp $createTime,
        public Timestamp $updateTime,
    ) {
    }

    /**
     * The value at a dotted field path ('meta.claimant'), or null when absent.
     */
    public function get(string $fieldPath): mixed
    {
        $value = $this->data;
        foreach (explode('.', $fieldPath) as $segment) {
            if (!is_array($value) || !array_key_exists($segment, $value)) {
                return null;
            }
            $value = $value[$segment];
        }

        return $value;
    }
}
