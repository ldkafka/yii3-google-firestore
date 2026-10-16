<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Support;

use Psr\Log\AbstractLogger;
use Stringable;

use function is_scalar;
use function strtr;

/**
 * Keeps log records in memory, with placeholders interpolated.
 */
final class ArrayLogger extends AbstractLogger
{
    /** @var list<array{level: string, message: string, context: array<mixed>}> */
    public array $records = [];

    /**
     * Record one line.
     *
     * @param mixed $level
     * @param array<mixed> $context
     */
    public function log($level, string|Stringable $message, array $context = []): void
    {
        $replace = [];
        foreach ($context as $key => $value) {
            if (is_scalar($value) || $value instanceof Stringable) {
                $replace['{' . $key . '}'] = (string) $value;
            }
        }
        $this->records[] = [
            'level' => is_scalar($level) ? (string) $level : 'unknown',
            'message' => strtr((string) $message, $replace),
            'context' => $context,
        ];
    }

    /**
     * Every record, as text with its context, for searching.
     */
    public function dump(): string
    {
        return print_r($this->records, true);
    }
}
