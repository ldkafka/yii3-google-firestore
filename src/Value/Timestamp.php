<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Value;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use InvalidArgumentException;
use Stringable;

use function gmdate;
use function intdiv;
use function preg_match;
use function sprintf;
use function str_pad;
use function strtoupper;

/**
 * A point in time with nanosecond precision, as Firestore stores it: seconds since the Unix epoch
 * plus nanoseconds. An updateTime read from Firestore keeps every digit, so it can go back
 * unchanged in a precondition.
 */
final readonly class Timestamp implements Stringable
{
    private const PATTERN = '/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/i';

    /**
     * @param int $seconds Seconds since 1970-01-01T00:00:00Z.
     * @param int $nanos 0 to 999,999,999.
     */
    public function __construct(public int $seconds, public int $nanos = 0)
    {
        if ($nanos < 0 || $nanos > 999_999_999) {
            throw new InvalidArgumentException('Timestamp nanos must be between 0 and 999999999.');
        }
    }

    /**
     * Parse an RFC 3339 time with 0 to 9 fraction digits and any offset.
     */
    public static function parse(string $rfc3339): self
    {
        if (preg_match(self::PATTERN, $rfc3339, $m) !== 1) {
            throw new InvalidArgumentException('Not an RFC 3339 timestamp.');
        }
        $offset = strtoupper($m[3]) === 'Z' ? '+00:00' : $m[3];
        $time = DateTimeImmutable::createFromFormat('!Y-m-d\TH:i:sP', strtoupper($m[1]) . $offset);
        if ($time === false) {
            throw new InvalidArgumentException('Not an RFC 3339 timestamp.');
        }
        $fraction = $m[2];

        return new self($time->getTimestamp(), $fraction === '' ? 0 : (int) str_pad($fraction, 9, '0'));
    }

    /**
     * The same instant as a PHP date (microseconds kept).
     */
    public static function fromDateTime(DateTimeInterface $time): self
    {
        return new self($time->getTimestamp(), (int) $time->format('u') * 1000);
    }

    /**
     * This instant as a UTC DateTimeImmutable, truncated to microseconds.
     */
    public function toDateTime(): DateTimeImmutable
    {
        $time = new DateTimeImmutable('@' . $this->seconds);
        $micros = intdiv($this->nanos, 1000);

        return $time->setTimezone(new DateTimeZone('UTC'))
            ->setTime((int) $time->format('H'), (int) $time->format('i'), (int) $time->format('s'), $micros);
    }

    /**
     * RFC 3339 in UTC: six fraction digits, or nine when the time has sub-microsecond digits.
     */
    public function __toString(): string
    {
        $fraction = $this->nanos % 1000 === 0
            ? sprintf('%06d', intdiv($this->nanos, 1000))
            : sprintf('%09d', $this->nanos);

        return gmdate('Y-m-d\TH:i:s', $this->seconds) . '.' . $fraction . 'Z';
    }
}
