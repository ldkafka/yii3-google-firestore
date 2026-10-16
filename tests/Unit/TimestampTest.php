<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Unit;

use DateTimeImmutable;
use InvalidArgumentException;
use Ldkafka\GoogleFirestore\Value\Timestamp;
use PHPUnit\Framework\TestCase;

/**
 * RFC 3339 with nanoseconds, both ways.
 */
final class TimestampTest extends TestCase
{
    public function testParseKeepsNineDigits(): void
    {
        $t = Timestamp::parse('2026-10-16T12:34:56.123456789Z');

        self::assertSame(123_456_789, $t->nanos);
        self::assertSame((new DateTimeImmutable('2026-10-16T12:34:56Z'))->getTimestamp(), $t->seconds);
        self::assertSame('2026-10-16T12:34:56.123456789Z', (string) $t);
    }

    public function testParseTakesAnyFractionAndOffset(): void
    {
        self::assertSame(0, Timestamp::parse('2026-10-16T12:34:56Z')->nanos);
        self::assertSame(100_000_000, Timestamp::parse('2026-10-16T12:34:56.1Z')->nanos);
        self::assertSame(129_000_000, Timestamp::parse('2026-10-16T13:21:52.129Z')->nanos);
        self::assertSame('2026-10-16T02:34:56.000000Z', (string) Timestamp::parse('2026-10-16T12:34:56+10:00'));
        self::assertSame('2026-10-16T12:34:56.500000Z', (string) Timestamp::parse('2026-10-16t12:34:56.5z'));
    }

    public function testStringIsSixDigitsWhenMicrosecondAligned(): void
    {
        self::assertSame('2026-10-16T13:21:52.070602Z', (string) Timestamp::parse('2026-10-16T13:21:52.070602Z'));
        self::assertSame('1970-01-01T00:00:00.000000Z', (string) new Timestamp(0));
    }

    public function testDateTimeBothWays(): void
    {
        $date = new DateTimeImmutable('2026-10-16T12:34:56.654321Z');
        $t = Timestamp::fromDateTime($date);

        self::assertSame(654_321_000, $t->nanos);
        self::assertSame('2026-10-16T12:34:56.654321+00:00', $t->toDateTime()->format('Y-m-d\TH:i:s.uP'));
        self::assertSame('2026-10-16T12:34:56.123456+00:00', (new Timestamp($t->seconds, 123_456_999))->toDateTime()->format('Y-m-d\TH:i:s.uP'));
    }

    public function testInvalidInputIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        Timestamp::parse('16 Oct 2026');
    }

    public function testNanosAreBounded(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new Timestamp(0, 1_000_000_000);
    }
}
