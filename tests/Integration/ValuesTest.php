<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Integration;

use DateTimeImmutable;
use Ldkafka\GoogleFirestore\Tests\Support\EmulatorTestCase;
use Ldkafka\GoogleFirestore\Value\Bytes;
use Ldkafka\GoogleFirestore\Value\GeoPoint;
use Ldkafka\GoogleFirestore\Value\Reference;
use Ldkafka\GoogleFirestore\Value\Timestamp;
use stdClass;

/**
 * Every value type survives a round trip through the emulator.
 */
final class ValuesTest extends EmulatorTestCase
{
    public function testEveryTypeRoundTrips(): void
    {
        $this->fs->set('values/all', [
            'null' => null,
            'true' => true,
            'false' => false,
            'int' => 42,
            'intMax' => PHP_INT_MAX,
            'intMin' => PHP_INT_MIN,
            'double' => -1.25,
            'zero' => 0.0,
            'nan' => NAN,
            'inf' => INF,
            'ninf' => -INF,
            'string' => "multi\nline ünïcödé ✓",
            'empty' => '',
            'micros' => Timestamp::parse('2026-10-16T12:34:56.123456Z'),
            'nanos' => Timestamp::parse('2026-10-16T12:34:56.123456789Z'),
            'date' => new DateTimeImmutable('2026-10-16T12:34:56.5+10:00'),
            'bytes' => new Bytes("\x00binary\xff\x00"),
            'reference' => new Reference('messages/m1'),
            'geo' => new GeoPoint(-33.8688, 151.2093),
            'geoZero' => new GeoPoint(0.0, 0.0),
            'list' => [1, 'two', [ 'three' => 3 ]],
            'emptyList' => [],
            'map' => ['a' => ['b' => ['c' => true]]],
            'emptyMap' => new stdClass(),
        ]);

        $doc = $this->fs->get('values/all');
        self::assertNotNull($doc);
        $data = $doc->data;

        self::assertNull($data['null']);
        self::assertTrue($data['true']);
        self::assertFalse($data['false']);
        self::assertSame(42, $data['int']);
        self::assertSame(PHP_INT_MAX, $data['intMax']);
        self::assertSame(PHP_INT_MIN, $data['intMin']);
        self::assertSame(-1.25, $data['double']);
        self::assertSame(0.0, $data['zero']);
        self::assertIsFloat($data['nan']);
        self::assertNan($data['nan']);
        self::assertSame(INF, $data['inf']);
        self::assertSame(-INF, $data['ninf']);
        self::assertSame("multi\nline ünïcödé ✓", $data['string']);
        self::assertSame('', $data['empty']);
        self::assertSame('2026-10-16T12:34:56.123456Z', (string) self::timestamp($data['micros']));
        self::assertSame('2026-10-16T12:34:56.123456Z', (string) self::timestamp($data['nanos']), 'stored with microsecond precision');
        self::assertSame('2026-10-16T02:34:56.500000Z', (string) self::timestamp($data['date']));
        self::assertEquals(new Bytes("\x00binary\xff\x00"), $data['bytes']);
        self::assertEquals(new Reference('messages/m1'), $data['reference']);
        self::assertEquals(new GeoPoint(-33.8688, 151.2093), $data['geo']);
        self::assertEquals(new GeoPoint(0.0, 0.0), $data['geoZero']);
        self::assertSame([1, 'two', ['three' => 3]], $data['list']);
        self::assertSame([], $data['emptyList']);
        self::assertSame(['a' => ['b' => ['c' => true]]], $data['map']);
        self::assertSame([], $data['emptyMap']);
    }

    private static function timestamp(mixed $value): Timestamp
    {
        self::assertInstanceOf(Timestamp::class, $value);

        return $value;
    }
}
