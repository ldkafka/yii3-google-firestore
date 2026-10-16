<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Unit;

use DateTimeImmutable;
use InvalidArgumentException;
use Ldkafka\GoogleFirestore\FieldValue;
use Ldkafka\GoogleFirestore\Internal\ValueCodec;
use Ldkafka\GoogleFirestore\Value\Bytes;
use Ldkafka\GoogleFirestore\Value\GeoPoint;
use Ldkafka\GoogleFirestore\Value\Reference;
use Ldkafka\GoogleFirestore\Value\Timestamp;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function fopen;
use function json_encode;

/**
 * Every row of the value mapping, both ways, and the wire rules.
 */
final class ValueCodecTest extends TestCase
{
    private const ROOT = 'projects/p/databases/(default)/documents';

    private ValueCodec $codec;

    protected function setUp(): void
    {
        $this->codec = new ValueCodec(self::ROOT);
    }

    /**
     * @return iterable<string, array{mixed, string}>
     */
    public static function encodings(): iterable
    {
        yield 'null' => [null, '{"nullValue":null}'];
        yield 'true' => [true, '{"booleanValue":true}'];
        yield 'false' => [false, '{"booleanValue":false}'];
        yield 'int' => [42, '{"integerValue":"42"}'];
        yield 'int64 max' => [PHP_INT_MAX, '{"integerValue":"9223372036854775807"}'];
        yield 'int64 min' => [PHP_INT_MIN, '{"integerValue":"-9223372036854775808"}'];
        yield 'double' => [1.5, '{"doubleValue":1.5}'];
        yield 'whole double' => [2.0, '{"doubleValue":2.0}'];
        yield 'NaN' => [NAN, '{"doubleValue":"NaN"}'];
        yield 'Infinity' => [INF, '{"doubleValue":"Infinity"}'];
        yield '-Infinity' => [-INF, '{"doubleValue":"-Infinity"}'];
        yield 'string' => ['héllo', '{"stringValue":"héllo"}'];
        yield 'timestamp' => [new Timestamp(1_800_000_000, 123_456_789), '{"timestampValue":"2027-01-15T08:00:00.123456789Z"}'];
        yield 'datetime' => [new DateTimeImmutable('2026-10-16T12:34:56.5+10:00'), '{"timestampValue":"2026-10-16T02:34:56.500000Z"}'];
        yield 'bytes' => [new Bytes("\x00\x01\xff"), '{"bytesValue":"AAH/"}'];
        yield 'reference' => [new Reference('messages/abc'), '{"referenceValue":"' . self::ROOT . '/messages/abc"}'];
        yield 'geopoint' => [new GeoPoint(-33.86, 151.2), '{"geoPointValue":{"latitude":-33.86,"longitude":151.2}}'];
        yield 'list' => [[1, 'a'], '{"arrayValue":{"values":[{"integerValue":"1"},{"stringValue":"a"}]}}'];
        yield 'empty array' => [[], '{"arrayValue":{}}'];
        yield 'map' => [['a' => 1], '{"mapValue":{"fields":{"a":{"integerValue":"1"}}}}'];
        yield 'empty map' => [new stdClass(), '{"mapValue":{"fields":{}}}'];
        yield 'nested' => [['m' => ['l' => [true]]], '{"mapValue":{"fields":{"m":{"mapValue":{"fields":{"l":{"arrayValue":{"values":[{"booleanValue":true}]}}}}}}}}'];
    }

    #[DataProvider('encodings')]
    public function testEncodesToTheWireForm(mixed $value, string $json): void
    {
        self::assertSame($json, json_encode($this->codec->encode($value), JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION));
    }

    public function testDecodesEveryType(): void
    {
        $fields = [
            'n' => ['nullValue' => null],
            'b' => ['booleanValue' => true],
            'i' => ['integerValue' => '-9223372036854775808'],
            'd' => ['doubleValue' => 1.25],
            'dInt' => ['doubleValue' => 0],
            'nan' => ['doubleValue' => 'NaN'],
            'inf' => ['doubleValue' => 'Infinity'],
            'ninf' => ['doubleValue' => '-Infinity'],
            's' => ['stringValue' => 'x'],
            't' => ['timestampValue' => '2026-10-16T13:21:52.070602Z'],
            'y' => ['bytesValue' => 'AAEC/w=='],
            'r' => ['referenceValue' => self::ROOT . '/t/a'],
            'rOther' => ['referenceValue' => 'projects/q/databases/other-db/documents/x/y'],
            'g' => ['geoPointValue' => ['latitude' => -33.86, 'longitude' => 151.2]],
            'g0' => ['geoPointValue' => []],
            'a' => ['arrayValue' => ['values' => [['integerValue' => '1']]]],
            'a0' => ['arrayValue' => []],
            'm' => ['mapValue' => ['fields' => ['k' => ['stringValue' => 'v']]]],
            'm0' => ['mapValue' => []],
        ];

        $data = $this->codec->decodeFields($fields);

        self::assertNull($data['n']);
        self::assertTrue($data['b']);
        self::assertSame(PHP_INT_MIN, $data['i']);
        self::assertSame(1.25, $data['d']);
        self::assertSame(0.0, $data['dInt']);
        self::assertIsFloat($data['nan']);
        self::assertNan($data['nan']);
        self::assertSame(INF, $data['inf']);
        self::assertSame(-INF, $data['ninf']);
        self::assertSame('x', $data['s']);
        self::assertEquals(new Timestamp(Timestamp::parse('2026-10-16T13:21:52Z')->seconds, 70_602_000), $data['t']);
        self::assertEquals(new Bytes("\x00\x01\x02\xff"), $data['y']);
        self::assertEquals(new Reference('t/a'), $data['r']);
        self::assertEquals(new Reference('x/y'), $data['rOther'], 'a reference to another database keeps its path only');
        self::assertEquals(new GeoPoint(-33.86, 151.2), $data['g']);
        self::assertEquals(new GeoPoint(0.0, 0.0), $data['g0'], 'proto3 JSON leaves out zero coordinates');
        self::assertSame([1], $data['a']);
        self::assertSame([], $data['a0']);
        self::assertSame(['k' => 'v'], $data['m']);
        self::assertSame([], $data['m0']);
    }

    public function testEmptyFieldsAreAJsonObject(): void
    {
        self::assertSame('{}', json_encode($this->codec->encodeFields([])));
    }

    public function testInvalidUtf8MustBeBytes(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Bytes');
        $this->codec->encode("\xff\xfe");
    }

    public function testASentinelIsNotAValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->codec->encode(FieldValue::serverTimestamp());
    }

    public function testAnUnknownTypeIsRefusedWithoutItsValue(): void
    {
        try {
            $this->codec->encode(fopen('php://memory', 'r'));
            self::fail('expected exception');
        } catch (InvalidArgumentException $e) {
            self::assertStringContainsString('resource', $e->getMessage());
        }
        $this->expectException(InvalidArgumentException::class);
        $this->codec->encode(new \ArrayObject([1]));
    }

    public function testFieldPathsAreQuotedWhereNeeded(): void
    {
        self::assertSame('meta.claimant', ValueCodec::quoteFieldPath('meta.claimant'));
        self::assertSame('_a1.B', ValueCodec::quoteFieldPath('_a1.B'));
        self::assertSame('`a-b`.c', ValueCodec::quoteFieldPath('a-b.c'));
        self::assertSame('`1x`', ValueCodec::quoteFieldPath('1x'));
        self::assertSame('`x\\`y`', ValueCodec::quoteFieldPath('x`y'));
        self::assertSame('`a\\\\b`', ValueCodec::quoteFieldPath('a\\b'));
        self::assertSame('m.`x.y`', ValueCodec::quoteSegments(['m', 'x.y']));

        $this->expectException(InvalidArgumentException::class);
        ValueCodec::quoteFieldPath('a..b');
    }

    public function testDocumentNamesAndPaths(): void
    {
        self::assertSame(self::ROOT . '/c/a b', $this->codec->documentName('c/a b'));
        self::assertSame('c/a b', $this->codec->relativePath(self::ROOT . '/c/a b'));
        $document = $this->codec->decodeDocument([
            'name' => self::ROOT . '/threads/t1/replies/r1',
            'fields' => ['x' => ['integerValue' => '1']],
            'createTime' => '2026-10-16T13:21:07.221777Z',
            'updateTime' => '2026-10-16T13:21:08.000001Z',
        ]);
        self::assertSame('threads/t1/replies/r1', $document->path);
        self::assertSame('r1', $document->id);
        self::assertSame(['x' => 1], $document->data);
        self::assertSame('2026-10-16T13:21:08.000001Z', (string) $document->updateTime);
    }
}
