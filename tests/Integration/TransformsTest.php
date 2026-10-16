<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Integration;

use Ldkafka\GoogleFirestore\FieldValue;
use Ldkafka\GoogleFirestore\Tests\Support\EmulatorTestCase;
use Ldkafka\GoogleFirestore\Value\Timestamp;

use function time;

/**
 * Server timestamp, increment, arrayUnion and arrayRemove on the emulator.
 */
final class TransformsTest extends EmulatorTestCase
{
    public function testTheTransforms(): void
    {
        $this->fs->set('messages/m1', ['keep' => 'k', 'attempts' => 1, 'labels' => ['infra'], 'watchers' => ['steward', 'infra', 'steward']]);

        $first = $this->fs->update('messages/m1', [
            'doneAt' => FieldValue::serverTimestamp(),
            'attempts' => FieldValue::increment(1),
            'labels' => FieldValue::arrayUnion('infra', 'urgent'),
            'watchers' => FieldValue::arrayRemove('steward'),
            'meta' => ['at' => FieldValue::serverTimestamp()],
        ]);
        $this->fs->update('messages/m1', ['attempts' => FieldValue::increment(1), 'ratio' => FieldValue::increment(0.5)]);

        $doneAt = $first->transformResults['doneAt'];
        self::assertInstanceOf(Timestamp::class, $doneAt);
        self::assertEqualsWithDelta(time(), $doneAt->seconds, 60);
        self::assertSame(2, $first->transformResults['attempts']);
        self::assertNull($first->transformResults['labels']);

        $doc = $this->fs->get('messages/m1');
        self::assertNotNull($doc);
        self::assertSame('k', $doc->get('keep'), 'a transform-only update with an empty mask leaves other fields alone');
        self::assertEquals($doneAt, $doc->get('doneAt'));
        self::assertEquals($doneAt, $doc->get('meta.at'));
        self::assertSame(3, $doc->get('attempts'), 'increment twice');
        self::assertSame(0.5, $doc->get('ratio'), 'a missing field counts as 0');
        self::assertSame(['infra', 'urgent'], $doc->get('labels'), 'arrayUnion adds no duplicates');
        self::assertSame(['infra'], $doc->get('watchers'), 'arrayRemove removes every copy');
    }

    public function testTransformsInACreate(): void
    {
        $result = $this->fs->create('messages', ['to' => 'infra', 'createdAt' => FieldValue::serverTimestamp()]);

        self::assertInstanceOf(Timestamp::class, $result->transformResults['createdAt']);
        self::assertEquals($result->transformResults['createdAt'], $this->fs->get($result->path)?->get('createdAt'));
    }
}
