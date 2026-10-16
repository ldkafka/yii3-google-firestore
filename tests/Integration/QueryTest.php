<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Integration;

use Ldkafka\GoogleFirestore\Document;
use Ldkafka\GoogleFirestore\Tests\Support\EmulatorTestCase;

use function array_map;
use function iterator_to_array;

/**
 * Filters, order, limit, cursors, stream(), count(), collection groups and collection lists.
 */
final class QueryTest extends EmulatorTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $batch = $this->fs->batch();
        for ($i = 1; $i <= 7; $i++) {
            $batch->set('messages/m' . $i, [
                'to' => $i % 2 === 0 ? 'steward' : 'infra',
                'status' => $i <= 4 ? 'new' : 'done',
                'n' => $i,
                'tags' => $i === 3 ? ['urgent'] : [],
                'note' => $i === 5 ? null : 'x',
            ]);
        }
        $batch->set('threads/t1', ['title' => 'one']);
        $batch->set('threads/t1/replies/r1', ['author' => 'infra']);
        $batch->set('threads/t2/replies/r2', ['author' => 'steward']);
        $batch->set('threads/t2/replies/r3', ['author' => 'infra']);
        $batch->commit();
    }

    public function testFilterOrderLimit(): void
    {
        $docs = $this->fs->query('messages')
            ->where('to', '==', 'infra')
            ->where('status', 'in', ['new', 'seen'])
            ->orderBy('n', 'DESC')
            ->limit(5)
            ->get();

        self::assertSame(['m3', 'm1'], self::ids($docs));
        self::assertSame(['m3'], self::ids($this->fs->query('messages')->where('tags', 'array-contains', 'urgent')->get()));
        self::assertSame(['m5'], self::ids($this->fs->query('messages')->where('note', '==', null)->get()));
        self::assertSame(['m6', 'm7'], self::ids($this->fs->query('messages')->where('n', '>', 5)->orderBy('n')->get()));
    }

    public function testCursorsFromADocumentAndFromValues(): void
    {
        $inbox = $this->fs->query('messages')->orderBy('n');

        $page1 = $inbox->limit(3)->get();
        $page2 = $inbox->limit(3)->startAfter($page1[2])->get();

        self::assertSame(['m1', 'm2', 'm3'], self::ids($page1));
        self::assertSame(['m4', 'm5', 'm6'], self::ids($page2));
        self::assertSame(['m3', 'm4'], self::ids($inbox->startAt([3])->endAt([4])->get()));
        self::assertSame(['m4'], self::ids($inbox->startAfter([3])->endBefore([5])->get()));
        self::assertSame(['m3', 'm4', 'm5', 'm6', 'm7'], self::ids($this->fs->query('messages')->offset(2)->orderBy('n')->get()));
    }

    public function testStreamAcrossThreePages(): void
    {
        $docs = iterator_to_array($this->fs->query('messages')->orderBy('n', 'DESC')->stream(3), false);

        self::assertSame(['m7', 'm6', 'm5', 'm4', 'm3', 'm2', 'm1'], self::ids($docs));
        self::assertCount(5, iterator_to_array($this->fs->query('messages')->limit(5)->stream(2), false));
    }

    public function testCountCollectionGroupAndListCollections(): void
    {
        self::assertSame(4, $this->fs->query('messages')->where('status', '==', 'new')->count());
        self::assertSame(0, $this->fs->query('nothing')->count());
        self::assertSame(['r1', 'r3'], self::ids($this->fs->collectionGroup('replies')->where('author', '==', 'infra')->orderBy('author')->get()));
        self::assertSame(['r1'], self::ids($this->fs->query('threads/t1/replies')->get()));
        self::assertSame(['messages', 'threads'], $this->fs->listCollections());
        self::assertSame(['replies'], $this->fs->listCollections('threads/t2'));
    }

    /**
     * @param list<Document> $docs
     * @return list<string>
     */
    private static function ids(array $docs): array
    {
        return array_map(static fn (Document $d): string => $d->id, $docs);
    }
}
