<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Unit;

use HttpSoft\Message\RequestFactory;
use HttpSoft\Message\StreamFactory;
use Ldkafka\GoogleFirestore\Auth\ServiceAccountCredentials;
use Ldkafka\GoogleFirestore\Exception\AbortedException;
use Ldkafka\GoogleFirestore\Exception\AlreadyExistsException;
use Ldkafka\GoogleFirestore\Exception\ApiException;
use Ldkafka\GoogleFirestore\Exception\ConfigurationException;
use Ldkafka\GoogleFirestore\Exception\FailedPreconditionException;
use Ldkafka\GoogleFirestore\Exception\NotFoundException;
use Ldkafka\GoogleFirestore\FirestoreClient;
use Ldkafka\GoogleFirestore\Tests\Support\ArrayCache;
use Ldkafka\GoogleFirestore\Tests\Support\GoogleKeyFixture;
use Ldkafka\GoogleFirestore\Tests\Support\QueueHttpClient;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * URLs, auth headers, reads by path, error statuses and the token cache.
 */
final class FirestoreClientTest extends TestCase
{
    private const ROOT = 'projects/demo-p/databases/(default)/documents';

    public function testEmulatorUrlAndBearerOwner(): void
    {
        $http = (new QueueHttpClient())->json(200, [['missing' => self::ROOT . '/c/a', 'readTime' => '2026-10-16T13:21:08Z']]);

        self::assertNull($http->client()->get('c/a'));

        $request = $http->requests[0];
        self::assertSame('http://emulator.test:8080/v1/' . self::ROOT . ':batchGet', (string) $request->getUri());
        self::assertSame('Bearer owner', $request->getHeaderLine('Authorization'));
        self::assertSame('application/json', $request->getHeaderLine('Content-Type'));
        self::assertSame(['documents' => [self::ROOT . '/c/a']], $http->body(0));
    }

    public function testProductionUrlForANamedDatabaseWithTheKeysProject(): void
    {
        $key = new GoogleKeyFixture();
        $http = (new QueueHttpClient())->token('tok-1')->json(200, ['collectionIds' => ['messages']]);
        $credentials = new ServiceAccountCredentials($key->path, $http, new RequestFactory(), new StreamFactory());
        $fs = $http->client($credentials, '', 'bus-db');

        self::assertSame(['messages'], $fs->listCollections());

        self::assertSame(ServiceAccountCredentials::TOKEN_ENDPOINT, (string) $http->requests[0]->getUri());
        self::assertSame('https://firestore.googleapis.com/v1/projects/example-project/databases/bus-db/documents:listCollectionIds', (string) $http->requests[1]->getUri());
        self::assertSame('Bearer tok-1', $http->requests[1]->getHeaderLine('Authorization'));
    }

    public function testOneTokenRequestPerLifetimeAndNoneOnASharedCache(): void
    {
        $key = new GoogleKeyFixture();
        $cache = new ArrayCache();
        $http = (new QueueHttpClient())->token('tok-1')
            ->json(200, ['collectionIds' => []])
            ->json(200, ['collectionIds' => []])
            ->json(200, ['collectionIds' => []]);
        $fs = $http->client(new ServiceAccountCredentials($key->path, $http, new RequestFactory(), new StreamFactory(), $cache), 'p');

        $fs->listCollections();
        $fs->listCollections();
        $other = $http->client(new ServiceAccountCredentials($key->path, $http, new RequestFactory(), new StreamFactory(), $cache), 'p');
        $other->listCollections();

        $tokenRequests = 0;
        foreach ($http->requests as $request) {
            $tokenRequests += (string) $request->getUri() === ServiceAccountCredentials::TOKEN_ENDPOINT ? 1 : 0;
        }
        self::assertSame(1, $tokenRequests);
        self::assertSame('Bearer tok-1', $http->requests[3]->getHeaderLine('Authorization'));
    }

    public function testGetAllKeepsTheAskedOrderWithNullForMissing(): void
    {
        $http = (new QueueHttpClient())->json(200, [
            ['missing' => self::ROOT . '/c/zz', 'readTime' => '2026-10-16T13:21:08Z'],
            ['found' => ['name' => self::ROOT . '/c/a', 'fields' => ['x' => ['integerValue' => '1']], 'createTime' => '2026-10-16T13:21:07Z', 'updateTime' => '2026-10-16T13:21:07Z']],
        ]);

        $docs = $http->client()->getAll(['c/a', 'c/zz', 'c/a']);

        self::assertSame(['c/a', 'c/zz'], array_keys($docs));
        self::assertSame(1, $docs['c/a']?->get('x'));
        self::assertNull($docs['c/zz']);
        self::assertSame(['documents' => [self::ROOT . '/c/a', self::ROOT . '/c/zz']], $http->body(0));
        self::assertSame([], $http->client()->getAll([]), 'nothing asked, nothing sent');
    }

    /**
     * @return iterable<string, array{int, string, class-string<ApiException>}>
     */
    public static function statuses(): iterable
    {
        yield 'not found' => [404, 'NOT_FOUND', NotFoundException::class];
        yield 'already exists' => [409, 'ALREADY_EXISTS', AlreadyExistsException::class];
        yield 'failed precondition' => [400, 'FAILED_PRECONDITION', FailedPreconditionException::class];
        yield 'aborted' => [409, 'ABORTED', AbortedException::class];
        yield 'invalid argument' => [400, 'INVALID_ARGUMENT', ApiException::class];
    }

    /**
     * @param class-string<ApiException> $class
     */
    #[DataProvider('statuses')]
    public function testTheBodyStatusPicksTheException(int $http, string $status, string $class): void
    {
        $queue = (new QueueHttpClient())->error($http, $status, 'the stored version (1) does not match the required base version (2)');

        try {
            $queue->client()->get('c/a');
            self::fail('expected exception');
        } catch (ApiException $e) {
            self::assertSame($class, $e::class);
            self::assertSame($status, $e->status);
            self::assertSame($http, $e->httpStatus);
            self::assertSame('batchGet', $e->method);
            self::assertStringContainsString('does not match', $e->getMessage());
        }
    }

    public function testAnErrorInsideAStreamedAnswerThrows(): void
    {
        $http = (new QueueHttpClient())->json(200, [
            ['document' => ['name' => self::ROOT . '/c/a', 'createTime' => '2026-10-16T13:21:07Z', 'updateTime' => '2026-10-16T13:21:07Z']],
            ['error' => ['code' => 400, 'message' => 'The query requires an index.', 'status' => 'FAILED_PRECONDITION']],
        ]);

        $this->expectException(FailedPreconditionException::class);
        $this->expectExceptionMessage('requires an index');
        $http->client()->query('c')->get();
    }

    public function testAnErrorWithoutABodyFallsBackOnTheHttpCode(): void
    {
        $http = (new QueueHttpClient())->json(400, 'not json');

        try {
            $http->client(retry: new \Ldkafka\GoogleFirestore\RetryPolicy(maxRetries: 0))->get('c/a');
            self::fail('expected exception');
        } catch (ApiException $e) {
            self::assertSame('UNKNOWN', $e->status);
        }
    }

    public function testConfigurationIsChecked(): void
    {
        try {
            new FirestoreClient(new QueueHttpClient(), new RequestFactory(), new StreamFactory(), null);
            self::fail('expected exception');
        } catch (ConfigurationException $e) {
            self::assertStringContainsString('credentials or an emulator', $e->getMessage());
        }

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('project id');
        (new QueueHttpClient())->client(projectId: '')->get('c/a');
    }

    public function testListCollectionsFollowsPageTokens(): void
    {
        $http = (new QueueHttpClient())
            ->json(200, ['collectionIds' => ['a'], 'nextPageToken' => 'next'])
            ->json(200, ['collectionIds' => ['b']]);

        self::assertSame(['a', 'b'], $http->client()->listCollections('threads/t1'));
        self::assertSame('/v1/' . self::ROOT . '/threads/t1:listCollectionIds', $http->path(0));
        self::assertSame('next', $http->body(1)['pageToken'] ?? null);
    }

    public function testBadDocumentPathsAreRefused(): void
    {
        $fs = (new QueueHttpClient())->client();
        foreach (['c', 'c/a/b', 'c//a', '/c/a', ''] as $path) {
            try {
                $fs->get($path);
                self::fail('expected InvalidArgumentException for ' . $path);
            } catch (\InvalidArgumentException) {
                self::addToAssertionCount(1);
            }
        }
    }
}
