<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Unit;

use HttpSoft\Message\RequestFactory;
use HttpSoft\Message\StreamFactory;
use Ldkafka\GoogleFirestore\Auth\ServiceAccountCredentials;
use Ldkafka\GoogleFirestore\FirestoreClient;
use Ldkafka\GoogleFirestore\RetryPolicy;
use Ldkafka\GoogleFirestore\Tests\Support\GoogleKeyFixture;
use Ldkafka\GoogleFirestore\Tests\Support\QueueHttpClient;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Yiisoft\Di\Container;
use Yiisoft\Di\ContainerConfig;

use function array_replace_recursive;
use function getenv;
use function putenv;

/**
 * config/params.php and config/di.php resolve in yiisoft/di.
 */
final class ConfigTest extends TestCase
{
    private const ENV = ['GOOGLE_APPLICATION_CREDENTIALS', 'FIRESTORE_EMULATOR_HOST'];

    /** @var array<string, string|false> */
    private array $saved = [];

    protected function setUp(): void
    {
        foreach (self::ENV as $name) {
            $this->saved[$name] = getenv($name);
            putenv($name);
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->saved as $name => $value) {
            putenv($value === false ? $name : $name . '=' . $value);
        }
    }

    public function testWithAKeyFileTheClientLogsInWithTheKeysProject(): void
    {
        $key = new GoogleKeyFixture();
        $http = (new QueueHttpClient())->token('tok')->json(200, ['collectionIds' => ['messages']]);
        $container = $this->container(['keyFile' => $key->path, 'databaseId' => 'bus-db', 'retry' => ['maxRetries' => 1]], $http);

        $fs = $container->get(FirestoreClient::class);
        self::assertInstanceOf(FirestoreClient::class, $fs);
        self::assertInstanceOf(ServiceAccountCredentials::class, $container->get(ServiceAccountCredentials::class));
        $retry = $container->get(RetryPolicy::class);
        self::assertInstanceOf(RetryPolicy::class, $retry);
        self::assertSame(1, $retry->maxRetries);

        self::assertSame(['messages'], $fs->listCollections());
        self::assertSame('https://firestore.googleapis.com/v1/projects/example-project/databases/bus-db/documents:listCollectionIds', (string) $http->requests[1]->getUri());
    }

    public function testWithAnEmulatorHostThereIsNoCredentialsObject(): void
    {
        putenv('FIRESTORE_EMULATOR_HOST=127.0.0.1:8080');
        $http = (new QueueHttpClient())->json(200, ['collectionIds' => []]);
        $fs = $this->container(['projectId' => 'demo-x'], $http)->get(FirestoreClient::class);
        self::assertInstanceOf(FirestoreClient::class, $fs);

        $fs->listCollections();

        self::assertSame(['projectId' => 'demo-x', 'databaseId' => '(default)', 'emulatorHost' => '127.0.0.1:8080', 'credentials' => null], $fs->__debugInfo());
        self::assertCount(1, $http->requests, 'no token request');
        self::assertSame('Bearer owner', $http->requests[0]->getHeaderLine('Authorization'));
    }

    /**
     * @param array<string, mixed> $override
     */
    private function container(array $override, QueueHttpClient $http): Container
    {
        $params = require __DIR__ . '/../../config/params.php';
        self::assertIsArray($params);
        $own = $params['ldkafka/yii3-google-firestore'] ?? null;
        self::assertIsArray($own);
        $params['ldkafka/yii3-google-firestore'] = array_replace_recursive($own, $override);
        $definitions = (static function (array $params): mixed {
            return require __DIR__ . '/../../config/di.php';
        })($params);
        self::assertIsArray($definitions);

        return new Container(ContainerConfig::create()->withDefinitions([
            ClientInterface::class => $http,
            RequestFactoryInterface::class => RequestFactory::class,
            StreamFactoryInterface::class => StreamFactory::class,
        ] + $definitions));
    }
}
