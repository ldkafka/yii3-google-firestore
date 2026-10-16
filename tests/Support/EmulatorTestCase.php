<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Support;

use HttpSoft\Message\RequestFactory;
use HttpSoft\Message\ResponseFactory;
use HttpSoft\Message\StreamFactory;
use Ldkafka\GoogleFirestore\FirestoreClient;
use Ldkafka\GoogleFirestore\RetryPolicy;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;
use Symfony\Component\HttpClient\HttpClient;
use Symfony\Component\HttpClient\Psr18Client;

use function getenv;
use function is_string;
use function microtime;
use function usleep;

/**
 * Integration tests against the Firestore emulator at FIRESTORE_EMULATOR_HOST; skipped without it.
 * Waits for the emulator once, and clears its data before each test.
 */
abstract class EmulatorTestCase extends TestCase
{
    public const PROJECT = 'demo-yii3-firestore';

    private const WAIT_SECONDS = 60;

    private static bool $ready = false;

    protected FirestoreClient $fs;

    protected string $host = '';

    protected function setUp(): void
    {
        $host = getenv('FIRESTORE_EMULATOR_HOST');
        if (!is_string($host) || $host === '') {
            self::markTestSkipped('FIRESTORE_EMULATOR_HOST is not set.');
        }
        $this->host = $host;
        $http = self::http();
        $requests = new RequestFactory();
        if (!self::$ready) {
            $deadline = microtime(true) + self::WAIT_SECONDS;
            while (!self::$ready) {
                try {
                    self::$ready = $http->sendRequest($requests->createRequest('GET', 'http://' . $host . '/'))->getStatusCode() === 200;
                } catch (ClientExceptionInterface) {
                    if (microtime(true) > $deadline) {
                        self::fail('The emulator at ' . $host . ' did not answer.');
                    }
                    usleep(500_000);
                }
            }
        }
        $reset = $http->sendRequest($requests->createRequest(
            'DELETE',
            'http://' . $host . '/emulator/v1/projects/' . self::PROJECT . '/databases/(default)/documents',
        ));
        self::assertSame(200, $reset->getStatusCode(), 'the emulator reset');
        $this->fs = $this->client();
    }

    /**
     * A client on the emulator.
     */
    protected function client(?ClientInterface $http = null, ?RetryPolicy $retry = null): FirestoreClient
    {
        return new FirestoreClient(
            $http ?? self::http(),
            new RequestFactory(),
            new StreamFactory(),
            null,
            self::PROJECT,
            emulatorHost: $this->host,
            retry: $retry ?? new RetryPolicy(),
        );
    }

    /**
     * The real PSR-18 client of the integration suite (Symfony, with timeouts).
     */
    public static function http(): ClientInterface
    {
        return new Psr18Client(HttpClient::create(['timeout' => 10, 'max_duration' => 30]), new ResponseFactory(), new StreamFactory());
    }
}
