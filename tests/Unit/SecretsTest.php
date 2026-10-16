<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Unit;

use HttpSoft\Message\RequestFactory;
use HttpSoft\Message\StreamFactory;
use Ldkafka\GoogleFirestore\Auth\ServiceAccountCredentials;
use Ldkafka\GoogleFirestore\Exception\AuthenticationException;
use Ldkafka\GoogleFirestore\Exception\ConfigurationException;
use Ldkafka\GoogleFirestore\Exception\FirestoreException;
use Ldkafka\GoogleFirestore\Exception\TransportException;
use Ldkafka\GoogleFirestore\FirestoreClient;
use Ldkafka\GoogleFirestore\RetryPolicy;
use Ldkafka\GoogleFirestore\Tests\Support\ArrayLogger;
use Ldkafka\GoogleFirestore\Tests\Support\GoogleKeyFixture;
use Ldkafka\GoogleFirestore\Tests\Support\QueueHttpClient;
use PHPUnit\Framework\TestCase;
use Throwable;

use function ini_set;
use function print_r;
use function substr;

/**
 * No token or key in exception messages, stack traces, the previous chain, dumps or log lines.
 */
final class SecretsTest extends TestCase
{
    private const TOKEN = 'ya29.secret-access-token-0123456789';

    private string|false $ignoreArgs = false;

    private GoogleKeyFixture $key;

    protected function setUp(): void
    {
        // Traces carry arguments, as on a development box.
        $this->ignoreArgs = ini_set('zend.exception_ignore_args', '0');
        $this->key = new GoogleKeyFixture();
    }

    protected function tearDown(): void
    {
        if ($this->ignoreArgs !== false) {
            ini_set('zend.exception_ignore_args', $this->ignoreArgs);
        }
    }

    public function testANetworkErrorThatEchoesTheTokenIsRedacted(): void
    {
        $logger = new ArrayLogger();
        $http = (new QueueHttpClient())
            ->token(self::TOKEN)
            ->networkError('Failed with header Authorization: Bearer ' . self::TOKEN)
            ->networkError('Failed with header Authorization: Bearer ' . self::TOKEN)
            ->error(503, 'UNAVAILABLE', 'Service unavailable')
            ->networkError('Failed with header Authorization: Bearer ' . self::TOKEN);
        $fs = $http->client($this->credentials($http), 'p', logger: $logger);

        try {
            $fs->get('messages/m1');
            self::fail('expected exception');
        } catch (TransportException $e) {
            $this->assertClean($e);
            self::assertStringContainsString('[redacted]', $e->getMessage());
            self::assertNull($e->getPrevious(), 'the client exception, which carries the request, is not kept');
        }
        self::assertCount(3, $logger->records);
        self::assertStringNotContainsString(self::TOKEN, $logger->dump());
        self::assertStringNotContainsString(self::keyPart(), $logger->dump());
    }

    public function testApiErrorsAndCommitFailuresCarryNoToken(): void
    {
        $http = (new QueueHttpClient())->token(self::TOKEN)->error(400, 'FAILED_PRECONDITION', 'the stored version does not match');
        $fs = $http->client($this->credentials($http), 'p');

        try {
            $fs->update('messages/m1', ['status' => 'claimed']);
            self::fail('expected exception');
        } catch (FirestoreException $e) {
            $this->assertClean($e);
        }
    }

    public function testATokenRefusalCarriesNoKeyOrAssertion(): void
    {
        $http = (new QueueHttpClient())->json(400, ['error' => 'invalid_grant', 'error_description' => 'Invalid JWT Signature.']);

        try {
            $http->client($this->credentials($http), 'p')->get('c/a');
            self::fail('expected exception');
        } catch (AuthenticationException $e) {
            $this->assertClean($e);
            parse_str((string) $http->requests[0]->getBody(), $form);
            $assertion = $form['assertion'] ?? null;
            self::assertIsString($assertion);
            self::assertStringNotContainsString($assertion, print_r($e->getTrace(), true));
        }
    }

    public function testAnInlineKeyIsRedactedInTraces(): void
    {
        $key = ServiceAccountCredentialsTest::keyArray($this->key);
        unset($key['client_email']);

        try {
            new ServiceAccountCredentials($key, new QueueHttpClient(), new RequestFactory(), new StreamFactory());
            self::fail('expected exception');
        } catch (ConfigurationException $e) {
            $this->assertClean($e);
        }
    }

    public function testDumpsShowNoTokenOrKey(): void
    {
        $key = ServiceAccountCredentialsTest::keyArray($this->key);
        $http = (new QueueHttpClient())->token(self::TOKEN)->json(200, ['collectionIds' => []]);
        $inline = new ServiceAccountCredentials($key, $http, new RequestFactory(), new StreamFactory());
        $fs = new FirestoreClient($http, new RequestFactory(), new StreamFactory(), $inline, 'p', retry: new RetryPolicy());
        $fs->listCollections();

        $fromFile = $this->credentials((new QueueHttpClient())->token(self::TOKEN));
        $fromFile->getAccessToken();

        foreach ([$fs, $inline, $fromFile] as $object) {
            $dump = print_r($object, true);
            self::assertStringNotContainsString(self::TOKEN, $dump);
            self::assertStringNotContainsString(self::keyPart(), $dump);
            self::assertStringNotContainsString('PRIVATE KEY', $dump);
        }
        self::assertStringContainsString($this->key->path, print_r($fromFile, true));
        self::assertStringContainsString('(inline key)', print_r($fs, true));
    }

    /**
     * A distinctive slice of the private key's base64 body (not kept in a property: PHPUnit's own
     * frames put this test object in every trace).
     */
    private static function keyPart(): string
    {
        return substr(GoogleKeyFixture::PRIVATE_KEY, 100, 40);
    }

    private function credentials(QueueHttpClient $http): ServiceAccountCredentials
    {
        return new ServiceAccountCredentials($this->key->path, $http, new RequestFactory(), new StreamFactory());
    }

    private function assertClean(Throwable $e): void
    {
        for ($current = $e; $current !== null; $current = $current->getPrevious()) {
            foreach ([self::TOKEN, self::keyPart(), 'PRIVATE KEY'] as $secret) {
                self::assertStringNotContainsString($secret, $current->getMessage());
                self::assertStringNotContainsString($secret, $current->getTraceAsString());
                self::assertStringNotContainsString($secret, print_r($current->getTrace(), true));
            }
        }
    }
}
