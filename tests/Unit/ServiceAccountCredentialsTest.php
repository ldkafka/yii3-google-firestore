<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Unit;

use HttpSoft\Message\RequestFactory;
use HttpSoft\Message\StreamFactory;
use Ldkafka\GoogleFirestore\Auth\ServiceAccountCredentials;
use Ldkafka\GoogleFirestore\Exception\AuthenticationException;
use Ldkafka\GoogleFirestore\Exception\ConfigurationException;
use Ldkafka\GoogleFirestore\Exception\TransportException;
use Ldkafka\GoogleFirestore\Tests\Support\ArrayCache;
use Ldkafka\GoogleFirestore\Tests\Support\GoogleKeyFixture;
use Ldkafka\GoogleFirestore\Tests\Support\QueueHttpClient;
use PHPUnit\Framework\TestCase;

use function base64_decode;
use function explode;
use function file_get_contents;
use function is_array;
use function json_decode;
use function openssl_verify;
use function parse_str;
use function putenv;
use function str_repeat;
use function strlen;
use function strtr;

/**
 * The service-account JWT exchange, its cache, and its errors.
 */
final class ServiceAccountCredentialsTest extends TestCase
{
    private GoogleKeyFixture $key;

    protected function setUp(): void
    {
        $this->key = new GoogleKeyFixture();
    }

    public function testAssertionCarriesTheClaimsAndAValidSignature(): void
    {
        $jwt = $this->credentials($this->key->path, new QueueHttpClient())->createAssertion(1_800_000_000);
        [$header, $claims, $signature] = explode('.', $jwt);

        self::assertSame(['alg' => 'RS256', 'typ' => 'JWT'], json_decode(self::b64($header), true));
        self::assertSame([
            'iss' => 'firestore-user@example-project.iam.gserviceaccount.com',
            'scope' => 'https://www.googleapis.com/auth/datastore',
            'aud' => ServiceAccountCredentials::TOKEN_ENDPOINT,
            'iat' => 1_800_000_000,
            'exp' => 1_800_003_600,
        ], json_decode(self::b64($claims), true));
        self::assertSame(1, openssl_verify($header . '.' . $claims, self::b64($signature), $this->key->publicKeyPem, OPENSSL_ALGO_SHA256));
        self::assertSame('example-project', $this->credentials($this->key->path, new QueueHttpClient())->getProjectId());
    }

    public function testTokenIsExchangedOnceThroughPsr18(): void
    {
        $http = (new QueueHttpClient())->token('tok-1');
        $credentials = $this->credentials($this->key->path, $http);

        self::assertSame('tok-1', $credentials->getAccessToken());
        $credentials->getAccessToken();
        self::assertCount(1, $http->requests, 'the second call reuses the token');

        $request = $http->requests[0];
        self::assertSame('POST', $request->getMethod());
        self::assertSame(ServiceAccountCredentials::TOKEN_ENDPOINT, (string) $request->getUri());
        self::assertSame('application/x-www-form-urlencoded', $request->getHeaderLine('Content-Type'));
        parse_str((string) $request->getBody(), $form);
        self::assertSame('urn:ietf:params:oauth:grant-type:jwt-bearer', $form['grant_type'] ?? null);
        $assertion = $form['assertion'] ?? null;
        self::assertIsString($assertion);
        self::assertCount(3, explode('.', $assertion));
    }

    public function testInlineKeyArrayIsAccepted(): void
    {
        $credentials = $this->credentials(self::keyArray($this->key), (new QueueHttpClient())->token('inline'));

        self::assertTrue($credentials->isConfigured());
        self::assertSame('inline', $credentials->getAccessToken());
    }

    public function testCacheIsSharedAndBoundToTheKeyIdentity(): void
    {
        $cache = new ArrayCache();
        $http = (new QueueHttpClient())->token('tok-first')->token('tok-second');

        self::assertSame('tok-first', $this->credentials($this->key->path, $http, $cache)->getAccessToken());
        self::assertSame('tok-first', $this->credentials($this->key->path, $http, $cache)->getAccessToken(), 'a new instance with the same key reuses the cached token');
        self::assertCount(1, $http->requests);

        $rotated = new GoogleKeyFixture('other@example-project.iam.gserviceaccount.com');
        self::assertSame('tok-second', $this->credentials($rotated->path, $http, $cache)->getAccessToken(), 'a different key never reuses the token');
        self::assertStringStartsWith('gcf-sa-token-', (string) array_key_first($cache->data));
    }

    public function testATokenNearExpiryIsRefreshed(): void
    {
        $http = (new QueueHttpClient())->token('short', 60)->token('fresh');
        $credentials = $this->credentials($this->key->path, $http);

        self::assertSame('short', $credentials->getAccessToken());
        self::assertSame('fresh', $credentials->getAccessToken(), 'refreshed 120 s before expiry');
    }

    public function testRejectedExchangeIsReportedWithoutTheAssertion(): void
    {
        $http = (new QueueHttpClient())->json(400, ['error' => 'invalid_grant', 'error_description' => 'Invalid JWT Signature.']);

        try {
            $this->credentials($this->key->path, $http)->getAccessToken();
            self::fail('expected exception');
        } catch (AuthenticationException $e) {
            self::assertSame(400, $e->httpStatus);
            self::assertSame('invalid_grant', $e->oauthError);
            self::assertSame('Google token exchange failed (HTTP 400): Invalid JWT Signature.', $e->getMessage());
        }
    }

    public function testNoAnswerFromTheTokenEndpoint(): void
    {
        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('oauth2.googleapis.com/token failed');
        $this->credentials($this->key->path, (new QueueHttpClient())->networkError('Could not resolve host'))->getAccessToken();
    }

    public function testMissingKeyFileIsAClearError(): void
    {
        $credentials = $this->credentials('/nowhere/key.json', new QueueHttpClient());

        self::assertFalse($credentials->isConfigured());
        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('/nowhere/key.json');
        $credentials->getAccessToken();
    }

    public function testInvalidKeyIsAClearError(): void
    {
        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('client_email/private_key missing');
        $this->credentials(['type' => 'service_account'], new QueueHttpClient());
    }

    public function testFromEnvironmentReadsTheStandardVariable(): void
    {
        putenv('GOOGLE_APPLICATION_CREDENTIALS=' . $this->key->path);
        try {
            $credentials = ServiceAccountCredentials::fromEnvironment(new QueueHttpClient(), new RequestFactory(), new StreamFactory());
            self::assertTrue($credentials->isConfigured());
            self::assertSame(['keyFile' => $this->key->path, 'scope' => ServiceAccountCredentials::SCOPE], $credentials->__debugInfo());
        } finally {
            putenv('GOOGLE_APPLICATION_CREDENTIALS');
        }
    }

    /**
     * @param string|array<string, mixed> $key
     */
    private function credentials(string|array $key, QueueHttpClient $http, ?ArrayCache $cache = null): ServiceAccountCredentials
    {
        return new ServiceAccountCredentials($key, $http, new RequestFactory(), new StreamFactory(), $cache);
    }

    /**
     * The fixture's key file, decoded.
     *
     * @return array<string, mixed>
     */
    public static function keyArray(GoogleKeyFixture $key): array
    {
        $data = json_decode((string) file_get_contents($key->path), true);
        $out = [];
        foreach (is_array($data) ? $data : [] as $name => $value) {
            $out[(string) $name] = $value;
        }

        return $out;
    }

    private static function b64(string $data): string
    {
        return (string) base64_decode(strtr($data, '-_', '+/') . str_repeat('=', (4 - strlen($data) % 4) % 4));
    }
}
