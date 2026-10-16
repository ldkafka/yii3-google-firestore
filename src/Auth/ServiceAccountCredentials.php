<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Auth;

use Ldkafka\GoogleFirestore\Exception\AuthenticationException;
use Ldkafka\GoogleFirestore\Exception\ConfigurationException;
use Ldkafka\GoogleFirestore\Exception\TransportException;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\SimpleCache\CacheInterface;
use Throwable;

use function base64_encode;
use function file_exists;
use function file_get_contents;
use function getenv;
use function hash;
use function http_build_query;
use function is_array;
use function is_int;
use function is_readable;
use function is_string;
use function json_decode;
use function json_encode;
use function openssl_pkey_get_private;
use function openssl_sign;
use function rtrim;
use function sprintf;
use function str_replace;
use function strtr;
use function time;

/**
 * OAuth2 access tokens for a Google service account, from its JSON key, without the Google client
 * libraries: a short-lived RS256 JWT is exchanged at the token endpoint through the PSR-18 client.
 *
 * Tokens are cached in memory and, when a PSR-16 cache is given, across processes. The cache key is
 * derived from the key material and the scope, so a replaced or rotated key never reuses a token
 * issued to the previous one. The key file is read lazily, on first use, so a missing file does not
 * break container construction. Neither the key nor the token shows in a dump or a stack trace.
 *
 * @see https://developers.google.com/identity/protocols/oauth2/service-account
 */
final class ServiceAccountCredentials
{
    public const TOKEN_ENDPOINT = 'https://oauth2.googleapis.com/token';
    public const SCOPE = 'https://www.googleapis.com/auth/datastore';

    /** Refresh this many seconds before Google's stated expiry. */
    private const EXPIRY_MARGIN = 120;
    private const CACHE_PREFIX = 'gcf-sa-token-';

    /** @var array{client_email: string, private_key: string, project_id?: string, private_key_id?: string}|null */
    private ?array $key = null;

    private ?string $accessToken = null;

    private int $expiresAt = 0;

    /**
     * @param string|array<string, mixed> $keyFileOrArray Path to the JSON key file, or the decoded key.
     * @param ClientInterface $http PSR-18 client for the token exchange (its timeouts apply).
     * @param RequestFactoryInterface $requests PSR-17 request factory.
     * @param StreamFactoryInterface $streams PSR-17 stream factory.
     * @param CacheInterface|null $cache Optional PSR-16 cache shared across processes.
     */
    public function __construct(
        #[\SensitiveParameter] private string|array $keyFileOrArray,
        private ClientInterface $http,
        private RequestFactoryInterface $requests,
        private StreamFactoryInterface $streams,
        private ?CacheInterface $cache = null,
    ) {
        if (is_array($keyFileOrArray)) {
            $this->key = self::validateKey($keyFileOrArray);
            $this->keyFileOrArray = [];
        }
    }

    /**
     * Credentials from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable (a key file path).
     */
    public static function fromEnvironment(
        ClientInterface $http,
        RequestFactoryInterface $requests,
        StreamFactoryInterface $streams,
        ?CacheInterface $cache = null,
    ): self {
        $path = getenv('GOOGLE_APPLICATION_CREDENTIALS');

        return new self(is_string($path) ? $path : '', $http, $requests, $streams, $cache);
    }

    /**
     * Whether key material is available (a readable file, or an inline key).
     */
    public function isConfigured(): bool
    {
        if ($this->key !== null) {
            return true;
        }
        $path = $this->keyFile();

        return $path !== '' && file_exists($path) && is_readable($path);
    }

    /**
     * Project id recorded in the key, or null when absent.
     *
     * @throws ConfigurationException When the key cannot be loaded.
     */
    public function getProjectId(): ?string
    {
        $key = $this->loadKey();

        return isset($key['project_id']) && $key['project_id'] !== '' ? $key['project_id'] : null;
    }

    /**
     * A valid bearer token, fetched or refreshed as needed.
     *
     * @throws ConfigurationException When the key is unusable.
     * @throws AuthenticationException When Google refuses the exchange.
     * @throws TransportException When the token endpoint gives no answer.
     */
    public function getAccessToken(): string
    {
        $now = time();
        if ($this->accessToken !== null && $this->expiresAt - self::EXPIRY_MARGIN > $now) {
            return $this->accessToken;
        }

        $cacheKey = $this->cacheKey();
        if ($this->cache !== null) {
            try {
                $cached = $this->cache->get($cacheKey);
            } catch (Throwable) {
                $cached = null;
            }
            if (is_array($cached) && is_string($cached['token'] ?? null) && is_int($cached['expires_at'] ?? null)
                && $cached['expires_at'] - self::EXPIRY_MARGIN > $now) {
                $this->accessToken = $cached['token'];
                $this->expiresAt = $cached['expires_at'];

                return $this->accessToken;
            }
        }

        [$token, $expiresIn] = $this->exchange();
        $this->accessToken = $token;
        $this->expiresAt = $now + $expiresIn;

        if ($this->cache !== null) {
            try {
                $this->cache->set($cacheKey, ['token' => $token, 'expires_at' => $this->expiresAt], $expiresIn);
            } catch (Throwable) {
                // Caching is an optimisation only.
            }
        }

        return $token;
    }

    /**
     * Build the signed JWT assertion for the given time (now by default).
     *
     * @throws ConfigurationException When the key cannot be loaded or used.
     */
    public function createAssertion(?int $now = null): string
    {
        $key = $this->loadKey();
        $now ??= time();

        $header = self::base64Url((string) json_encode(['alg' => 'RS256', 'typ' => 'JWT']));
        $claims = self::base64Url((string) json_encode([
            'iss' => $key['client_email'],
            'scope' => self::SCOPE,
            'aud' => self::TOKEN_ENDPOINT,
            'iat' => $now,
            'exp' => $now + 3600,
        ]));
        $input = $header . '.' . $claims;

        $privateKey = openssl_pkey_get_private($key['private_key']);
        if ($privateKey === false) {
            throw new ConfigurationException('The service-account private key could not be parsed.');
        }
        $signature = '';
        if (!openssl_sign($input, $signature, $privateKey, OPENSSL_ALGO_SHA256)) {
            throw new ConfigurationException('Signing the service-account assertion failed.');
        }

        return $input . '.' . self::base64Url($signature);
    }

    /**
     * What a dump shows: the key file path only.
     *
     * @return array{keyFile: string, scope: string}
     */
    public function __debugInfo(): array
    {
        return ['keyFile' => is_string($this->keyFileOrArray) ? $this->keyFileOrArray : '(inline key)', 'scope' => self::SCOPE];
    }

    /**
     * @return array{0: string, 1: int} Access token and its lifetime in seconds.
     */
    private function exchange(): array
    {
        $assertion = $this->createAssertion();
        $request = $this->requests->createRequest('POST', self::TOKEN_ENDPOINT)
            ->withHeader('Content-Type', 'application/x-www-form-urlencoded')
            ->withHeader('Accept', 'application/json')
            ->withBody($this->streams->createStream(http_build_query([
                'grant_type' => 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                'assertion' => $assertion,
            ])));

        try {
            $response = $this->http->sendRequest($request);
        } catch (ClientExceptionInterface $e) {
            throw new TransportException(sprintf(
                'HTTP request to %s failed: %s: %s',
                self::TOKEN_ENDPOINT,
                $e::class,
                str_replace($assertion, '[redacted]', $e->getMessage()),
            ));
        }

        $status = $response->getStatusCode();
        $data = json_decode((string) $response->getBody(), true);
        if ($status !== 200 || !is_array($data) || !is_string($data['access_token'] ?? null)) {
            $error = is_array($data) && is_string($data['error'] ?? null) ? $data['error'] : '';
            $description = is_array($data) && is_string($data['error_description'] ?? null) ? $data['error_description'] : '';
            $detail = $description !== '' ? $description : $error;
            throw new AuthenticationException(
                sprintf('Google token exchange failed (HTTP %d)%s', $status, $detail !== '' ? ': ' . $detail : ''),
                $status,
                $error,
            );
        }
        $expiresIn = $data['expires_in'] ?? 3600;

        return [$data['access_token'], is_int($expiresIn) ? $expiresIn : 3600];
    }

    /**
     * @return array{client_email: string, private_key: string, project_id?: string, private_key_id?: string}
     */
    private function loadKey(): array
    {
        if ($this->key !== null) {
            return $this->key;
        }
        if (!$this->isConfigured()) {
            throw new ConfigurationException(sprintf(
                'Google service-account key file not found or not readable: %s',
                $this->keyFile() === '' ? '(no path configured)' : $this->keyFile(),
            ));
        }

        $data = json_decode((string) file_get_contents($this->keyFile()), true);
        if (!is_array($data)) {
            throw new ConfigurationException('The service-account key file is not valid JSON.');
        }

        return $this->key = self::validateKey($data);
    }

    /**
     * @param array<mixed> $data
     * @return array{client_email: string, private_key: string, project_id?: string, private_key_id?: string}
     */
    private static function validateKey(#[\SensitiveParameter] array $data): array
    {
        if (!is_string($data['client_email'] ?? null) || !is_string($data['private_key'] ?? null)) {
            throw new ConfigurationException('The service-account key is not a valid Google JSON key (client_email/private_key missing).');
        }
        $key = ['client_email' => $data['client_email'], 'private_key' => $data['private_key']];
        if (is_string($data['project_id'] ?? null)) {
            $key['project_id'] = $data['project_id'];
        }
        if (is_string($data['private_key_id'] ?? null)) {
            $key['private_key_id'] = $data['private_key_id'];
        }

        return $key;
    }

    private function keyFile(): string
    {
        return is_string($this->keyFileOrArray) ? $this->keyFileOrArray : '';
    }

    private function cacheKey(): string
    {
        $key = $this->loadKey();
        $identity = $key['client_email'] . '|' . ($key['private_key_id'] ?? '') . '|' . hash('sha256', $key['private_key']);

        return self::CACHE_PREFIX . hash('sha256', $identity . '|' . self::SCOPE);
    }

    private static function base64Url(string $data): string
    {
        return rtrim(strtr(base64_encode($data), '+/', '-_'), '=');
    }
}
