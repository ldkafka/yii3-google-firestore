<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore;

use InvalidArgumentException;
use Ldkafka\GoogleFirestore\Auth\ServiceAccountCredentials;
use Ldkafka\GoogleFirestore\Exception\AbortedException;
use Ldkafka\GoogleFirestore\Exception\ApiException;
use Ldkafka\GoogleFirestore\Exception\ConfigurationException;
use Ldkafka\GoogleFirestore\Exception\FirestoreException;
use Ldkafka\GoogleFirestore\Exception\TransportException;
use Ldkafka\GoogleFirestore\Internal\ValueCodec;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Log\LoggerInterface;
use stdClass;
use Throwable;

use function array_is_list;
use function array_key_exists;
use function array_keys;
use function array_map;
use function array_slice;
use function count;
use function explode;
use function implode;
use function in_array;
use function is_array;
use function is_int;
use function is_string;
use function json_decode;
use function json_encode;
use function rawurlencode;
use function sprintf;
use function str_contains;
use function str_replace;
use function strlen;
use function substr;

/**
 * Firestore over the REST API v1: documents, queries, batched writes and transactions.
 *
 * Every write goes through `commit` and every read by path through `batchGet`, so no document id
 * sits in a URL. Reads are retried on RESOURCE_EXHAUSTED, UNAVAILABLE, INTERNAL, DEADLINE_EXCEEDED
 * and network errors; a commit only on RESOURCE_EXHAUSTED and (outside a transaction) ABORTED,
 * because those mean nothing was written. A commit that fails with a 5xx or a network error is not
 * retried: it may have been written, so the caller re-reads. Timeouts are the PSR-18 client's.
 */
final class FirestoreClient
{
    public const DEFAULT_DATABASE = '(default)';
    public const API_BASE = 'https://firestore.googleapis.com/v1';
    /** The bearer token the emulator takes (it checks none without --rules). */
    public const EMULATOR_TOKEN = 'owner';

    /** @internal */
    public const CALL_READ = 'read';
    /** @internal */
    public const CALL_COMMIT = 'commit';
    /** @internal */
    public const CALL_TRANSACTION_COMMIT = 'transactionCommit';

    private const RETRYABLE = [
        self::CALL_READ => ['RESOURCE_EXHAUSTED', 'UNAVAILABLE', 'INTERNAL', 'DEADLINE_EXCEEDED'],
        self::CALL_COMMIT => ['RESOURCE_EXHAUSTED', 'ABORTED'],
        self::CALL_TRANSACTION_COMMIT => ['RESOURCE_EXHAUSTED'],
    ];
    private const HTTP_FALLBACK_STATUS = [
        404 => 'NOT_FOUND',
        429 => 'RESOURCE_EXHAUSTED',
        500 => 'INTERNAL',
        502 => 'UNAVAILABLE',
        503 => 'UNAVAILABLE',
        504 => 'DEADLINE_EXCEEDED',
    ];
    private const MAX_SERVER_MESSAGE = 500;
    private const LIST_PAGE_SIZE = 300;

    private ?ValueCodec $codec = null;

    /**
     * @param ClientInterface $http PSR-18 client; set its timeouts.
     * @param RequestFactoryInterface $requests PSR-17 request factory.
     * @param StreamFactoryInterface $streams PSR-17 stream factory.
     * @param ServiceAccountCredentials|null $credentials Null only with the emulator.
     * @param string $projectId '' = the key's project_id (required with the emulator).
     * @param string $databaseId '(default)' or a named database.
     * @param string|null $emulatorHost 'host:port': plain HTTP and "Bearer owner", no login.
     * @param RetryPolicy $retry Retry limits and backoff.
     * @param LoggerInterface|null $logger Gets one warning per retry (method, status, attempt, delay).
     */
    public function __construct(
        private ClientInterface $http,
        private RequestFactoryInterface $requests,
        private StreamFactoryInterface $streams,
        private ?ServiceAccountCredentials $credentials,
        private string $projectId = '',
        private string $databaseId = self::DEFAULT_DATABASE,
        private ?string $emulatorHost = null,
        private RetryPolicy $retry = new RetryPolicy(),
        private ?LoggerInterface $logger = null,
    ) {
        if ($this->emulatorHost === '') {
            $this->emulatorHost = null;
        }
        if ($this->credentials === null && $this->emulatorHost === null) {
            throw new ConfigurationException('Firestore needs service-account credentials or an emulator host.');
        }
        if ($this->databaseId === '') {
            $this->databaseId = self::DEFAULT_DATABASE;
        }
    }

    /**
     * One document by path ('messages/abc'), or null when it doesn't exist.
     *
     * @phpstan-impure
     */
    public function get(string $path): ?Document
    {
        return $this->batchGet([$path], null)[$path];
    }

    /**
     * Several documents in one call.
     *
     * @param list<string> $paths
     * @return array<string, Document|null> Keyed by path, in the order asked; null for a missing one.
     *
     * @phpstan-impure
     */
    public function getAll(array $paths): array
    {
        return $this->batchGet($paths, null);
    }

    /**
     * Create a document; fails with AlreadyExistsException if the id exists.
     *
     * @param array<string, mixed> $data Field values; FieldValue sentinels allowed.
     * @param string|null $id Null = a random 20-character id.
     */
    public function create(string $collection, array $data, ?string $id = null): WriteResult
    {
        $batch = $this->batch();
        $batch->create($collection, $data, $id);

        return $batch->commit()[0];
    }

    /**
     * Replace the document, or with $merge write only the given (nested) fields. Creates it if missing.
     *
     * @param array<string, mixed> $data Field values; FieldValue sentinels allowed.
     */
    public function set(string $path, array $data, bool $merge = false): WriteResult
    {
        $batch = $this->batch();
        $batch->set($path, $data, $merge);

        return $batch->commit()[0];
    }

    /**
     * Update the named fields only: keys are dotted field paths ('meta.claimant') and the update mask
     * is the keys. By default the document must exist (NotFoundException); pass
     * Precondition::updateTime() to write only if nobody changed it since it was read.
     *
     * @param array<string, mixed> $fields Dotted field path => value; FieldValue sentinels allowed.
     */
    public function update(string $path, array $fields, ?Precondition $precondition = null): WriteResult
    {
        $batch = $this->batch();
        $batch->update($path, $fields, $precondition);

        return $batch->commit()[0];
    }

    /**
     * Delete a document (deleting a missing one succeeds unless a precondition says otherwise).
     */
    public function delete(string $path, ?Precondition $precondition = null): void
    {
        $batch = $this->batch();
        $batch->delete($path, $precondition);
        $batch->commit();
    }

    /**
     * A query on one collection: 'messages', or a subcollection 'threads/t1/replies'.
     */
    public function query(string $collectionPath): Query
    {
        $segments = self::segments($collectionPath);
        if (count($segments) % 2 === 0) {
            throw new InvalidArgumentException('A collection path has an odd number of segments.');
        }
        $collectionId = $segments[count($segments) - 1];

        return new Query($this, implode('/', array_slice($segments, 0, -1)), $collectionId, false);
    }

    /**
     * A query on every collection with this id, at any depth.
     */
    public function collectionGroup(string $collectionId): Query
    {
        if ($collectionId === '' || str_contains($collectionId, '/')) {
            throw new InvalidArgumentException('A collection id is one non-empty segment.');
        }

        return new Query($this, '', $collectionId, true);
    }

    /**
     * Collection ids: the root collections, or a document's subcollections. Follows page tokens.
     *
     * @return list<string>
     *
     * @phpstan-impure
     */
    public function listCollections(string $documentPath = ''): array
    {
        if ($documentPath !== '') {
            self::documentSegments($documentPath);
        }
        $ids = [];
        $token = '';
        do {
            $body = ['pageSize' => self::LIST_PAGE_SIZE] + ($token !== '' ? ['pageToken' => $token] : []);
            $answer = $this->call('listCollectionIds', $documentPath, $body, self::CALL_READ);
            foreach (is_array($answer['collectionIds'] ?? null) ? $answer['collectionIds'] : [] as $id) {
                if (is_string($id)) {
                    $ids[] = $id;
                }
            }
            $next = $answer['nextPageToken'] ?? '';
            $token = is_string($next) ? $next : '';
        } while ($token !== '');

        return $ids;
    }

    /**
     * Writes that commit together, all or nothing.
     */
    public function batch(): WriteBatch
    {
        return new WriteBatch($this);
    }

    /**
     * Run a read-write transaction. $fn gets a Transaction for its reads and writes; the writes
     * commit when it returns. On ABORTED (contention) $fn runs again, up to the policy's
     * transactionAttempts, so it must have no side effects outside the Transaction, should read
     * before it writes, and must not swallow FirestoreException. Any other exception rolls back
     * and is rethrown.
     *
     * @template T
     * @param callable(Transaction): T $fn
     * @return T
     */
    public function runTransaction(callable $fn): mixed
    {
        $previous = null;
        for ($run = 1; ; $run++) {
            $id = $this->beginTransaction($previous);
            $transaction = new Transaction($this, $id);
            try {
                $result = $fn($transaction);
                $transaction->batch()->commitIn($id);

                return $result;
            } catch (AbortedException $e) {
                $delay = $this->retry->transactionDelayMs($run);
                if ($delay === null) {
                    throw $e;
                }
                $this->logger?->warning('Firestore transaction aborted; run {run} of {max} in {delayMs} ms', [
                    'run' => $run + 1,
                    'max' => $this->retry->transactionAttempts,
                    'delayMs' => $delay,
                ]);
                $this->retry->sleep($delay);
                $previous = $id;
            } catch (Throwable $e) {
                $this->rollback($id);
                throw $e;
            }
        }
    }

    /**
     * What a dump shows: no client, no token, no key.
     *
     * @return array<string, mixed>
     */
    public function __debugInfo(): array
    {
        return [
            'projectId' => $this->projectId,
            'databaseId' => $this->databaseId,
            'emulatorHost' => $this->emulatorHost,
            'credentials' => $this->credentials?->__debugInfo(),
        ];
    }

    /**
     * Read documents, optionally inside a transaction.
     *
     * @internal
     * @param list<string> $paths
     * @return array<string, Document|null>
     */
    public function batchGet(array $paths, ?string $transaction): array
    {
        $codec = $this->codec();
        $result = [];
        foreach ($paths as $path) {
            self::documentSegments($path);
            $result[$path] = null;
        }
        if ($result === []) {
            return [];
        }
        $body = ['documents' => array_map($codec->documentName(...), array_keys($result))];
        if ($transaction !== null) {
            $body['transaction'] = $transaction;
        }
        foreach ($this->call('batchGet', '', $body, self::CALL_READ) as $item) {
            if (is_array($item) && isset($item['found'])) {
                $document = $codec->decodeDocument($item['found']);
                if (array_key_exists($document->path, $result)) {
                    $result[$document->path] = $document;
                }
            }
        }

        return $result;
    }

    /**
     * Commit writes, optionally ending a transaction.
     *
     * @internal
     * @param list<array<string, mixed>> $writes REST Write objects.
     * @return array<array-key, mixed> The commit response.
     */
    public function commit(array $writes, ?string $transaction): array
    {
        $body = ['writes' => $writes];
        if ($transaction !== null) {
            $body['transaction'] = $transaction;
        }

        return $this->call('commit', '', $body, $transaction === null ? self::CALL_COMMIT : self::CALL_TRANSACTION_COMMIT);
    }

    /**
     * One REST call under the documents root (or a document below it), with retries.
     *
     * @internal
     * @param string $parentPath '' for the database, or a document path for its subcollections.
     * @param array<string, mixed> $body
     * @param string $call One of the CALL_* constants: decides what is retried.
     * @return array<array-key, mixed> The decoded answer (a list for streamed methods).
     */
    public function call(string $method, string $parentPath, array $body, string $call): array
    {
        $codec = $this->codec();
        $name = $codec->documentsRoot();
        if ($parentPath !== '') {
            $name .= '/' . implode('/', array_map(rawurlencode(...), self::segments($parentPath)));
        }
        $url = $this->baseUrl() . '/' . $name . ':' . $method;
        $json = (string) json_encode($body === [] ? new stdClass() : $body, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION);
        $retryable = self::RETRYABLE[$call] ?? [];

        for ($attempt = 1; ; $attempt++) {
            try {
                return $this->send($method, $url, $json);
            } catch (ApiException $e) {
                if (!in_array($e->status, $retryable, true)) {
                    throw $e;
                }
                $status = $e->status;
            } catch (TransportException $e) {
                if ($call !== self::CALL_READ) {
                    throw $e;
                }
                $status = 'NETWORK_ERROR';
            }
            $delay = $this->retry->delayMs($attempt);
            if ($delay === null) {
                throw $e;
            }
            $this->logger?->warning('Firestore {method} failed with {status}; retry {attempt} of {max} in {delayMs} ms', [
                'method' => $method,
                'status' => $status,
                'attempt' => $attempt,
                'max' => $this->retry->maxRetries,
                'delayMs' => $delay,
            ]);
            $this->retry->sleep($delay);
        }
    }

    /**
     * The codec for this database (resolves the project id on first use).
     *
     * @internal
     */
    public function codec(): ValueCodec
    {
        if ($this->codec === null) {
            $project = $this->projectId;
            if ($project === '' && $this->emulatorHost === null) {
                $project = (string) $this->credentials?->getProjectId();
            }
            if ($project === '') {
                throw new ConfigurationException('No Firestore project id: set projectId (required with the emulator) or use a key with project_id.');
            }
            $this->codec = new ValueCodec('projects/' . $project . '/databases/' . $this->databaseId . '/documents');
        }

        return $this->codec;
    }

    /**
     * The segments of a document path, checked.
     *
     * @internal
     * @return list<string>
     */
    public static function documentSegments(string $path): array
    {
        $segments = self::segments($path);
        if (count($segments) % 2 !== 0) {
            throw new InvalidArgumentException('A document path has an even number of segments (collection/id).');
        }

        return $segments;
    }

    /**
     * @return list<string>
     */
    private static function segments(string $path): array
    {
        $segments = explode('/', $path);
        if (in_array('', $segments, true)) {
            throw new InvalidArgumentException('A path has an empty segment.');
        }

        return $segments;
    }

    /**
     * @return array<array-key, mixed>
     */
    private function send(string $method, string $url, string $json): array
    {
        $token = $this->emulatorHost !== null ? self::EMULATOR_TOKEN : $this->credentials?->getAccessToken() ?? '';
        $request = $this->requests->createRequest('POST', $url)
            ->withHeader('Authorization', 'Bearer ' . $token)
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('Accept', 'application/json')
            ->withBody($this->streams->createStream($json));

        try {
            $response = $this->http->sendRequest($request);
        } catch (ClientExceptionInterface $e) {
            $uri = $request->getUri();
            throw new TransportException(sprintf(
                'HTTP request to %s://%s%s failed: %s: %s',
                $uri->getScheme(),
                $uri->getAuthority(),
                $uri->getPath(),
                $e::class,
                str_replace($token, '[redacted]', $e->getMessage()),
            ));
        }

        $httpStatus = $response->getStatusCode();
        $data = json_decode((string) $response->getBody(), true, 512, JSON_BIGINT_AS_STRING);
        // A streamed answer (runQuery) is a list, and its error is an element's `error`, also on a non-2xx answer.
        $listError = null;
        foreach (is_array($data) && array_is_list($data) ? $data : [] as $item) {
            if (is_array($item) && isset($item['error'])) {
                $listError = $item['error'];
                break;
            }
        }
        if ($httpStatus < 200 || $httpStatus >= 300) {
            throw $this->apiError($method, $httpStatus, is_array($data) ? $data['error'] ?? $listError : null);
        }
        if (!is_array($data)) {
            throw ApiException::forStatus($method, $httpStatus, 'UNKNOWN', 'the answer is not JSON');
        }
        if ($listError !== null) {
            $code = is_array($listError) ? $listError['code'] ?? 0 : 0;
            throw $this->apiError($method, is_int($code) ? $code : 0, $listError);
        }

        return $data;
    }

    private function apiError(string $method, int $httpStatus, mixed $error): ApiException
    {
        $status = is_array($error) && is_string($error['status'] ?? null) ? $error['status'] : '';
        if ($status === '') {
            $status = self::HTTP_FALLBACK_STATUS[$httpStatus] ?? 'UNKNOWN';
        }
        $message = is_array($error) && is_string($error['message'] ?? null) ? $error['message'] : '';
        if (strlen($message) > self::MAX_SERVER_MESSAGE) {
            $message = substr($message, 0, self::MAX_SERVER_MESSAGE) . '...';
        }

        return ApiException::forStatus($method, $httpStatus, $status, $message);
    }

    private function baseUrl(): string
    {
        return $this->emulatorHost !== null ? 'http://' . $this->emulatorHost . '/v1' : self::API_BASE;
    }

    private function beginTransaction(?string $retryOf): string
    {
        $readWrite = $retryOf !== null ? ['retryTransaction' => $retryOf] : new stdClass();
        $answer = $this->call('beginTransaction', '', ['options' => ['readWrite' => $readWrite]], self::CALL_READ);
        $id = $answer['transaction'] ?? null;
        if (!is_string($id) || $id === '') {
            throw new FirestoreException('Firestore beginTransaction gave no transaction id.');
        }

        return $id;
    }

    private function rollback(string $transaction): void
    {
        try {
            $this->call('rollback', '', ['transaction' => $transaction], self::CALL_READ);
        } catch (FirestoreException $e) {
            $this->logger?->warning('Firestore rollback failed: {error}', ['error' => $e->getMessage()]);
        }
    }
}
