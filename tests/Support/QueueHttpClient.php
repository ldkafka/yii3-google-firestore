<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Support;

use HttpSoft\Message\RequestFactory;
use HttpSoft\Message\Response;
use HttpSoft\Message\StreamFactory;
use Ldkafka\GoogleFirestore\Auth\ServiceAccountCredentials;
use Ldkafka\GoogleFirestore\FirestoreClient;
use Ldkafka\GoogleFirestore\RetryPolicy;
use LogicException;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;

use function array_key_exists;
use function array_shift;
use function is_array;
use function json_decode;
use function json_encode;

/**
 * A PSR-18 client that answers from a queue and records every request.
 */
final class QueueHttpClient implements ClientInterface
{
    /** @var list<RequestInterface> */
    public array $requests = [];

    /** @var list<ResponseInterface|ClientExceptionInterface> */
    private array $queue = [];

    /**
     * Queue a JSON answer.
     */
    public function json(int $status, mixed $body): self
    {
        $response = (new Response($status))->withHeader('Content-Type', 'application/json');
        $response->getBody()->write((string) json_encode($body));
        $this->queue[] = $response;

        return $this;
    }

    /**
     * Queue a Google error answer.
     */
    public function error(int $status, string $rpcStatus, string $message = 'test error'): self
    {
        return $this->json($status, ['error' => ['code' => $status, 'message' => $message, 'status' => $rpcStatus]]);
    }

    /**
     * Queue a token endpoint answer.
     */
    public function token(string $token, int $expiresIn = 3600): self
    {
        return $this->json(200, ['access_token' => $token, 'expires_in' => $expiresIn, 'token_type' => 'Bearer']);
    }

    /**
     * Queue a network failure (the client throws).
     */
    public function networkError(string $message): self
    {
        $this->queue[] = new NetworkError($message);

        return $this;
    }

    /**
     * Answer the next queued response, or throw the queued failure.
     */
    public function sendRequest(RequestInterface $request): ResponseInterface
    {
        $this->requests[] = $request;
        $next = array_shift($this->queue) ?? throw new LogicException('No queued answer for ' . $request->getUri()->getPath());
        if ($next instanceof ClientExceptionInterface) {
            if ($next instanceof NetworkError) {
                $next->request = $request;
            }
            throw $next;
        }

        return $next;
    }

    /**
     * The JSON body of request $i, decoded.
     *
     * @return array<mixed>
     */
    public function body(int $i): array
    {
        $request = $this->requests[$i] ?? throw new LogicException('No request ' . $i);
        $data = json_decode((string) $request->getBody(), true);

        return is_array($data) ? $data : [];
    }

    /**
     * The value at a key path in decoded JSON, or null.
     */
    public static function dig(mixed $data, string|int ...$keys): mixed
    {
        foreach ($keys as $key) {
            if (!is_array($data) || !array_key_exists($key, $data)) {
                return null;
            }
            $data = $data[$key];
        }

        return $data;
    }

    /**
     * The path of request $i.
     */
    public function path(int $i): string
    {
        return ($this->requests[$i] ?? throw new LogicException('No request ' . $i))->getUri()->getPath();
    }

    /**
     * A client on this queue: the emulator unless credentials are given.
     */
    public function client(
        ?ServiceAccountCredentials $credentials = null,
        string $projectId = 'demo-p',
        string $databaseId = '(default)',
        ?LoggerInterface $logger = null,
        ?RetryPolicy $retry = null,
    ): FirestoreClient {
        return new FirestoreClient(
            $this,
            new RequestFactory(),
            new StreamFactory(),
            $credentials,
            $projectId,
            $databaseId,
            $credentials === null ? 'emulator.test:8080' : null,
            $retry ?? new RetryPolicy(sleep: static function (int $ms): void {
            }),
            $logger,
        );
    }
}
