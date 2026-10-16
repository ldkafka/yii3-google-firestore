<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Support;

use HttpSoft\Message\Response;
use HttpSoft\Message\StreamFactory;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

use function is_array;
use function is_string;
use function json_decode;
use function json_encode;
use function str_ends_with;
use function substr;

/**
 * Passes requests to a real client, except the first $faults commits of a transaction: it rolls
 * that transaction back on the server and answers 409 ABORTED, as contention would.
 */
final class FaultInjectingClient implements ClientInterface
{
    public int $injected = 0;

    /**
     * @param ClientInterface $inner The real client.
     * @param int $faults How many transaction commits to abort.
     */
    public function __construct(private ClientInterface $inner, private int $faults = 1)
    {
    }

    /**
     * Send, or abort a transaction commit.
     */
    public function sendRequest(RequestInterface $request): ResponseInterface
    {
        $path = $request->getUri()->getPath();
        if ($this->faults > 0 && str_ends_with($path, ':commit')) {
            $body = json_decode((string) $request->getBody(), true);
            $transaction = is_array($body) ? $body['transaction'] ?? null : null;
            if (is_string($transaction)) {
                $this->faults--;
                $this->injected++;
                $rollback = $request
                    ->withUri($request->getUri()->withPath(substr($path, 0, -7) . ':rollback'))
                    ->withBody((new StreamFactory())->createStream((string) json_encode(['transaction' => $transaction])));
                $this->inner->sendRequest($rollback);

                $response = (new Response(409))->withHeader('Content-Type', 'application/json');
                $response->getBody()->write((string) json_encode(['error' => [
                    'code' => 409,
                    'message' => 'Too much contention on these documents. Please try again.',
                    'status' => 'ABORTED',
                ]]));

                return $response;
            }
        }

        return $this->inner->sendRequest($request);
    }
}
