<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Tests\Support;

use HttpSoft\Message\Request;
use Psr\Http\Client\NetworkExceptionInterface;
use Psr\Http\Message\RequestInterface;
use RuntimeException;

/**
 * A PSR-18 network error that carries the request, as real clients' do.
 */
final class NetworkError extends RuntimeException implements NetworkExceptionInterface
{
    public RequestInterface $request;

    /**
     * @param string $message The client's message.
     */
    public function __construct(string $message)
    {
        parent::__construct($message);
        $this->request = new Request('GET', 'http://unset.test/');
    }

    /**
     * The request that failed.
     */
    public function getRequest(): RequestInterface
    {
        return $this->request;
    }
}
