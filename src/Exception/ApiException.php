<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Exception;

/**
 * Firestore answered with an error. The status is the body's `error.status` (e.g. 'UNAVAILABLE');
 * the HTTP code is only a fallback, because 400 and 409 each stand for two statuses.
 */
class ApiException extends FirestoreException
{
    /**
     * @param string $message Safe text: method, status and the server's message.
     * @param string $method The REST method, e.g. 'commit' or 'runQuery'.
     * @param int $httpStatus The HTTP status of the answer.
     * @param string $status The google.rpc status name, e.g. 'FAILED_PRECONDITION'.
     */
    final public function __construct(
        string $message,
        public readonly string $method,
        public readonly int $httpStatus,
        public readonly string $status,
    ) {
        parent::__construct($message, $httpStatus);
    }

    /**
     * The exception for a status: one of the subclasses callers branch on, or ApiException itself.
     */
    public static function forStatus(string $method, int $httpStatus, string $status, string $serverMessage): self
    {
        $class = match ($status) {
            'NOT_FOUND' => NotFoundException::class,
            'ALREADY_EXISTS' => AlreadyExistsException::class,
            'FAILED_PRECONDITION' => FailedPreconditionException::class,
            'ABORTED' => AbortedException::class,
            default => self::class,
        };
        $message = 'Firestore ' . $method . ' failed: ' . $status . ' (HTTP ' . $httpStatus . ')'
            . ($serverMessage !== '' ? ': ' . $serverMessage : '');

        return new $class($message, $method, $httpStatus, $status);
    }
}
