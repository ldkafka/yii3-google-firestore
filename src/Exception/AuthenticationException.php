<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Exception;

/**
 * The Google token endpoint refused the service-account key.
 */
final class AuthenticationException extends FirestoreException
{
    /**
     * @param string $message Safe text: the HTTP status and Google's OAuth error, never the assertion.
     * @param int $httpStatus The token endpoint's HTTP status.
     * @param string $oauthError The OAuth `error` code (e.g. 'invalid_grant'), or ''.
     */
    public function __construct(string $message, public readonly int $httpStatus, public readonly string $oauthError = '')
    {
        parent::__construct($message, $httpStatus);
    }
}
