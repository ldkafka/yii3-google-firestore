<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Exception;

/**
 * FAILED_PRECONDITION: A precondition failed (the document changed since it was read), or a query needs an index.
 */
final class FailedPreconditionException extends ApiException
{
}
