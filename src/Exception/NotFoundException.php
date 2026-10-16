<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Exception;

/**
 * NOT_FOUND: The document does not exist (an update, or a delete or write that requires it to exist).
 */
final class NotFoundException extends ApiException
{
}
