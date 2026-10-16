<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Exception;

/**
 * ABORTED: Firestore aborted the request because of contention; a transaction is re-run.
 */
final class AbortedException extends ApiException
{
}
