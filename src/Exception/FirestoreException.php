<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Exception;

use RuntimeException;

/**
 * Base of every exception this package throws on purpose (bad arguments throw \InvalidArgumentException).
 */
class FirestoreException extends RuntimeException
{
}
