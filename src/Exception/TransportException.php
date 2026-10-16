<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Exception;

/**
 * No HTTP answer: the PSR-18 client threw. Its class and message are copied into this message (with
 * any token blanked); it is not kept as the previous exception, because it may carry the request
 * and its Authorization header.
 */
final class TransportException extends FirestoreException
{
}
