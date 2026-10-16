<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Exception;

/**
 * No credentials and no emulator, an unusable service-account key, or no project id.
 */
final class ConfigurationException extends FirestoreException
{
}
