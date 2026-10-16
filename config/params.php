<?php

declare(strict_types=1);

/**
 * Defaults for Yii3 applications using yiisoft/config. Override in your application's params
 * (keep the key file out of version control).
 */
return [
    'ldkafka/yii3-google-firestore' => [
        // Path to the service-account JSON key. Empty = GOOGLE_APPLICATION_CREDENTIALS.
        'keyFile' => '',
        // Project of the database. Empty = the key's project_id (required with the emulator).
        'projectId' => '',
        // '(default)' or a named database id.
        'databaseId' => '(default)',
        // Emulator 'host:port'. Empty = FIRESTORE_EMULATOR_HOST. When set: no login, plain HTTP.
        'emulatorHost' => '',
        // Use the application's PSR-16 cache (Psr\SimpleCache\CacheInterface) for access tokens.
        'useCache' => true,
        'retry' => ['maxRetries' => 3, 'baseDelayMs' => 200, 'maxDelayMs' => 5000, 'transactionAttempts' => 5],
    ],
];
