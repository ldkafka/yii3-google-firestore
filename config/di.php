<?php

declare(strict_types=1);

use Ldkafka\GoogleFirestore\Auth\ServiceAccountCredentials;
use Ldkafka\GoogleFirestore\FirestoreClient;
use Ldkafka\GoogleFirestore\RetryPolicy;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Log\LoggerInterface;
use Psr\SimpleCache\CacheInterface;
use Yiisoft\Definitions\Reference;

/**
 * The application binds ClientInterface (with its timeouts), RequestFactoryInterface and
 * StreamFactoryInterface.
 *
 * @var array<string, mixed> $params
 */

$config = (array) ($params['ldkafka/yii3-google-firestore'] ?? []);
$string = static fn (mixed $value): string => is_string($value) ? $value : '';

$keyFile = $string($config['keyFile'] ?? '');
if ($keyFile === '') {
    $env = getenv('GOOGLE_APPLICATION_CREDENTIALS');
    $keyFile = is_string($env) ? $env : '';
}
$emulator = $string($config['emulatorHost'] ?? '');
if ($emulator === '') {
    $env = getenv('FIRESTORE_EMULATOR_HOST');
    $emulator = is_string($env) ? $env : '';
}
$psr = [
    'http' => Reference::to(ClientInterface::class),
    'requests' => Reference::to(RequestFactoryInterface::class),
    'streams' => Reference::to(StreamFactoryInterface::class),
];

return [
    ServiceAccountCredentials::class => [
        '__construct()' => ['keyFileOrArray' => $keyFile] + $psr + [
            'cache' => !empty($config['useCache']) ? Reference::optional(CacheInterface::class) : null,
        ],
    ],

    RetryPolicy::class => [
        '__construct()' => (array) ($config['retry'] ?? []),
    ],

    FirestoreClient::class => [
        '__construct()' => $psr + [
            'credentials' => $emulator === '' ? Reference::to(ServiceAccountCredentials::class) : null,
            'projectId' => $string($config['projectId'] ?? ''),
            'databaseId' => $string($config['databaseId'] ?? '') ?: FirestoreClient::DEFAULT_DATABASE,
            'emulatorHost' => $emulator !== '' ? $emulator : null,
            'retry' => Reference::to(RetryPolicy::class),
            'logger' => Reference::optional(LoggerInterface::class),
        ],
    ],
];
