# yii3-google-firestore

[![tests](https://github.com/ldkafka/yii3-google-firestore/actions/workflows/tests.yml/badge.svg)](https://github.com/ldkafka/yii3-google-firestore/actions/workflows/tests.yml)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)

A Google Cloud Firestore client for PHP 8.4 over the REST API v1, with a thin Yii3 config layer.
It needs no gRPC extension, no Google SDK and no Guzzle: only the PSR interfaces for HTTP (PSR-18
client, PSR-17 factories), cache (PSR-16) and logging (PSR-3).

- Documents: get, batch get, create, set, update with a field mask, delete.
- Structured queries: filters, order, limit, offset, cursors, page-by-page streaming, count,
  collection-group queries, collection lists.
- Safe writes: `exists` and `updateTime` preconditions, atomic batches, read-write transactions that
  re-run on contention.
- Field transforms: server timestamp, increment, array union, array remove.
- Every Firestore value type, both ways.
- Service-account login (JWT bearer grant) with a shared token cache, and the Firestore emulator.

Status: 0.1. The API may change before 1.0 (the CHANGELOG says how). Plans: [ROADMAP.md](ROADMAP.md).

## Requirements

- PHP 8.4 with `openssl` and `json`.
- Any PSR-18 client and PSR-17 request and stream factories.
- A Firestore database in Native mode.
- A service-account JSON key with data access to it (for example `roles/datastore.user`).

## Installation

```bash
composer require ldkafka/yii3-google-firestore
```

Then pick a PSR-18 client. **Set its timeouts:** PSR-18 has no timeout setting, so they are the
client's, and the token exchange goes through the same client. The package bounds only the retries
per call, the delay between them and the runs of a transaction.

Symfony HttpClient (with any PSR-17 implementation, here httpsoft/http-message):

```php
use HttpSoft\Message\RequestFactory;
use HttpSoft\Message\ResponseFactory;
use HttpSoft\Message\StreamFactory;
use Symfony\Component\HttpClient\HttpClient;
use Symfony\Component\HttpClient\Psr18Client;

$http = new Psr18Client(HttpClient::create(['timeout' => 10, 'max_duration' => 30]), new ResponseFactory(), new StreamFactory());
$requests = new RequestFactory();
$streams = new StreamFactory();
```

Guzzle 7 (it is its own PSR-17 factory). It waits forever by default, so always pass timeouts:

```php
$http = new GuzzleHttp\Client(['connect_timeout' => 10, 'timeout' => 30]);
$requests = $streams = new GuzzleHttp\Psr7\HttpFactory();
```

## Quick start (plain PHP)

```php
use Ldkafka\GoogleFirestore\Auth\ServiceAccountCredentials;
use Ldkafka\GoogleFirestore\FirestoreClient;

$credentials = new ServiceAccountCredentials('/run/secrets/firestore.json', $http, $requests, $streams);
$fs = new FirestoreClient($http, $requests, $streams, $credentials);   // project from the key, database (default)

$result = $fs->create('messages', ['to' => 'infra', 'subject' => 'Feed 13752', 'status' => 'new']);
$message = $fs->get($result->path);
echo $message?->get('subject');
```

`ServiceAccountCredentials::fromEnvironment($http, $requests, $streams)` reads the key path from
`GOOGLE_APPLICATION_CREDENTIALS`, and the constructor also takes the decoded key as an array. Pass a
PSR-16 cache as the last argument to share the access token between processes; the cache key is
bound to the key's identity, so a rotated key never reuses a token. Tokens are refreshed 120 seconds
before they expire. A named database: `new FirestoreClient(..., $credentials, databaseId: 'bus-db')`.

## Documents

Paths are relative to the database: `'messages/abc'`, or `'threads/t1/replies/r1'` below a
document. Ids may hold any character except `/`; they never go into a URL.

```php
use Ldkafka\GoogleFirestore\Precondition;

$doc = $fs->get('messages/abc');                       // Document or null
$docs = $fs->getAll(['messages/a', 'messages/b']);    // ['messages/a' => Document|null, ...] in the order asked

$fs->create('messages', $data);                        // random 20-character id, made on the client
$fs->create('messages', $data, 'abc');                 // AlreadyExistsException if 'messages/abc' exists

$fs->set('messages/abc', $data);                       // replace (creates it if missing)
$fs->set('messages/abc', ['meta' => ['seen' => true]], merge: true);   // only meta.seen changes

$fs->update('messages/abc', ['status' => 'seen', 'meta.by' => 'steward']);  // NotFoundException if missing
$fs->update('messages/abc', ['status' => 'done'], Precondition::updateTime($doc->updateTime));

$fs->delete('messages/abc');
$fs->delete('messages/abc', Precondition::exists());   // NotFoundException if missing
```

- **`Document`** has `path`, `id`, `data` (decoded fields), `createTime`, `updateTime`, and
  `get('meta.by')` for a dotted path (null when absent).
- **`update()`** keys are dotted field paths, and the update mask is the keys. Without a
  precondition the document must exist. A segment that isn't a simple name (`a-b`, `1x`) is quoted
  for you; a field name with a literal dot can only be written with `set(merge: true)` and nested
  arrays.
- **Writes return a `WriteResult`:** `path`, `updateTime` (null for a delete) and
  `transformResults`.

## Queries

```php
$inbox = $fs->query('messages')
    ->where('to', '==', 'infra')
    ->where('status', 'in', ['new', 'seen'])
    ->orderBy('createdAt', 'DESC');

$page1 = $inbox->limit(20)->get();                                       // list<Document>
$page2 = $inbox->limit(20)->startAfter($page1[count($page1) - 1])->get(); // cursor from the last document
foreach ($inbox->stream(pageSize: 200) as $doc) { /* every match, page by page */ }
$open = $inbox->count();

$fs->query('threads/t1/replies')->get();                                 // a subcollection
$fs->collectionGroup('replies')->where('author', '==', 'infra')->get();  // every 'replies' collection
$fs->listCollections();                                                  // ['messages', 'threads']
$fs->listCollections('threads/t1');                                      // a document's subcollections
```

| Operator | Wire | Operator | Wire |
|---|---|---|---|
| `==` | EQUAL | `array-contains` | ARRAY_CONTAINS |
| `!=` | NOT_EQUAL | `array-contains-any` | ARRAY_CONTAINS_ANY |
| `<` `<=` | LESS_THAN(_OR_EQUAL) | `in` | IN |
| `>` `>=` | GREATER_THAN(_OR_EQUAL) | `not-in` | NOT_IN |

- Several `where()` calls are ANDed. `== null`, `!= null`, `== NAN` and `!= NAN` become the
  IS_NULL, IS_NOT_NULL, IS_NAN and IS_NOT_NAN filters.
- Every call returns a changed copy; the original query stays as it was.
- **Cursors** (`startAt`, `startAfter`, `endAt`, `endBefore`) take a `Document` or the values of the
  `orderBy` fields in order. A document cursor also orders by `__name__` (in the last order's
  direction), so ties are broken the same way on every page. `stream()` pages that way.
- **Page with cursors, not `offset()`:** Firestore still reads, and bills, the documents an offset
  skips.
- **Indexes:** in production most queries that combine filters and orders need a composite index.
  Without it the query fails with `FailedPreconditionException` (the same class as a precondition
  failure). The emulator doesn't check indexes.

## Batches and transactions

```php
use Ldkafka\GoogleFirestore\FieldValue;
use Ldkafka\GoogleFirestore\Transaction;
use Ldkafka\GoogleFirestore\Value\Reference;

$batch = $fs->batch();
$path = $batch->create('messages', ['to' => 'steward', 'createdAt' => FieldValue::serverTimestamp()]);
$batch->update('threads/t1', ['last' => new Reference($path)]);
$batch->delete('messages/old-1');
$results = $batch->commit();             // list<WriteResult>, one per write, in order; all or nothing

$n = $fs->runTransaction(function (Transaction $tx): int {
    $counter = $tx->get('counters/messages');
    $n = (int) ($counter?->get('n') ?? 0) + 1;
    $tx->set('counters/messages', ['n' => $n]);
    return $n;
});
```

A `Transaction` has `get()`, `getAll()`, `runQuery($query)` (reads in the transaction) and the
four writes of a batch, which commit when the callable returns. When Firestore answers ABORTED
(contention), the callable runs again with `retryTransaction`, up to `transactionAttempts` runs
(default 5). Any other exception rolls the transaction back and is rethrown. So the callable:

- may run more than once, and must have no side effects outside `$tx`;
- should do its reads before its writes;
- must not swallow `FirestoreException`.

## Field transforms

```php
$fs->update('messages/' . $id, [
    'status'    => 'done',
    'doneAt'    => FieldValue::serverTimestamp(),
    'attempts'  => FieldValue::increment(1),
    'labels'    => FieldValue::arrayUnion('infra', 'urgent'),
    'watchers'  => FieldValue::arrayRemove('steward'),
    'meta'      => ['at' => FieldValue::serverTimestamp()],
]);
```

Sentinels work in `create()`, `set()` and `update()`, at any depth, and their paths stay out of the
update mask, so an update made only of transforms changes nothing else. `$result->transformResults`
holds what the server set (`'doneAt' => Timestamp`, `'attempts' => 2`); the array transforms give
null. The server time has millisecond precision. `increment()` on a missing field starts from 0;
`arrayRemove()` removes every copy and turns a missing field into an empty array.

## Values

| Firestore | Read as | Written from |
|---|---|---|
| nullValue | `null` | `null` |
| booleanValue | `bool` | `bool` |
| integerValue (int64) | `int` | `int` |
| doubleValue | `float` | `float`; NAN and ±INF too |
| timestampValue | `Value\Timestamp` | `Timestamp`, any `DateTimeInterface` |
| stringValue | `string` | `string` (valid UTF-8) |
| bytesValue | `Value\Bytes` | `Bytes` (raw binary) |
| referenceValue | `Value\Reference` (relative path) | `Reference('messages/abc')` |
| geoPointValue | `Value\GeoPoint` | `GeoPoint` |
| arrayValue | list | a list; `[]` is an empty array |
| mapValue | `array<string, mixed>` | an associative array; `new stdClass()` for an empty map |

- `Timestamp` keeps nanoseconds, so an `updateTime` you read goes back unchanged in a precondition.
  Firestore stores timestamp fields to the microsecond and truncates the rest.
- Binary data goes in `Bytes`; a non-UTF-8 string is refused.
- A reference to another database reads as its document path only.
- A PHP value with no Firestore type (a resource, another object) is an `\InvalidArgumentException`.

## Concurrency: claiming a message

Two agents read the same new message; each claims it with the version it read. The first write
changes the version, so the second is refused: exactly one wins.

```php
use Ldkafka\GoogleFirestore\Exception\FailedPreconditionException;
use Ldkafka\GoogleFirestore\Exception\NotFoundException;

$candidates = $fs->query('messages')
    ->where('to', '==', 'infra')->where('status', '==', 'new')
    ->orderBy('createdAt')->limit(5)->get();

foreach ($candidates as $msg) {
    try {
        $fs->update($msg->path, [
            'status'    => 'claimed',
            'claimant'  => 'infra',
            'claimedAt' => FieldValue::serverTimestamp(),
        ], Precondition::updateTime($msg->updateTime));
        handle($msg);                     // only one agent gets here for this message
        break;
    } catch (FailedPreconditionException | NotFoundException) {
        continue;                         // another agent changed or removed it first
    }
}
```

**When the outcome is unknown** (a 5xx `ApiException` or a `TransportException` from the commit),
the write may have happened. Re-read the document and check `claimant`. A blind retry of a claim
that won would be refused, and the winner would think it lost; that is why commits are never retried
on those errors.

## Errors and retries

| Exception | When |
|---|---|
| `FirestoreException` | base of everything thrown on purpose |
| `ConfigurationException` | no credentials and no emulator; key missing, unreadable or invalid; no project id |
| `AuthenticationException` | the token endpoint refused the key (`httpStatus`, `oauthError`) |
| `TransportException` | no HTTP answer: the PSR-18 client threw |
| `ApiException` | Firestore answered with an error (`method`, `httpStatus`, `status`) |
| `NotFoundException` | NOT_FOUND: update or `exists: true` on a missing document |
| `AlreadyExistsException` | ALREADY_EXISTS: create, or `exists: false`, on an existing document |
| `FailedPreconditionException` | FAILED_PRECONDITION: an `updateTime` mismatch, or a query that needs an index |
| `AbortedException` | ABORTED: contention |

The status comes from the body's `error.status`, because HTTP 400 and 409 each stand for two
statuses. Bad arguments (a bad path, an unknown operator) throw `\InvalidArgumentException`.

| Call | Retried on |
|---|---|
| batchGet, runQuery, runAggregationQuery, listCollectionIds, beginTransaction, rollback | RESOURCE_EXHAUSTED (429), UNAVAILABLE (503, and a bare 502), INTERNAL (500), DEADLINE_EXCEEDED (504), network errors |
| commit outside a transaction | RESOURCE_EXHAUSTED, ABORTED (both mean nothing was written) |
| commit of a transaction | RESOURCE_EXHAUSTED (ABORTED re-runs the whole transaction) |

`RetryPolicy` sets the limits: `maxRetries` 3 per call, backoff `min(maxDelayMs, baseDelayMs × 2^(n−1))`
with a jitter of 0.75 to 1 (base 200 ms, cap 5 s), and `transactionAttempts` 5. Each retry logs one
PSR-3 warning with the method, the status, the attempt and the delay. `Retry-After` is not read.

## The emulator

```bash
docker run -d --rm --name firestore-emulator -p 127.0.0.1:8080:8080 \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud emulators firestore start --host-port=0.0.0.0:8080
export FIRESTORE_EMULATOR_HOST=127.0.0.1:8080
```

```php
$fs = new FirestoreClient($http, $requests, $streams, null, 'demo-project', emulatorHost: '127.0.0.1:8080');
```

What the emulator does (checked on the image above, Standard edition, Native mode):

- It starts with that one command, without a prompt or `--project`; any project id works.
  `GET /` answers `200 Ok` once it is up.
- With the emulator the client uses plain HTTP and sends `Authorization: Bearer owner`. Without
  `--rules`, the emulator also accepts requests with no token or any other token.
- Named databases work: any database id is served, separately from `(default)`.
- Clear one database's data with
  `DELETE http://HOST/emulator/v1/projects/PROJECT/databases/DATABASE/documents` (wait for the
  answer); the data also goes when the container stops.
- Timestamps come back with six fraction digits; a nine-digit `updateTime` is accepted in a
  precondition.
- It answers ABORTED on its own under contention ("Transaction lock timeout.", after about two
  seconds), so transaction re-runs can be tested against it. It checks no indexes and no IAM.

If port 8080 on the host is taken, run the PHP side in a container on the emulator's network:
`docker run --rm --network container:firestore-emulator -v "$PWD":/app -w /app -e FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 php:8.4-cli-alpine php vendor/bin/phpunit --testsuite integration`.

## Yii3

With `yiisoft/config` the package registers `config/params.php` and `config/di.php`:

```php
// config/params.php of your application
'ldkafka/yii3-google-firestore' => [
    'keyFile' => '/run/secrets/firestore.json',   // empty = GOOGLE_APPLICATION_CREDENTIALS
    'projectId' => '',                            // empty = the key's project_id
    'databaseId' => '(default)',
    'emulatorHost' => '',                         // empty = FIRESTORE_EMULATOR_HOST; set = no login
    'useCache' => true,                           // the app's PSR-16 cache for the token
    'retry' => ['maxRetries' => 3, 'baseDelayMs' => 200, 'maxDelayMs' => 5000, 'transactionAttempts' => 5],
],
```

The application binds `Psr\Http\Client\ClientInterface` (with its timeouts),
`Psr\Http\Message\RequestFactoryInterface` and `Psr\Http\Message\StreamFactoryInterface`; then take
`FirestoreClient` from the container. With an emulator host there is no credentials object.

**Set `keyFile` explicitly** in any process that also uses another Google package that falls back on
`GOOGLE_APPLICATION_CREDENTIALS` (ldkafka/yii3-google-cloud-logging does), or both will use the same
key.

## Security

- Keep key files out of version control; `.gitignore` covers `*.key`, `*credentials*.json` and
  `*service-account*.json`. The only key in this repository is a throwaway test key.
- No token or key appears in an exception message, a stack trace, the previous-exception chain, a
  `print_r()` or `var_dump()` of the client or the credentials, or a log line. The key argument is
  `#[\SensitiveParameter]`; the PSR-18 client's exception is not kept as the previous exception,
  because it can carry the request with its `Authorization` header; any token in its message is
  blanked.
- Exception messages never hold field values the package sent. An `ApiException` message ends with
  the server's own message (cut at 500 characters), which can name document paths.

## Testing

```bash
composer install
vendor/bin/phpunit                          # unit tests (no network)
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 vendor/bin/phpunit --testsuite integration
FIRESTORE_SMOKE=1 FIRESTORE_SMOKE_KEY=/path/key.json vendor/bin/phpunit --testsuite smoke
vendor/bin/phpstan analyse                  # level 9, src and tests
```

- The integration suite skips itself without `FIRESTORE_EMULATOR_HOST` and clears the emulator before
  each test. It includes two PHP processes claiming one message at the same moment, ten rounds,
  exactly one winner each.
- The smoke suite runs against a real database, in a scratch collection that it empties afterwards
  (`FIRESTORE_SMOKE_DATABASE` picks a named database). It checks create, get, query, delete, one
  read-write transaction and the three precondition statuses.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
