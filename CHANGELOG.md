# Changelog

All notable changes to this package are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). In 0.x a minor release may break the API; such a release
has a "Backwards compatibility" section. Patch releases never do.

## [0.1.1] - Unreleased

### Fixed
- An error answer (non-2xx) whose body is a list, as `runQuery` streams it (`[{"error": {...}}]`),
  now gives the element's status and message (e.g. `FAILED_PRECONDITION` for a missing index,
  `NOT_FOUND` for a missing database) instead of `UNKNOWN`.
- An HTTP 404 without a JSON body is `NOT_FOUND` (`NotFoundException`), not `UNKNOWN`.

## [0.1.0] - Unreleased

### Added
- `FirestoreClient` over the Firestore REST API v1, through any PSR-18 client and PSR-17 factories:
  `get()`, `getAll()`, `create()` (a 20-character id made on the client, or a given id), `set()`
  (replace or merge), `update()` (dotted field paths, the keys are the update mask), `delete()`,
  `query()`, `collectionGroup()`, `listCollections()`, `batch()` and `runTransaction()`.
- `Query`: immutable; `where()` with every operator (null and NAN become the unary filters),
  `orderBy()`, `limit()`, `offset()`, `startAt()`, `startAfter()`, `endAt()`, `endBefore()` from a
  document or from values, `get()`, `stream()` (pages through cursors) and `count()`.
- `WriteBatch` (atomic, in order) and `Transaction` (reads with the transaction id, buffered writes;
  the callable re-runs on ABORTED, any other exception rolls back).
- `Precondition::exists()` and `Precondition::updateTime()`, so two writers can't both claim one
  document.
- `FieldValue::serverTimestamp()`, `increment()`, `arrayUnion()`, `arrayRemove()`, at any depth;
  `WriteResult::$transformResults` gives back what the server set.
- Value mapping both ways: null, bool, int64, double (NaN and infinities), string, `Timestamp`
  (nanoseconds kept), `Bytes`, `Reference`, `GeoPoint`, lists and maps (empty ones included).
- Typed exceptions: `ConfigurationException`, `AuthenticationException`, `TransportException`,
  `ApiException` and, by the body's status, `NotFoundException`, `AlreadyExistsException`,
  `FailedPreconditionException`, `AbortedException`.
- `RetryPolicy`: reads retried on RESOURCE_EXHAUSTED, UNAVAILABLE, INTERNAL, DEADLINE_EXCEEDED and
  network errors; a commit only on RESOURCE_EXHAUSTED and, outside a transaction, ABORTED (never on
  a 5xx or a network error); backoff with jitter; one PSR-3 warning per retry.
- `ServiceAccountCredentials`: RS256 JWT bearer grant through the PSR-18 client, the datastore scope,
  an optional PSR-16 token cache bound to the key's identity; the key and the token are redacted in
  stack traces and dumps.
- The emulator: `FIRESTORE_EMULATOR_HOST`, plain HTTP, `Bearer owner`, no login.
- Yii3 wiring: `config/params.php` and `config/di.php`.
- Tests: unit tests, integration tests on the Firestore emulator (including two processes claiming
  one message and real transaction contention), an opt-in smoke suite for a real database; GitHub
  Actions on PHP 8.4 and 8.5, `composer validate --strict` and PHPStan level 9.
