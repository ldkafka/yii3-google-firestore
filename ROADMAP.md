# Roadmap

The order and timing below follow real use of the package. They are plans, not promises.

## 0.1.x

- Fixes and small additions from the message bus's real use.

## Only if asked later

- OR filters, `select` field masks, read-only and `readTime` reads.
- Sum and average aggregations; minimum and maximum transforms; a delete-field sentinel.
- `listDocuments`; honouring `Retry-After` and `RetryInfo`; a built-in HTTP client.

## Non-goals

- Real-time listeners (`Listen` needs gRPC or streaming; poll instead).
- Admin APIs: indexes, security rules, databases.
- Firebase Authentication.
