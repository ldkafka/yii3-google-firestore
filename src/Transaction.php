<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore;

/**
 * What a FirestoreClient::runTransaction() callable gets: reads inside the transaction, and
 * writes buffered until it commits.
 */
final class Transaction
{
    private WriteBatch $batch;

    /**
     * @internal Made by FirestoreClient::runTransaction().
     */
    public function __construct(private FirestoreClient $client, private string $id)
    {
        $this->batch = $client->batch();
    }

    /**
     * One document read in the transaction, or null when it doesn't exist.
     *
     * @phpstan-impure
     */
    public function get(string $path): ?Document
    {
        return $this->client->batchGet([$path], $this->id)[$path];
    }

    /**
     * Several documents read in the transaction.
     *
     * @param list<string> $paths
     * @return array<string, Document|null> Keyed by path, in the order asked.
     *
     * @phpstan-impure
     */
    public function getAll(array $paths): array
    {
        return $this->client->batchGet($paths, $this->id);
    }

    /**
     * Run a query in the transaction.
     *
     * @return list<Document>
     *
     * @phpstan-impure
     */
    public function runQuery(Query $query): array
    {
        return $query->run($this->id);
    }

    /**
     * Buffer a create.
     *
     * @param array<string, mixed> $data
     * @return string The new document's path.
     */
    public function create(string $collection, array $data, ?string $id = null): string
    {
        return $this->batch->create($collection, $data, $id);
    }

    /**
     * Buffer a set (replace, or merge).
     *
     * @param array<string, mixed> $data
     */
    public function set(string $path, array $data, bool $merge = false): void
    {
        $this->batch->set($path, $data, $merge);
    }

    /**
     * Buffer an update of the named fields.
     *
     * @param array<string, mixed> $fields Dotted field path => value.
     */
    public function update(string $path, array $fields, ?Precondition $precondition = null): void
    {
        $this->batch->update($path, $fields, $precondition);
    }

    /**
     * Buffer a delete.
     */
    public function delete(string $path, ?Precondition $precondition = null): void
    {
        $this->batch->delete($path, $precondition);
    }

    /**
     * The buffered writes.
     *
     * @internal
     */
    public function batch(): WriteBatch
    {
        return $this->batch;
    }
}
