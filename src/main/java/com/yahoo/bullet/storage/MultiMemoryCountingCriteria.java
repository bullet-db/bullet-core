/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.yahoo.bullet.storage.Criteria.checkType;

/**
 * An example of a simple criteria that counts the number of stored entries for the {@link MultiMemoryCountingCriteria}
 * storage manager. {@link #retrieve(StorageManager)} counts the keys in the default namespace
 * ({@link MultiMemoryStorageManager#getDefaultNamespace()}) while {@link #apply(StorageManager, List)} takes a
 * {@link List} of String of namespaces to retrieve the total count for. If the {@link List} is empty or null, the total
 * count is returned.
 */
public class MultiMemoryCountingCriteria implements Criteria<List, Long> {
    @Override
    public <V extends Serializable> CompletableFuture<Map<String, V>> get(StorageManager<V> storage) {
        throw new UnsupportedOperationException("The counting criteria does not allow fetching data");
    }

    @Override
    public <V extends Serializable> CompletableFuture<Long> retrieve(StorageManager<V> storage) {
        return CompletableFuture.completedFuture(sum(storage.getDefaultNamespace(), storage));
    }

    @Override
    public <V extends Serializable> CompletableFuture<Long> apply(StorageManager<V> storage, List query) {
        if (query == null || query.isEmpty()) {
            return CompletableFuture.completedFuture(sum(storage));
        }
        long sum = 0;
        for (Object namespace: query) {
            sum += sum(namespace.toString(), storage);
        }
        return CompletableFuture.completedFuture(sum);
    }


    private <V extends Serializable> Long sum(String namespace, StorageManager<V> storage) {
        checkType(storage, MultiMemoryStorageManager.class);
        return sum(namespace, (MultiMemoryStorageManager<V>) storage);
    }

    private <V extends Serializable> Long sum(String namespace, MultiMemoryStorageManager<V> storage) {
        storage.validateNamespace(namespace);
        return storage.getStorage().get(namespace).values().stream().mapToLong(Map::size).sum();
    }

    private <V extends Serializable> Long sum(StorageManager<V> storage) {
        checkType(storage, MultiMemoryStorageManager.class);
        return sum((MultiMemoryStorageManager<V>) storage);
    }

    private <V extends Serializable> Long sum(MultiMemoryStorageManager<V> storage) {
        return storage.getStorage().values().stream()
                                            .mapToLong(n -> n.values().stream().mapToLong(Map::size).sum())
                                            .sum();
    }
}
