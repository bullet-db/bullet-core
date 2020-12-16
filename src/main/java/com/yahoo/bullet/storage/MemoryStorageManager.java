/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A Storage manager that stores everything in-memory.
 */
public class MemoryStorageManager extends StorageManager<Serializable> implements Serializable {
    private static final long serialVersionUID = 3815534537510449363L;
    private static final CompletableFuture<Boolean> SUCCESS = CompletableFuture.completedFuture(true);

    private Map<String, byte[]> storage;

    /**
     * Constructor.
     *
     * @param config The {@link BulletConfig} to create this manager with.
     */
    public MemoryStorageManager(BulletConfig config) {
        super(config);
        storage = new HashMap<>();
    }

    @Override
    public CompletableFuture<byte[]> get(String id) {
        return CompletableFuture.completedFuture(storage.get(id));
    }

    @Override
    public CompletableFuture<byte[]> remove(String id) {
        return CompletableFuture.completedFuture(storage.remove(id));
    }

    @Override
    public CompletableFuture<Boolean> put(String id, byte[] value) {
        storage.put(id, value);
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> putAll(Map<String, byte[]> data) {
        if (data != null) {
            storage.putAll(data);
        }
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Map<String, byte[]>> getAll() {
        return CompletableFuture.completedFuture(storage.isEmpty() ? null : new HashMap<>(storage));
    }

    @Override
    public CompletableFuture<Map<String, byte[]>> getAll(Set<String> ids) {
        if (storage.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        Map<String, byte[]> copy = new HashMap<>();
        if (ids != null) {
            ids.forEach(id -> copy.put(id, storage.get(id)));
        }
        return CompletableFuture.completedFuture(copy);
    }

    @Override
    public CompletableFuture<Boolean> clear() {
        storage.clear();
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> clear(Set<String> ids) {
        if (ids != null) {
            ids.forEach(storage::remove);
        }
        return SUCCESS;
    }
}
