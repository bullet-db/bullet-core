/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A default implementation that does nothing if you do not want to use a StorageManager.
 */
public class NullStorageManager extends StorageManager {
    private static final long serialVersionUID = -1718811448543607136L;

    private static final CompletableFuture<Boolean> FAIL = CompletableFuture.completedFuture(false);
    private static final CompletableFuture<Boolean> SUCCESS = CompletableFuture.completedFuture(true);

    /**
     * Constructor.
     *
     * @param config The {@link BulletConfig} to create this manager with.
     */
    public NullStorageManager(BulletConfig config) {
        super(config);
    }

    @Override
    public CompletableFuture<byte[]> get(String id) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<byte[]> remove(String id) {
        return get(id);
    }

    @Override
    public CompletableFuture<Boolean> clear(Set<String> ids) {
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> put(String id, byte[] value) {
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> putAll(Map<String, byte[]> data) {
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Map<String, byte[]>> getAll() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> clear() {
        return SUCCESS;
    }
}
