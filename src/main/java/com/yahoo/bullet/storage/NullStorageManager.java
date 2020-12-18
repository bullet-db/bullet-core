/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A default implementation that does nothing if you do not want to use a StorageManager.
 */
public class NullStorageManager<V extends Serializable> extends StorageManager<V> implements Serializable {
    private static final long serialVersionUID = -1718811448543607136L;

    /**
     * Constructor.
     *
     * @param config The {@link BulletConfig} to create this manager with.
     */
    public NullStorageManager(BulletConfig config) {
        super(config);
    }

    @Override
    protected CompletableFuture<Boolean> putRaw(String namespace, String id, byte[] value) {
        return SUCCESS;
    }

    @Override
    protected CompletableFuture<byte[]> getRaw(String namespace, String id) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<Map<String, byte[]>> getAllRaw(String namespace) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<byte[]> removeRaw(String namespace, String id) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> clear() {
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> clear(String namespace) {
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> clear(String namespace, Set<String> ids) {
        return SUCCESS;
    }
}
