/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @param <E> The specific type returned by this {@link Criteria} when applied to a storage.
 *
 * Encapsulates the idea of criteria that can apply themselves to a {@link StorageManager} search query. This can be
 * anything like the WHERE clause of a SQL query for relational storages or whatever else. It provides the
 * {@link #retrieve(StorageManager)} to retrieve the data from the storage as a key-value mapping to the raw data
 * stored. The {@link Criteria} can also wrap its results in its own format when using the
 * {@link #match(StorageManager)} interface.
 */
public interface Criteria<E> {
    <V extends Serializable> CompletableFuture<Map<String, byte[]>> retrieve(StorageManager<V> storage);
    <V extends Serializable> CompletableFuture<E> match(StorageManager<V> storage);
}
