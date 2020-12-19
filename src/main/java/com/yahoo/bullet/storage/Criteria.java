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
 * {@link #get(StorageManager)} to retrieve the data from the storage as a key-value mapping to the raw data
 * stored. The {@link Criteria} can also wrap its results in its own format when using the
 * {@link #retrieve(StorageManager)} interface to return objects of an expected type.
 */
public interface Criteria<E> {
    /**
     * Retrieves data from the given {@link StorageManager} as a {@link CompletableFuture} resolving to a key-value
     * mapping of the raw data stored in the storage.
     *
     * @param storage The {@link StorageManager} to retrieve data from.
     * @param <V> The type of the data stored in the storage.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of String keys to the raw data in the storage.
     */
    <V extends Serializable> CompletableFuture<Map<String, V>> get(StorageManager<V> storage);

    /**
     * Retrieves data from the given {@link StorageManager} as a {@link CompletableFuture} resolving to the type of this
     * {@link Criteria}.
     *
     * @param storage The {@link StorageManager} to retrieve data from.
     * @param <V> The type of the data stored in the storage.
     * @return A {@link CompletableFuture} that resolves to this {@link Criteria} type.
     */
    <V extends Serializable> CompletableFuture<E> retrieve(StorageManager<V> storage);

    /**
     * Applies the criteria to the {@link StorageManager}. This is left to the specific criteria to choose what to do.
     *
     * @param storage The {@link StorageManager} to apply this to.
     * @param <V> The type of the data stored in the storage.
     * @return A {@link CompletableFuture} that resolves to this {@link Criteria} type.
     */
    <V extends Serializable> CompletableFuture<E> apply(StorageManager<V> storage);
}
