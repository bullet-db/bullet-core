/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * @param <T> The specific type used by this {@link Criteria} as arguments.
 * @param <R> The specific type returned by this {@link Criteria} when applied to a storage.
 *
 * Encapsulates the idea of criteria that can apply themselves to a {@link StorageManager} search query. This can be
 * anything like the WHERE clause of a SQL query for relational storages or whatever else. It provides the
 * {@link #get(StorageManager)} to retrieve the data from the storage as a key-value mapping to the raw data
 * stored. The {@link Criteria} can also wrap its results in its own format when using the
 * {@link #retrieve(StorageManager)} interface to return objects of an expected type.
 * <p>
 * A specific {@link Criteria} is intended to implemented along with the particular {@link StorageManager} so that
 * non-public interfaces can be shared between them.
 * <p>
 * In order to be able to do arbitrary changes or queries to the storage, the {@link #apply(StorageManager, Object)} is
 * also provided. This method is intended to be used for anything dealing with the storage, including retrieval. The
 * specific arguments for the query are left to the specific criteria.
 */
public interface Criteria<T, R> {
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
    <V extends Serializable> CompletableFuture<R> retrieve(StorageManager<V> storage);

    /**
     * Applies the criteria to the {@link StorageManager}. This is left to the specific criteria to choose what to do.
     *
     * @param storage The {@link StorageManager} to apply this to.
     * @param query The specific query to apply to this {@link Criteria}.
     * @param <V> The type of the data stored in the storage.
     * @return A {@link CompletableFuture} that resolves to this {@link Criteria} type.
     */
    <V extends Serializable> CompletableFuture<R> apply(StorageManager<V> storage, T query);

    /**
     * Utility method to help check and cast if a given storage is of the given type. It does not check generic types
     * for the storage.
     *
     * @param storage The storage to check.
     * @param klazz The non-null super-type that the given storage is supposed to inherit from.
     * @param <V> The type of the storage.
     * @param <S> The super-type of the storage.
     * @return The casted storage.
     * @throws UnsupportedOperationException if the storage is not an instance of the given class.
     */
    @SuppressWarnings("unchecked")
    static <V extends Serializable, S extends StorageManager> S checkType(StorageManager<V> storage, Class<S> klazz) {
        Objects.requireNonNull(klazz);
        if (!klazz.isInstance(storage)) {
            String name = storage == null ? "null" : storage.getClass().getName();
            throw new UnsupportedOperationException(name + " is not an instance of " + klazz.getName());
        }
        return (S) storage;
    }
}
