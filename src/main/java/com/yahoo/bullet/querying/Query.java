/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

/**
 * Encapsulates the concept of a Query. A Query can consume a type T as data and supply another type R as results.
 *
 * @param <T> The type of the data that this Query can consume.
 * @param <R> The type of the data that this Query produces.
 */
public interface Query<T, R> {
    /**
     * Returns true iff the Query has been satisfied for this data.
     *
     * @param data The data to consume.
     *
     * @return true if the consumed data caused the Query to become satisfied.
     */
    boolean consume(T data);

    /**
     * Gets the data as processed so far. Depending on the query, {@link #consume(Object)} may need to be true first.
     *
     * @return The data produced by the Query.
     */
    R getData();
}
