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
 * Encapsulates the idea of criteria that can apply themselves to a {@link StorageManager} search query. This can be
 * anything like the WHERE clause of a SQL query for relational storages or whatever else.
 */
public interface Criteria {
    <V extends Serializable> CompletableFuture<Map<String, byte[]>> retrieve(StorageManager<V> storage);
}
