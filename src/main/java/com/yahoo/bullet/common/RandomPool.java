/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.common;

import java.util.List;
import java.util.Random;

public class RandomPool<T> {
    private List<T> items;

    private static final Random RANDOM = new Random();

    /**
     * Constructor for the RandomPool that takes a list of items.
     * @param items A list of items to form the pool with.
     */
    public RandomPool(List<T> items) {
        this.items = items;
    }

    /**
     * Get a random item from the pool.
     *
     * @return a randomly chosen item from the pool.
     */
    public T get() {
        if (items == null || items.isEmpty()) {
            return null;
        }
        return items.get(RANDOM.nextInt(items.size()));
    }

    /**
     * Clear the RandomPool. Gets now return null.
     */
    public void clear() {
        items = null;
    }

    @Override
    public String toString() {
        return items == null ? null : items.toString();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null) {
            return false;
        }
        if (!(object instanceof RandomPool)) {
            return false;
        }
        RandomPool asPool = (RandomPool) object;
        return items == null ? asPool.items == null : items.equals(asPool.items);
    }

    @Override
    public int hashCode() {
        // Any number would do since we want RandomPools of null to be equal to each other.
        return items == null ? 42 : items.hashCode();
    }
}
