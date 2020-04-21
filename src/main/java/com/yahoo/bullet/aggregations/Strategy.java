/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.common.Monoidal;
import com.yahoo.bullet.result.Meta;

public interface Strategy extends Monoidal {
    /**
     * Returns false if more data should not be consumed or combined. This method can be used to avoid passing more
     * data into this Strategy. By default, returns false unless overridden.
     *
     * @return A boolean denoting whether the next consumption or combination should not occur.
     */
    @Override
    default boolean isClosed() {
        return false;
    }

    /**
     * Get the {@link Meta} so far. By default, returns an empty one.
     *
     * @return The resulting metadata of the data aggregated so far.
     */
    @Override
    default Meta getMetadata() {
        return new Meta();
    }
}

