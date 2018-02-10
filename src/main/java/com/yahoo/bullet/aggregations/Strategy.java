/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Monoidal;
import com.yahoo.bullet.result.Meta;

import static com.yahoo.bullet.common.BulletError.makeError;

public interface Strategy extends Monoidal {
    String REQUIRES_FEED_RESOLUTION = "Please add a field for this aggregation.";

    BulletError REQUIRES_FIELD_ERROR =
            makeError("This aggregation type requires at least one field", REQUIRES_FEED_RESOLUTION);

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

