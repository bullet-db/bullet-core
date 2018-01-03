/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.common.Queryable;
import com.yahoo.bullet.parsing.ParsingError;
import com.yahoo.bullet.result.Metadata;

import static com.yahoo.bullet.parsing.ParsingError.makeError;

public interface Strategy extends Queryable {
    String REQUIRES_FEED_RESOLUTION = "Please add a field for this aggregation.";

    ParsingError REQUIRES_FIELD_ERROR =
            makeError("This aggregation type requires at least one field", REQUIRES_FEED_RESOLUTION);

    /**
     * Returns false if more data will be not be consumed or combined. This method can be used to avoid passing more
     * data into this Strategy. By default, returns false unless overridden.
     *
     * @return A boolean denoting whether the next consumption or combination will not occur.
     */
    @Override
    default boolean isClosed() {
        return false;
    }

    /**
     * Get the {@link Metadata} so far. By default, returns an empty one.
     *
     * @return The resulting metadata of the data aggregated so far.
     */
    @Override
    default Metadata getMetadata() {
        return new Metadata();
    }
}

