/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;

import java.util.List;

import static com.yahoo.bullet.parsing.Error.makeError;

public interface Strategy {
    String REQUIRES_FEED_RESOLUTION = "Please add a field for this aggregation.";

    Error REQUIRES_FIELD_ERROR =
            makeError("This aggregation type requires at least one field", REQUIRES_FEED_RESOLUTION);

    /**
     * Returns true if more data will be consumed or combined. This method can be used to avoid passing more
     * data into this Strategy.
     *
     * @return A boolean denoting whether the next consumption or combination will occur.
     */
    default boolean isAcceptingData() {
        return true;
    }

    /**
     * Returns true if the data consumed/combined constitutes a micro-batch. In the case where micro-batching is
     * done and all aggregation strategies are meant to be additive, the strategy will reset its aggregation state.
     *
     * @return A boolean denoting if the data consumed/combined so far constitutes a micro-batch.
     */
    default boolean isMicroBatch() {
        return false;
    }

    /**
     * Consumes a single {@link BulletRecord} into the aggregation.
     *
     * @param data The {@link BulletRecord} to consume.
     */
    void consume(BulletRecord data);

    /**
     * Combines a serialized intermediate aggregation into this aggregation.
     *
     * @param serializedAggregation A serialized representation of an aggregation. This must be have been produced by
     *                              the {@link #getSerializedAggregation()} method.
     */
    void combine(byte[] serializedAggregation);

    /**
     * Serialize the aggregation done so far.
     *
     * @return the serialized representation of the aggregation so far.
     */
    byte[] getSerializedAggregation();

    /**
     * Get the Aggregation done so far as a {@link Clip}.
     *
     * @return The resulting {@link Clip} representing aggregation and metadata of the data aggregated so far.
     */
    Clip getAggregation();

    /**
     * Checks to see if this Strategy is valid. Any other methods may behave unexpectedly unless initialize passes.
     *
     * @return An {@link List} of {@link Error} that contains errors if validation failed or null if succeeded.
     */
    List<Error> initialize();
}

