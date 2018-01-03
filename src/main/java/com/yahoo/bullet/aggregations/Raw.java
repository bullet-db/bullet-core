/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Implements the LIMIT operation on multiple raw {@link BulletRecord}.
 *
 * A call to {@link #getSerializedAggregation()} or {@link #getAggregation()} will return the current collection of
 * records, which is a {@link List} of {@link BulletRecord}.
 *
 * A call to {@link #combine(byte[])} with the result of {@link #getSerializedAggregation()} will combine records from
 * the {@link List} till the aggregation size is reached.
 *
 * This {@link Strategy} will only consume or combine till the specified aggregation size is reached.
 */
@Slf4j
public class Raw implements Strategy {
    private ArrayList<BulletRecord> aggregate = new ArrayList<>();

    private Integer size;
    private int consumed = 0;
    private int combined = 0;

    /**
     * Constructor that takes in an {@link Aggregation} and a {@link BulletConfig}. The size of the aggregation is used
     * as a LIMIT operation.
     *
     * @param aggregation The aggregation that specifies how and what this will compute.
     * @param config The config that has relevant configs for this strategy.
     */
    @SuppressWarnings("unchecked")
    public Raw(Aggregation aggregation, BulletConfig config) {
        int maximumSize = config.getAs(BulletConfig.RAW_AGGREGATION_MAX_SIZE, Integer.class);
        size = Math.min(aggregation.getSize(), maximumSize);
    }

    @Override
    public boolean isClosed() {
        return consumed + combined >= size;
    }

    @Override
    public void consume(BulletRecord data) {
        if (isClosed() || data == null) {
            return;
        }
        consumed++;
        aggregate.add(data);
    }

    /**
     * Since {@link #getSerializedAggregation()} returns a {@link List} of {@link BulletRecord}, this method consumes
     * that list. If the deserialized List has a size that takes the aggregated records above the aggregation size, only
     * the first X records in the List will be combined till the size is reached.
     *
     * @param serializedAggregation A serialized {@link List} of {@link BulletRecord}.
     */
    @Override
    public void combine(byte[] serializedAggregation) {
        if (isClosed() || serializedAggregation == null) {
            return;
        }
        ArrayList<BulletRecord> batch = SerializerDeserializer.fromBytes(serializedAggregation);
        if (batch == null || batch.isEmpty()) {
            return;
        }
        int batchSize = batch.size();
        int maximumLeft = size - aggregate.size();
        if (batchSize <= maximumLeft) {
            aggregate.addAll(batch);
            combined += batchSize;
        } else {
            aggregate.addAll(batch.subList(0, maximumLeft));
            combined += maximumLeft;
        }
    }

    /**
     * Returns the serialized {@link List} of {@link BulletRecord} seen before the last call to {@link #reset()}.
     *
     * @return the serialized byte[] representing the {@link List} of {@link BulletRecord} or null if it could not.
     */
    @Override
    public byte[] getSerializedAggregation() {
        if (aggregate.isEmpty()) {
            return null;
        }
        return SerializerDeserializer.toBytes(aggregate);
    }

    /**
     * Gets the aggregated records so far since the last call to {@link #reset()}. The records have a size that is at
     * most the maximum specified by the {@link Aggregation}.
     *
     * @return a {@link Clip} of the records so far.
     */
    @Override
    public Clip getAggregation() {
        return Clip.of(getAggregatedRecords());
    }

    /**
     * Gets the aggregated records so far since the last call to {@link #reset()}. The records have a size that is at
     * most the maximum specified by the {@link Aggregation}.
     *
     * @return a {@link List} of the records so far.
     */
    @Override
    public List<BulletRecord> getAggregatedRecords() {
        return aggregate;
    }

    @Override
    public void reset() {
        aggregate = new ArrayList<>();
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        return Optional.empty();
    }
}
