/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Meta;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;

import static com.yahoo.bullet.result.Meta.addIfNonNull;

public class SlidingRecord extends Basic {
    public static String NAME = "Sliding";

    protected int maxCount;
    protected int recordCount;

    @AllArgsConstructor @Getter
    private static class Data implements Serializable {
        private static final long serialVersionUID = -3035790881273001274L;
        private final long count;
        private final byte[] data;
    }

    /**
     * Creates an instance of this windowing scheme with the provided {@link Strategy} and {@link BulletConfig}.
     *
     * @param aggregation The non-null initialized aggregation strategy that this window will operate.
     * @param window The initialized, configured window to use.
     * @param config The validated config to use.
     */
    public SlidingRecord(Strategy aggregation, Window window, BulletConfig config) {
        super(aggregation, window, config);
    }

    @Override
    protected Map<String, Object> getMetadata(Map<String, String> metadataKeys) {
        Map<String, Object> meta = super.getMetadata(metadataKeys);
        addIfNonNull(meta, metadataKeys, Meta.Concept.WINDOW_SIZE, () -> this.recordCount);
        return meta;
    }

    @Override
    public void consume(BulletRecord data) {
        super.consume(data);
        recordCount++;
    }

    @Override
    public void combine(byte[] data) {
        Data wrapped = SerializerDeserializer.fromBytes(data);
        super.combine(wrapped.getData());
        recordCount += wrapped.getCount();
    }

    @Override
    public byte[] getData() {
        byte[] data = super.getData();
        Data wrapped = new Data(recordCount, data);
        return SerializerDeserializer.toBytes(wrapped);
    }

    @Override
    public void reset() {
        super.reset();
        recordCount = 0;
    }

    @Override
    public boolean isClosed() {
        return super.isClosed() || recordCount >= maxCount;
    }

    @Override
    public boolean isClosedForPartition() {
        return super.isClosedForPartition() || recordCount >= 1;
    }

    @Override
    protected String name() {
        return NAME;
    }
}
