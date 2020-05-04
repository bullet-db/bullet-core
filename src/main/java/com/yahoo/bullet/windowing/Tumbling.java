/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.result.Meta;

import java.util.Map;

import static com.yahoo.bullet.result.Meta.addIfNonNull;

public class Tumbling extends Basic {
    public static final String NAME = "Tumbling";

    protected long nextCloseTime;
    protected long windowLength;

    /**
     * Creates an instance of this windowing scheme with the provided {@link Strategy} and {@link BulletConfig}.
     *
     * @param aggregation The non-null initialized aggregation strategy that this window will operate.
     * @param window The initialized, configured window to use.
     * @param config The validated config to use.
     */
    public Tumbling(Strategy aggregation, Window window, BulletConfig config) {
        super(aggregation, window, config);
        windowLength = (long) window.getEmitEvery();
        nextCloseTime = System.currentTimeMillis() + windowLength;
    }

    @Override
    protected Map<String, Object> getMetadata(Map<String, String> metadataKeys) {
        Map<String, Object> meta = super.getMetadata(metadataKeys);
        addIfNonNull(meta, metadataKeys, Meta.Concept.WINDOW_EXPECTED_EMIT_TIME, () -> this.nextCloseTime);
        return meta;
    }

    @Override
    public void reset() {
        super.reset();
        nextCloseTime = nextCloseTime + windowLength;
    }

    @Override
    public boolean isClosed() {
        return System.currentTimeMillis() >= nextCloseTime;
    }

    @Override
    public boolean isClosedForPartition() {
        // For tumbling windows, isClosedForPartition is the same as isClosed.
        return isClosed();
    }

    @Override
    public void start() {
        nextCloseTime = System.currentTimeMillis() + windowLength;
    }

    @Override
    protected String name() {
        return NAME;
    }
}
