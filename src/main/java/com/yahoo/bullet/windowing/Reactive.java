/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Window;

/**
 * This window is just a {@link SlidingRecord} window with a maximum of one record - both {@link #isClosedForPartition()}
 * and {@link #isClosed()} are identical. Using this will make results be emitted as they arrive - making it snappy or
 * reactive.
 */
public class Reactive extends SlidingRecord {
    /**
     * Creates an instance of this windowing scheme with the provided {@link Strategy} and {@link BulletConfig}.
     *
     * @param aggregation The non-null initialized aggregation strategy that this window will operate.
     * @param window The initialized, configured window to use.
     * @param config The validated config to use.
     */
    public Reactive(Strategy aggregation, Window window, BulletConfig config) {
        super(aggregation, window, config);
        this.maxCount = 1;
    }
}
