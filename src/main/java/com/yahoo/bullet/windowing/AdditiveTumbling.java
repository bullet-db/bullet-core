/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Window;

public class AdditiveTumbling extends Tumbling {
    /**
     * Creates an instance of this windowing scheme with the provided {@link Strategy} and {@link BulletConfig}.
     *
     * @param aggregation The non-null initialized aggregation strategy that this window will operate.
     * @param window The initialized, configured window to use.
     * @param config The validated config to use.
     */
    public AdditiveTumbling(Strategy aggregation, Window window, BulletConfig config) {
        super(aggregation, window, config);
    }

    @Override
    public void reset() {
        nextCloseTime = nextCloseTime + windowLength;
        windowCount++;
    }

    @Override
    public void resetForPartition() {
        // Do reset the strategy.
        aggregation.reset();
        reset();
    }
}
