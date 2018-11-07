/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.operations;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.windowing.AdditiveTumbling;
import com.yahoo.bullet.windowing.Basic;
import com.yahoo.bullet.windowing.Scheme;
import com.yahoo.bullet.windowing.SlidingRecord;
import com.yahoo.bullet.windowing.Tumbling;

public class WindowingOperations {
    /**
     * Create a windowing {@link Scheme} for this particular {@link Query}.
     *
     * @param query The configured, initialized query to find a scheme for.
     * @param strategy The aggregation strategy to use for in the windowing scheme.
     * @param config The {@link BulletConfig} to use for configuration.
     * @return A windowing scheme to use for this query.
     */
    public static Scheme findScheme(Query query, Strategy strategy, BulletConfig config) {
        /*
         * TODO: Support other windows
         * The windows we support at the moment:
         * 1. No window -> Basic
         * 2. Window is emit RECORD and include RECORD -> SlidingRecord
         * 3. Window is emit TIME and include ALL -> Additive Tumbling
         * 4. All other windows -> Tumbling (RAW can be Tumbling too)
         */
        Window window = query.getWindow();
        if (window == null) {
            return new Basic(strategy, null, config);
        }

        Window.Classification classification = window.getType();
        if (classification == Window.Classification.RECORD_RECORD) {
            return new SlidingRecord(strategy, window, config);
        }
        if (classification == Window.Classification.TIME_ALL) {
            return new AdditiveTumbling(strategy, window, config);
        }
        return new Tumbling(strategy, window, config);
    }
}
