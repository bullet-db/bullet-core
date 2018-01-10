/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.querying.AggregationOperations.AggregationType;
import com.yahoo.bullet.windowing.AdditiveTumbling;
import com.yahoo.bullet.windowing.Basic;
import com.yahoo.bullet.windowing.Reactive;
import com.yahoo.bullet.windowing.Scheme;
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
        Window window = query.getWindow();

        // For now, if no window -> Basic, if Raw -> Reactive, if time based, Tumbling or AdditiveTumbling
        // TODO: Support other windows
        if (window == null) {
            return new Basic(strategy, null, config);
        }
        Window.Classification type = window.getType();
        if (query.getAggregation().getType() == AggregationType.RAW) {
            return new Reactive(strategy, window, config);
        } else if (type == Window.Classification.TIME_ALL) {
            return new AdditiveTumbling(strategy, window, config);
        }
        return new Tumbling(strategy, window, config);
    }
}
