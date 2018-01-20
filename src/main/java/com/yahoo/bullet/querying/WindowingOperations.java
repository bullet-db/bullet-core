/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.Window;
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

        // For now, if no window -> Basic, if Raw but not TIME_TIME -> Reactive, if time based, Tumbling or AdditiveTumbling
        // TODO: Support other windows
        if (window == null) {
            return new Basic(strategy, null, config);
        }
        Window.Classification type = window.getType();
        if (query.getAggregation().getType() == Aggregation.Type.RAW) {
            // If Raw but we have a window that's anything but TIME_TIME ~> Tumbling/Hopping, force to Reactive
            if (type != Window.Classification.TIME_TIME) {
                return new Reactive(strategy, window, config);
            }
        }
        // Raw cannot be Additive
        if (type == Window.Classification.TIME_ALL) {
            return new AdditiveTumbling(strategy, window, config);
        }
        // Raw can be Tumbling and all other aggregations default to Tumbling
        return new Tumbling(strategy, window, config);
    }
}
