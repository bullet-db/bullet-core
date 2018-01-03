/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.windowing.Scheme;

public class WindowingOperations {
    public enum WindowUnit {
        RECORD, TIME, ALL
    }

    /**
     * Create a windowing {@link Scheme} for this particular {@link Query}.
     *
     * @param query The configured, initialized query to find a scheme for.
     * @param strategy The aggregation strategy to use for in the windowing scheme.
     * @param config The {@link BulletConfig} to use for configuration.
     * @return A windowing scheme to use for this query.
     */
    public static Scheme findScheme(Query query, Strategy strategy, BulletConfig config) {
        return null;
    }
}
