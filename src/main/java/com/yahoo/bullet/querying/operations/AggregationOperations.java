/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.querying.operations;

import com.yahoo.bullet.aggregations.CountDistinct;
import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.GroupAll;
import com.yahoo.bullet.aggregations.GroupBy;
import com.yahoo.bullet.aggregations.Raw;
import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.parsing.Aggregation;

public class AggregationOperations {
    /**
     * Returns a new {@link Strategy} instance that can handle this aggregation.
     *
     * @param aggregation The non-null, initialized {@link Aggregation} instance.
     * @param config The {@link BulletConfig} containing configuration for the strategy.
     *
     * @return The created instance of a strategy that can implement the Aggregation.
     */
    public static Strategy findStrategy(Aggregation aggregation, BulletConfig config) {
        // Guaranteed to be present.
        switch (aggregation.getType()) {
            case COUNT_DISTINCT:
                return new CountDistinct(aggregation, config);
            case DISTRIBUTION:
                return new Distribution(aggregation, config);
            case RAW:
                return new Raw(aggregation, config);
            case TOP_K:
                return new TopK(aggregation, config);
        }

        // If we have any fields -> GroupBy
        return Utilities.isEmpty(aggregation.getFields()) ? new GroupAll(aggregation, config) : new GroupBy(aggregation, config);
    }
}
