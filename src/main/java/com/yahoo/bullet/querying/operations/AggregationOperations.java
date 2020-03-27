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
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.CountDistinctAggregation;
import com.yahoo.bullet.query.aggregations.DistributionAggregation;
import com.yahoo.bullet.query.aggregations.GroupAggregation;
import com.yahoo.bullet.query.aggregations.TopKAggregation;

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
        Strategy strategy = null;
        // Guaranteed to be present.
        switch (aggregation.getType()) {
            case COUNT_DISTINCT:
                strategy = new CountDistinct((CountDistinctAggregation) aggregation, config);
                break;
            case DISTRIBUTION:
                strategy = new Distribution((DistributionAggregation) aggregation, config);
                break;
            case RAW:
                strategy = new Raw(aggregation, config);
                break;
            case TOP_K:
                strategy = new TopK((TopKAggregation) aggregation, config);
                break;
            case GROUP:
                // If we have any fields -> GroupBy
                GroupAggregation groupAggregation = (GroupAggregation) aggregation;
                strategy = groupAggregation.hasFields() ? new GroupBy(groupAggregation, config) : new GroupAll(groupAggregation, config);
        }
        return strategy;
    }
}
