/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.google.gson.annotations.SerializedName;
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
    public enum AggregationType {
        // The alternate value of DISTINCT for GROUP is allowed since having no GROUP operations is implicitly
        // a DISTINCT
        @SerializedName(value = "GROUP", alternate = { "DISTINCT" })
        GROUP,
        @SerializedName("COUNT DISTINCT")
        COUNT_DISTINCT,
        @SerializedName("TOP K")
        TOP_K,
        @SerializedName("DISTRIBUTION")
        DISTRIBUTION,
        // The alternate value of LIMIT for RAW is allowed to preserve backward compatibility.
        @SerializedName(value = "RAW", alternate = { "LIMIT" })
        RAW
    }

    /**
     * Returns a new {@link Strategy} instance that can handle this aggregation.
     *
     * @param aggregation The non-null, initialized {@link Aggregation} instance whose strategy is required.
     * @param config The {@link BulletConfig} containing configuration for the strategy.
     *
     * @return The created instance of a strategy that can implement the Aggregation.
     */
    public static Strategy findStrategy(Aggregation aggregation, BulletConfig config) {
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
