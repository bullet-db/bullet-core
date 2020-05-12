/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.querying.aggregations.Strategy;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@Getter @AllArgsConstructor
public abstract class Aggregation implements Configurable, Serializable {
    private static final long serialVersionUID = -4451469769203362270L;

    protected Integer size;
    protected AggregationType type;

    @Override
    public void configure(BulletConfig config) {
        int sizeDefault = config.getAs(BulletConfig.AGGREGATION_DEFAULT_SIZE, Integer.class);
        int sizeMaximum = config.getAs(BulletConfig.AGGREGATION_MAX_SIZE, Integer.class);

        // Null or not positive, then default, else min of size and max
        size = (size == null || size <= 0) ? sizeDefault : Math.min(size, sizeMaximum);
    }

    /**
     * Returns a new {@link Strategy} instance that can handle this aggregation.
     *
     * @param config The {@link BulletConfig} containing configuration for the strategy.
     *
     * @return The created instance of a strategy that can implement this aggregation.
     */
    public abstract Strategy getStrategy(BulletConfig config);

    public List<String> getFields() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + "}";
    }
}
