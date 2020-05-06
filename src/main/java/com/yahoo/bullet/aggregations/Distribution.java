/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.DistributionAggregation;
import com.yahoo.bullet.record.BulletRecord;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.yahoo.bullet.common.Utilities.extractFieldAsNumber;

/**
 * This {@link Strategy} uses {@link QuantileSketch} to find distributions of a numeric field. Based on the size
 * configured for the sketch, the normalized rank error can be determined and tightly bound.
 */
public class Distribution extends SketchingStrategy<QuantileSketch> {
    @Getter @AllArgsConstructor
    public enum Type {
        QUANTILE("QUANTILE"),
        PMF("FREQ"),
        CDF("CUMFREQ");

        private String name;
    }

    private String field;

    /**
     * Constructor that requires an {@link Aggregation} and a {@link BulletConfig} configuration.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     * @param config The config that has relevant configs for this strategy.
     */
    @SuppressWarnings("unchecked")
    public Distribution(DistributionAggregation aggregation, BulletConfig config) {
        super(aggregation, config);
        field = aggregation.getField();
        sketch = aggregation.getSketch(config);
    }

    @Override
    public void consume(BulletRecord data) {
        Number value = extractFieldAsNumber(field, data);
        if (value != null) {
            sketch.update(value.doubleValue());
        }
    }
}
