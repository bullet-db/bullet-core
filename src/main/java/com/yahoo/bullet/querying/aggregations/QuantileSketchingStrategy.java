/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.aggregations;

import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.query.aggregations.LinearDistribution;
import com.yahoo.bullet.query.aggregations.ManualDistribution;
import com.yahoo.bullet.query.aggregations.RegionDistribution;
import com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.Distribution;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;

import java.util.List;

import static com.yahoo.bullet.common.Utilities.extractFieldAsNumber;

/**
 * This {@link Strategy} uses {@link QuantileSketch} to find distributions of a numeric field. Based on the size
 * configured for the sketch, the normalized rank error can be determined and tightly bound.
 */
public class QuantileSketchingStrategy extends SketchingStrategy<QuantileSketch> {
    private String field;

    /**
     * Constructor that requires an {@link Aggregation} and a {@link BulletConfig} configuration.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     * @param config The config that has relevant configs for this strategy.
     */
    @SuppressWarnings("unchecked")
    public QuantileSketchingStrategy(Distribution aggregation, BulletConfig config) {
        super(aggregation, config);
        field = aggregation.getField();
        sketch = getSketch(aggregation, config);
    }

    @Override
    public void consume(BulletRecord data) {
        Number value = extractFieldAsNumber(field, data);
        if (value != null) {
            sketch.update(value.doubleValue());
        }
    }

    private static QuantileSketch getSketch(Distribution aggregation, BulletConfig config) {
        int entries = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES, Integer.class);
        int rounding = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_GENERATED_POINTS_ROUNDING, Integer.class);
        int pointLimit = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, Integer.class);
        int maxPoints = Math.min(pointLimit, aggregation.getSize());
        BulletRecordProvider provider = config.getBulletRecordProvider();
        if (aggregation instanceof LinearDistribution) {
            int numberOfPoints = ((LinearDistribution) aggregation).getNumberOfPoints();
            return new QuantileSketch(entries, rounding, aggregation.getDistributionType(), Math.min(numberOfPoints, maxPoints), provider);
        } else if (aggregation instanceof ManualDistribution) {
            // Limit number of points
            List<Double> points = ((ManualDistribution) aggregation).getPoints();
            double[] cleanedPoints = points.stream().limit(maxPoints).mapToDouble(d -> d).toArray();
            return new QuantileSketch(entries, aggregation.getDistributionType(), cleanedPoints, provider);
        } else if (aggregation instanceof RegionDistribution) {
            RegionDistribution distribution = (RegionDistribution) aggregation;
            double start = distribution.getStart();
            double end = distribution.getEnd();
            double increment = distribution.getIncrement();
            return new QuantileSketch(entries, aggregation.getDistributionType(), getPoints(start, end, increment, maxPoints, rounding), provider);
        }
        throw new IllegalArgumentException("Unknown distribution input mode.");
    }

    private static double[] getPoints(double start, double end, double increment, int maxPoints, int rounding) {
        int numberOfPoints = Math.min((int) ((end - start) / increment) + 1, maxPoints);
        return Utilities.generatePoints(start, num -> num + increment, numberOfPoints, rounding);
    }
}
