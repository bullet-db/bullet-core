/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.record.BulletRecordProvider;
import lombok.Getter;

@Getter
public class RegionDistributionAggregation extends DistributionAggregation {
    private static final long serialVersionUID = -7033735418893233303L;

    private double start;
    private double end;
    private double increment;

    /**
     * Constructor that creates a DISTRIBUTION aggregation with a specified max size.
     *
     * @param field The non-null field
     * @param type The non-null distribution type
     * @param size The max size of the DISTRIBUTION aggregation. Can be null.
     * @param start The start of the range of the distribution.
     * @param end The end of the range of the distribution.
     * @param increment The interval between points in the distribution.
     */
    public RegionDistributionAggregation(String field, Distribution.Type type, Integer size, double start, double end, double increment) {
        super(field, type, size);
        if (!areNumbersValid(start, end, increment)) {
            throw new BulletException("If specifying the distribution by range and interval, the start must be less than the end and the interval must be positive.",
                                      "Please specify valid values for 'start', 'end', and 'increment'.");
        }
        if (isRangeInvalid(start, end, type)) {
            throw new BulletException("The quantile distribution requires points to be within the interval [0, 1] inclusive.",
                                      "Please specify values for 'start' and 'end' within the interval [0, 1] inclusive.");
        }
        this.start = start;
        this.end = end;
        this.increment = increment;
    }

    @Override
    public QuantileSketch getSketch(BulletConfig config) {
        int entries = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES, Integer.class);
        int rounding = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_GENERATED_POINTS_ROUNDING, Integer.class);
        int pointLimit = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, Integer.class);
        int maxPoints = Math.min(pointLimit, size);
        BulletRecordProvider provider = config.getBulletRecordProvider();
        return new QuantileSketch(entries, distributionType, generatePoints(maxPoints, rounding), provider);
    }

    private double[] generatePoints(int maxPoints, int rounding) {
        int numPoints = Math.min((int) ((end - start) / increment), maxPoints);
        double[] points = new double[numPoints];
        double d = start;
        for (int i = 0; i < numPoints; ++i) {
            points[i] = Utilities.round(d, rounding);
            d += increment;
        }
        return points;
    }

    private static boolean areNumbersValid(double start, double end, double increment) {
        return start < end && increment > 0.0;
    }

    private static boolean isRangeInvalid(double start, double end, Distribution.Type type) {
        // if type is QUANTILE, invalid range if start < 0 or end > 1
        return type == Distribution.Type.QUANTILE && (start < 0.0 || end > 1.0);
    }
}
