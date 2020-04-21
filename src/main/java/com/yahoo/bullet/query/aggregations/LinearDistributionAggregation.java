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
import com.yahoo.bullet.record.BulletRecordProvider;
import lombok.Getter;

@Getter
public class LinearDistributionAggregation extends DistributionAggregation {
    private static final long serialVersionUID = -5320252906943658246L;

    private final int numberOfPoints;

    /**
     * Constructor that creates a DISTRIBUTION aggregation with a specified max size.
     *
     * @param field The non-null field
     * @param type The non-null distribution type
     * @param size The max size of the DISTRIBUTION aggregation. Can be null.
     * @param numberOfPoints The number of equidistant points for this distribution.
     */
    public LinearDistributionAggregation(String field, Distribution.Type type, Integer size, int numberOfPoints) {
        super(field, type, size);
        if (numberOfPoints <= 0) {
            throw new BulletException("If specifying the distribution by number of points, the number must be positive.", "Please specify a positive number.");
        }
        this.numberOfPoints = numberOfPoints;
    }

    @Override
    public QuantileSketch getSketch(BulletConfig config) {
        int entries = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES, Integer.class);
        int rounding = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_GENERATED_POINTS_ROUNDING, Integer.class);
        int pointLimit = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, Integer.class);
        int maxPoints = Math.min(pointLimit, size);
        BulletRecordProvider provider = config.getBulletRecordProvider();
        return new QuantileSketch(entries, rounding, distributionType, Math.min(numberOfPoints, maxPoints), provider);
    }
}
