/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.record.BulletRecordProvider;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Getter
public class ManualDistributionAggregation extends DistributionAggregation {
    private static final long serialVersionUID = 7022392121466809427L;

    private List<Double> points;

    /**
     * Constructor that creates a DISTRIBUTION aggregation with a specified max size.
     *
     * @param field The non-null field
     * @param type The non-null distribution type
     * @param size The max size of the DISTRIBUTION aggregation. Can be null.
     * @param points The points of the distribution. Must be not be empty.
     */
    public ManualDistributionAggregation(String field, Distribution.Type type, Integer size, List<Double> points) {
        super(field, type, size);
        Utilities.requireNonNullList(points);
        if (points.isEmpty()) {
            throw new BulletException("If specifying the distribution by a list of points, the list must contain at least one point.",
                                      "Please specify at least one point.");
        }
        if (isListInvalid(points, type)) {
            throw new BulletException("The quantile distribution requires points to be within the interval [0, 1] inclusive.",
                                      "Please specify a list of valid points.");
        }
        this.points = points.stream().distinct().sorted().collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public QuantileSketch getSketch(BulletConfig config) {
        int entries = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES, Integer.class);
        int pointLimit = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, Integer.class);
        int maxPoints = Math.min(pointLimit, size);
        BulletRecordProvider provider = config.getBulletRecordProvider();
        // Limit number of points
        double[] cleanedPoints = points.stream().limit(maxPoints).mapToDouble(d -> d).toArray();
        return new QuantileSketch(entries, distributionType, cleanedPoints, provider);
    }

    private static boolean isListInvalid(List<Double> points, Distribution.Type type) {
        // if type is QUANTILE, invalid range if start < 0 or end > 1
        return type == Distribution.Type.QUANTILE && (points.get(0) < 0.0 || points.get(points.size() - 1) > 1.0);
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", field: " + field + ", distributionType: " + distributionType + ", points: " + points + "}";
    }
}
