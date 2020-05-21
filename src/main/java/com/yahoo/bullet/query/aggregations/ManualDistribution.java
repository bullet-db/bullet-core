/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Getter
public class ManualDistribution extends Distribution {
    private static final long serialVersionUID = 7022392121466809427L;
    private static final BulletException MANUAL_DISTRIBUTION_MISSING_POINTS =
            new BulletException("If specifying the distribution by a list of points, the list must contain at least one point.",
                                "Please specify at least one point.");
    private static final BulletException QUANTILE_POINTS_INVALID_RANGE =
            new BulletException("The quantile distribution requires points to be within the interval [0, 1] inclusive.",
                                "Please specify a list of valid points.");


    private List<Double> points;

    /**
     * Constructor that creates a DISTRIBUTION aggregation with a specified max size. The points in this distribution
     * are specified manually.
     *
     * @param field The non-null field
     * @param type The non-null distribution type
     * @param size The max size of the DISTRIBUTION aggregation. Can be null.
     * @param points The points of the distribution. Must be not be empty.
     */
    public ManualDistribution(String field, DistributionType type, Integer size, List<Double> points) {
        super(field, type, size);
        Utilities.requireNonNull(points);
        if (points.isEmpty()) {
            throw MANUAL_DISTRIBUTION_MISSING_POINTS;
        }
        if (isListInvalid(points, type)) {
            throw QUANTILE_POINTS_INVALID_RANGE;
        }
        this.points = points.stream().distinct().sorted().collect(Collectors.toCollection(ArrayList::new));
    }

    private static boolean isListInvalid(List<Double> points, DistributionType type) {
        // if type is QUANTILE, invalid range if start < 0 or end > 1
        return type == DistributionType.QUANTILE && (points.get(0) < 0.0 || points.get(points.size() - 1) > 1.0);
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", field: " + field + ", distributionType: " + distributionType + ", points: " + points + "}";
    }
}
