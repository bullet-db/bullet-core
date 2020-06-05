/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.common.BulletException;
import lombok.Getter;

@Getter
public class RegionDistribution extends Distribution {
    private static final long serialVersionUID = -7033735418893233303L;
    private static final BulletException REGION_DISTRIBUTION_INVALID_RANGE =
            new BulletException("If specifying the distribution by range and interval, the start must be less than the end and the interval must be positive.",
                                "Please specify valid values for 'start', 'end', and 'increment'.");
    private static final BulletException QUANTILE_POINTS_INVALID_RANGE =
            new BulletException("The QUANTILE distribution requires points to be within the interval [0, 1] inclusive.",
                                "Please specify values for 'start' and 'end' within the interval [0, 1] inclusive.");

    private final double start;
    private final double end;
    private final double increment;

    /**
     * Constructor that creates a DISTRIBUTION aggregation with a specified max size. The points of this distribution
     * are specified by a given range and interval between points.
     *
     * @param field The non-null field.
     * @param type The non-null distribution type.
     * @param size The max size of the DISTRIBUTION aggregation. Can be null.
     * @param start The start of the range of the distribution.
     * @param end The end of the range of the distribution.
     * @param increment The interval between points in the distribution.
     */
    public RegionDistribution(String field, DistributionType type, Integer size, double start, double end, double increment) {
        super(field, type, size);
        if (!areNumbersValid(start, end, increment)) {
            throw REGION_DISTRIBUTION_INVALID_RANGE;
        }
        if (isRangeInvalid(start, end, type)) {
            throw QUANTILE_POINTS_INVALID_RANGE;
        }
        this.start = start;
        this.end = end;
        this.increment = increment;
    }

    private static boolean areNumbersValid(double start, double end, double increment) {
        return start < end && increment > 0.0;
    }

    private static boolean isRangeInvalid(double start, double end, DistributionType type) {
        // if type is QUANTILE, invalid range if start < 0 or end > 1
        return type == DistributionType.QUANTILE && (start < 0.0 || end > 1.0);
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", field: " + field + ", distributionType: " + distributionType +
               ", start: " + start + ", end: " + end + ", increment: " + increment + "}";
    }
}
