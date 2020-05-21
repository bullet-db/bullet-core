/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.common.BulletException;
import lombok.Getter;

@Getter
public class LinearDistribution extends Distribution {
    private static final long serialVersionUID = -5320252906943658246L;
    private static final BulletException NUMBER_OF_POINTS_MUST_BE_POSITIVE =
            new BulletException("If specifying the distribution by the number of points, the number must be positive.", "Please specify a positive number.");

    private final int numberOfPoints;

    /**
     * Constructor that creates a DISTRIBUTION aggregation with a specified max size. The number of equidistant
     * points in this distribution are specified.
     *
     * @param field The non-null field
     * @param type The non-null distribution type
     * @param size The max size of the DISTRIBUTION aggregation. Can be null.
     * @param numberOfPoints The number of equidistant points for this distribution.
     */
    public LinearDistribution(String field, DistributionType type, Integer size, int numberOfPoints) {
        super(field, type, size);
        if (numberOfPoints <= 0) {
            throw NUMBER_OF_POINTS_MUST_BE_POSITIVE;
        }
        this.numberOfPoints = numberOfPoints;
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", field: " + field + ", distributionType: " + distributionType + ", numberOfPoints: " + numberOfPoints + "}";
    }
}
