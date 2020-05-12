/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.querying.aggregations.QuantileSketchingStrategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LinearDistributionTest {
    private BulletConfig config = new BulletConfig();

    @Test
    public void testLinearDistributionAggregation() {
        LinearDistribution aggregation = new LinearDistribution("foo", DistributionType.QUANTILE, 500, 10);
        aggregation.configure(config);

        Assert.assertEquals(aggregation.getType(), AggregationType.DISTRIBUTION);
        Assert.assertEquals(aggregation.getDistributionType(), DistributionType.QUANTILE);
        Assert.assertEquals(aggregation.getNumberOfPoints(), 10);
        Assert.assertTrue(aggregation.getStrategy(config) instanceof QuantileSketchingStrategy);
    }

    @Test(expectedExceptions = BulletException.class,
          expectedExceptionsMessageRegExp = "If specifying the distribution by the number of points, the number must be positive\\.")
    public void testConstructorNumberOfPointsNonPositiveThrows() {
        new LinearDistribution("foo", DistributionType.QUANTILE, 10, 0);
    }

    @Test
    public void testToString() {
        LinearDistribution aggregation = new LinearDistribution("foo", DistributionType.QUANTILE, null, 10);

        Assert.assertEquals(aggregation.toString(), "{size: null, type: DISTRIBUTION, field: foo, distributionType: QUANTILE, numberOfPoints: 10}");
    }
}
