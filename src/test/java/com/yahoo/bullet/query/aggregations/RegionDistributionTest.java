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

public class RegionDistributionTest {
    private BulletConfig config = new BulletConfig();

    @Test
    public void testRegionDistributionAggregation() {
        RegionDistribution aggregation = new RegionDistribution("foo", DistributionType.QUANTILE, 500, 0.0, 1.0, 0.2);
        aggregation.configure(config);

        Assert.assertEquals(aggregation.getType(), AggregationType.DISTRIBUTION);
        Assert.assertEquals(aggregation.getDistributionType(), DistributionType.QUANTILE);
        Assert.assertEquals(aggregation.getStart(), 0.0);
        Assert.assertEquals(aggregation.getEnd(), 1.0);
        Assert.assertEquals(aggregation.getIncrement(), 0.2);
        Assert.assertTrue(aggregation.getStrategy(config) instanceof QuantileSketchingStrategy);
    }

    @Test(expectedExceptions = BulletException.class,
          expectedExceptionsMessageRegExp = "If specifying the distribution by range and interval, the start must be less than the end and the interval must be positive\\.")
    public void testConstructorBadStartEnd() {
        new RegionDistribution("foo", DistributionType.QUANTILE, 500, 1000.0, 0.0, 100.0);
    }

    @Test(expectedExceptions = BulletException.class,
          expectedExceptionsMessageRegExp = "If specifying the distribution by range and interval, the start must be less than the end and the interval must be positive\\.")
    public void testConstructorBadIncrement() {
        new RegionDistribution("foo", DistributionType.QUANTILE, 500, 0.0, 1000.0, -100.0);
    }

    @Test(expectedExceptions = BulletException.class,
          expectedExceptionsMessageRegExp = "The QUANTILE distribution requires points to be within the interval \\[0, 1\\] inclusive\\.")
    public void testConstructorInvalidPointsLow() {
        new RegionDistribution("foo", DistributionType.QUANTILE, 500, -1.0, 0.0, 1.0);
    }

    @Test(expectedExceptions = BulletException.class,
          expectedExceptionsMessageRegExp = "The QUANTILE distribution requires points to be within the interval \\[0, 1\\] inclusive\\.")
    public void testConstructorInvalidPointsHigh() {
        new RegionDistribution("foo", DistributionType.QUANTILE, 500, 2.0, 3.0, 1.0);
    }

    @Test
    public void testToString() {
        RegionDistribution aggregation = new RegionDistribution("foo", DistributionType.CDF, null, 0.0, 10000.0, 1000.0);

        Assert.assertEquals(aggregation.toString(), "{size: null, type: DISTRIBUTION, field: foo, distributionType: CDF, start: 0.0, end: 10000.0, increment: 1000.0}");
    }
}
