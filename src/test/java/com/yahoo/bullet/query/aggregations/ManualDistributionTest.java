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

import java.util.Arrays;
import java.util.Collections;

public class ManualDistributionTest {
    @Test
    public void testManualDistributionAggregation() {
        ManualDistribution aggregation = new ManualDistribution("foo", DistributionType.QUANTILE, 500, Arrays.asList(0.2, 0.3, 0.5));
        BulletConfig config = new BulletConfig();
        aggregation.configure(config);

        Assert.assertEquals(aggregation.getType(), AggregationType.DISTRIBUTION);
        Assert.assertEquals(aggregation.getDistributionType(), DistributionType.QUANTILE);
        Assert.assertEquals(aggregation.getPoints(), Arrays.asList(0.2, 0.3, 0.5));
        Assert.assertTrue(aggregation.getStrategy(config) instanceof QuantileSketchingStrategy);
    }

    @Test(expectedExceptions = BulletException.class,
          expectedExceptionsMessageRegExp = "If specifying the distribution by a list of points, the list must contain at least one point\\.")
    public void testConstructorMissingPoints() {
        new ManualDistribution("foo", DistributionType.QUANTILE, 500, Collections.emptyList());
    }

    @Test(expectedExceptions = BulletException.class,
          expectedExceptionsMessageRegExp = "The QUANTILE distribution requires points to be within the interval \\[0, 1\\] inclusive\\.")
    public void testConstructorInvalidPointsLow() {
        new ManualDistribution("foo", DistributionType.QUANTILE, 500, Arrays.asList(-1.0));
    }

    @Test(expectedExceptions = BulletException.class,
          expectedExceptionsMessageRegExp = "The QUANTILE distribution requires points to be within the interval \\[0, 1\\] inclusive\\.")
    public void testConstructorInvalidPointsHigh() {
        new ManualDistribution("foo", DistributionType.QUANTILE, 500, Arrays.asList(2.0));
    }

    @Test
    public void testToString() {
        ManualDistribution aggregation = new ManualDistribution("foo", DistributionType.PMF, null, Arrays.asList(0.0, 10000.0));

        Assert.assertEquals(aggregation.toString(), "{size: null, type: DISTRIBUTION, field: foo, distributionType: PMF, points: [0.0, 10000.0]}");
    }
}
