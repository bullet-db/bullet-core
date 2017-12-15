/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations;

import com.yahoo.bullet.querying.AggregationOperations;
import com.yahoo.bullet.querying.AggregationOperations.AggregationOperator;
import com.yahoo.bullet.querying.AggregationOperations.DistributionType;
import com.yahoo.bullet.querying.AggregationOperations.GroupOperationType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AggregationOperationsTest {
    @Test
    public void testGroupOperationTypeIdentifying() {
        GroupOperationType count = GroupOperationType.COUNT;
        Assert.assertFalse(count.isMe("count"));
        Assert.assertFalse(count.isMe(null));
        Assert.assertFalse(count.isMe(""));
        Assert.assertFalse(count.isMe(GroupOperationType.SUM.getName()));
        Assert.assertTrue(count.isMe(GroupOperationType.COUNT.getName()));
    }

    @Test
    public void testCustomAggregationOperator() {
        AggregationOperator  product = (a, b) -> a != null && b != null ? a.doubleValue() * b.doubleValue() : null;
        Assert.assertNull(product.apply(6L, null));
        Assert.assertEquals(product.apply(6L, 2L).longValue(), 12L);
        Assert.assertEquals(product.apply(6.1, 2L).doubleValue(), 12.2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMinUnsupported() {
        AggregationOperations.MIN.apply(null, 2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMaxUnsupported() {
        AggregationOperations.MAX.apply(null, 2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCountUnsupported() {
        AggregationOperations.COUNT.apply(null, 2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testSumUnsupported() {
        AggregationOperations.MAX.apply(null, 2);
    }

    @Test
    public void testMin() {
        Assert.assertEquals(AggregationOperations.MIN.apply(1, 2).intValue(), 1);
        Assert.assertEquals(AggregationOperations.MIN.apply(2.1, 1.2).doubleValue(), 1.2);
        Assert.assertEquals(AggregationOperations.MIN.apply(1.0, 1.0).doubleValue(), 1.0);
    }

    @Test
    public void testMax() {
        Assert.assertEquals(AggregationOperations.MAX.apply(1, 2).intValue(), 2);
        Assert.assertEquals(AggregationOperations.MAX.apply(2.1, 1.2).doubleValue(), 2.1);
        Assert.assertEquals(AggregationOperations.MAX.apply(1.0, 1.0).doubleValue(), 1.0);
    }

    @Test
    public void testSum() {
        Assert.assertEquals(AggregationOperations.SUM.apply(1, 2).intValue(), 3);
        Assert.assertEquals(AggregationOperations.SUM.apply(2.1, 1.2).doubleValue(), 3.3);
        Assert.assertEquals(AggregationOperations.SUM.apply(2.0, 41).longValue(), 43L);
    }

    @Test
    public void testCount() {
        Assert.assertEquals(AggregationOperations.COUNT.apply(1, 2).intValue(), 3);
        Assert.assertEquals(AggregationOperations.COUNT.apply(2.1, 1.2).doubleValue(), 3.0);
        Assert.assertEquals(AggregationOperations.COUNT.apply(1.0, 41).longValue(), 42L);
    }

    @Test
    public void testDistributionTypeIdentifying() {
        Assert.assertFalse(DistributionType.QUANTILE.isMe("quantile"));
        Assert.assertFalse(DistributionType.QUANTILE.isMe("foo"));
        Assert.assertFalse(DistributionType.QUANTILE.isMe(null));
        Assert.assertFalse(DistributionType.QUANTILE.isMe(""));
        Assert.assertFalse(DistributionType.QUANTILE.isMe(DistributionType.PMF.getName()));
        Assert.assertTrue(DistributionType.QUANTILE.isMe(DistributionType.QUANTILE.getName()));
    }
}
