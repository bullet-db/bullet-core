/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.aggregations.CountDistinct;
import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.GroupAll;
import com.yahoo.bullet.aggregations.GroupBy;
import com.yahoo.bullet.aggregations.Raw;
import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.aggregations.grouping.GroupOperation.AggregationOperator;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class AggregationOperationsTest {
    @Test
    public void testGroupOperationTypeIdentifying() {
        GroupOperation.GroupOperationType count = GroupOperation.GroupOperationType.COUNT;
        Assert.assertFalse(count.isMe("count"));
        Assert.assertFalse(count.isMe(null));
        Assert.assertFalse(count.isMe(""));
        Assert.assertFalse(count.isMe(GroupOperation.GroupOperationType.SUM.getName()));
        Assert.assertTrue(count.isMe(GroupOperation.GroupOperationType.COUNT.getName()));
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
        GroupOperation.MIN.apply(null, 2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMaxUnsupported() {
        GroupOperation.MAX.apply(null, 2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCountUnsupported() {
        GroupOperation.COUNT.apply(null, 2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testSumUnsupported() {
        GroupOperation.MAX.apply(null, 2);
    }

    @Test
    public void testMin() {
        Assert.assertEquals(GroupOperation.MIN.apply(1, 2).intValue(), 1);
        Assert.assertEquals(GroupOperation.MIN.apply(2.1, 1.2).doubleValue(), 1.2);
        Assert.assertEquals(GroupOperation.MIN.apply(1.0, 1.0).doubleValue(), 1.0);
    }

    @Test
    public void testMax() {
        Assert.assertEquals(GroupOperation.MAX.apply(1, 2).intValue(), 2);
        Assert.assertEquals(GroupOperation.MAX.apply(2.1, 1.2).doubleValue(), 2.1);
        Assert.assertEquals(GroupOperation.MAX.apply(1.0, 1.0).doubleValue(), 1.0);
    }

    @Test
    public void testSum() {
        Assert.assertEquals(GroupOperation.SUM.apply(1, 2).intValue(), 3);
        Assert.assertEquals(GroupOperation.SUM.apply(2.1, 1.2).doubleValue(), 3.3);
        Assert.assertEquals(GroupOperation.SUM.apply(2.0, 41).longValue(), 43L);
    }

    @Test
    public void testCount() {
        Assert.assertEquals(GroupOperation.COUNT.apply(1, 2).intValue(), 3);
        Assert.assertEquals(GroupOperation.COUNT.apply(2.1, 1.2).doubleValue(), 3.0);
        Assert.assertEquals(GroupOperation.COUNT.apply(1.0, 41).longValue(), 42L);
    }

    @Test
    public void testDistributionTypeIdentifying() {
        Assert.assertFalse(Distribution.DistributionType.QUANTILE.isMe("quantile"));
        Assert.assertFalse(Distribution.DistributionType.QUANTILE.isMe("foo"));
        Assert.assertFalse(Distribution.DistributionType.QUANTILE.isMe(null));
        Assert.assertFalse(Distribution.DistributionType.QUANTILE.isMe(""));
        Assert.assertFalse(Distribution.DistributionType.QUANTILE.isMe(Distribution.DistributionType.PMF.getName()));
        Assert.assertTrue(Distribution.DistributionType.QUANTILE.isMe(Distribution.DistributionType.QUANTILE.getName()));
    }
    @Test(expectedExceptions = NullPointerException.class)
    public void testUnimplementedStrategies() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(null);
        aggregation.configure(config);

        Assert.assertNull(AggregationOperations.findStrategy(aggregation, config));
    }

    @Test
    public void testRawStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), Raw.class);
    }

    @Test
    public void testGroupAllStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(AggregationOperations.AggregationType.GROUP);
        aggregation.setAttributes(singletonMap(GroupOperation.OPERATIONS,
                                  singletonList(singletonMap(GroupOperation.OPERATION_TYPE,
                                                GroupOperation.GroupOperationType.COUNT.getName()))));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), GroupAll.class);
    }

    @Test
    public void testCountDistinctStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(AggregationOperations.AggregationType.COUNT_DISTINCT);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), CountDistinct.class);
    }

    @Test
    public void testDistinctStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(AggregationOperations.AggregationType.GROUP);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), GroupBy.class);
    }

    @Test
    public void testGroupByStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(AggregationOperations.AggregationType.GROUP);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.setAttributes(singletonMap(GroupOperation.OPERATIONS,
                                  singletonList(singletonMap(GroupOperation.OPERATION_TYPE,
                                                GroupOperation.GroupOperationType.COUNT.getName()))));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), GroupBy.class);
    }

    @Test
    public void testDistributionStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(AggregationOperations.AggregationType.DISTRIBUTION);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), Distribution.class);
    }

    @Test
    public void testTopKStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(AggregationOperations.AggregationType.TOP_K);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), TopK.class);
    }
}
