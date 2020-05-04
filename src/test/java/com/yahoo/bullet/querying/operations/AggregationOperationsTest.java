/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.operations;

import com.yahoo.bullet.aggregations.CountDistinct;
import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.GroupAll;
import com.yahoo.bullet.aggregations.GroupBy;
import com.yahoo.bullet.aggregations.Raw;
import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.aggregations.Aggregation;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class AggregationOperationsTest {
    /*
    @Test(expectedExceptions = NullPointerException.class)
    public void testNullType() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(null);
        aggregation.configure(config);
        AggregationOperations.findStrategy(aggregation, config);
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
        aggregation.setType(Aggregation.Type.GROUP);
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
        aggregation.setType(Aggregation.Type.COUNT_DISTINCT);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), CountDistinct.class);
    }

    @Test
    public void testDistinctStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(Aggregation.Type.GROUP);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), GroupBy.class);
    }

    @Test
    public void testGroupByStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(Aggregation.Type.GROUP);
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
        aggregation.setType(Aggregation.Type.DISTRIBUTION);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), Distribution.class);
    }

    @Test
    public void testTopKStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(Aggregation.Type.TOP_K);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), TopK.class);
    }
    */
}
