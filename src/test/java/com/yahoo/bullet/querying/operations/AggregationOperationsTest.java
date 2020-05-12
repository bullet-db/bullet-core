/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.operations;

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

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), RawStrategy.class);
    }

    @Test
    public void testGroupAllStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(Aggregation.DistributionType.GROUP);
        aggregation.setAttributes(singletonMap(GroupOperation.OPERATIONS,
                                  singletonList(singletonMap(GroupOperation.OPERATION_TYPE,
                                                GroupOperation.GroupOperationType.COUNT.getName()))));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), GroupStrategy.class);
    }

    @Test
    public void testCountDistinctStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(Aggregation.DistributionType.COUNT_DISTINCT);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), ThetaSketchingStrategy.class);
    }

    @Test
    public void testDistinctStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(Aggregation.DistributionType.GROUP);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), TupleSketchingStrategy.class);
    }

    @Test
    public void testGroupByStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(Aggregation.DistributionType.GROUP);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.setAttributes(singletonMap(GroupOperation.OPERATIONS,
                                  singletonList(singletonMap(GroupOperation.OPERATION_TYPE,
                                                GroupOperation.GroupOperationType.COUNT.getName()))));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), TupleSketchingStrategy.class);
    }

    @Test
    public void testDistributionStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(Aggregation.DistributionType.DISTRIBUTION);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), QuantileSketchingStrategy.class);
    }

    @Test
    public void testTopKStrategy() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();
        aggregation.setType(Aggregation.DistributionType.TOP_K);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(config);

        Assert.assertEquals(AggregationOperations.findStrategy(aggregation, config).getClass(), FrequentItemsSketchingStrategy.class);
    }
    */
}
