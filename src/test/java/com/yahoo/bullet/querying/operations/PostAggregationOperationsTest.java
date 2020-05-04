/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.operations;

public class PostAggregationOperationsTest {
    /*
    @Test(expectedExceptions = NullPointerException.class)
    public void testNullType() {
        PostAggregation aggregation = new OrderBy();
        aggregation.setType(null);
        PostAggregationOperations.findPostStrategy(aggregation);
    }

    @Test
    public void testOrderByPostStrategy() {
        PostAggregation aggregation = new OrderBy();
        aggregation.setType(PostAggregation.Type.ORDER_BY);
        Assert.assertEquals(PostAggregationOperations.findPostStrategy(aggregation).getClass(), OrderByStrategy.class);
    }

    @Test
    public void testComputationPostStrategy() {
        PostAggregation aggregation = new Computation(Collections.emptyList());
        aggregation.setType(PostAggregation.Type.COMPUTATION);
        Assert.assertEquals(PostAggregationOperations.findPostStrategy(aggregation).getClass(), ComputationStrategy.class);
    }
    */
}
