/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.postaggregations.Computation;
import com.yahoo.bullet.postaggregations.OrderBy;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PostAggregationOperationsTest {
    @Test(expectedExceptions = NullPointerException.class)
    public void testNullType() {
        PostAggregation aggregation = new PostAggregation();
        aggregation.setType(null);
        PostAggregationOperations.findPostStrategy(aggregation);
    }

    @Test
    public void testOrderByPostStrategy() {
        PostAggregation aggregation = new PostAggregation();

        Assert.assertEquals(PostAggregationOperations.findPostStrategy(aggregation).getClass(), OrderBy.class);
    }

    @Test
    public void testComputationPostStrategy() {
        PostAggregation aggregation = new PostAggregation();
        aggregation.setType(PostAggregation.Type.COMPUTATION);

        Assert.assertEquals(PostAggregationOperations.findPostStrategy(aggregation).getClass(), Computation.class);
    }
}
