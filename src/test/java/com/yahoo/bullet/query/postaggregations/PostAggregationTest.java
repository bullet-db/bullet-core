package com.yahoo.bullet.query.postaggregations;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PostAggregationTest {
    @Test
    public void testPriority() {
        Assert.assertTrue(PostAggregation.Type.HAVING.getPriority() < PostAggregation.Type.COMPUTATION.getPriority());
        Assert.assertTrue(PostAggregation.Type.COMPUTATION.getPriority() < PostAggregation.Type.ORDER_BY.getPriority());
        Assert.assertTrue(PostAggregation.Type.ORDER_BY.getPriority() < PostAggregation.Type.CULLING.getPriority());
    }
}
