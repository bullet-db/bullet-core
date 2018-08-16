/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import org.testng.Assert;
import org.testng.annotations.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

public class PostAggregationTest {
    @Test
    public void testToString() {
        PostAggregation aggregation = new PostAggregation();

        Assert.assertEquals(aggregation.toString(), "{type: ORDER_BY, attributes: null}");

        aggregation.setType(PostAggregation.Type.ORDER_BY);
        Assert.assertEquals(aggregation.toString(), "{type: ORDER_BY, attributes: null}");

        aggregation.setAttributes(singletonMap("foo", asList(1, 2, 3)));
        Assert.assertEquals(aggregation.toString(), "{type: ORDER_BY, attributes: {foo=[1, 2, 3]}}");
    }
}
