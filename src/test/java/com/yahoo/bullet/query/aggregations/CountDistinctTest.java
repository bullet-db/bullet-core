/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.querying.aggregations.ThetaSketchingStrategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class CountDistinctTest {
    private BulletConfig config = new BulletConfig();

    @Test
    public void testCountDistinctAggregation() {
        CountDistinct aggregation = new CountDistinct(Arrays.asList("foo", "abc"), "count");
        aggregation.configure(config);

        Assert.assertEquals(aggregation.getType(), AggregationType.COUNT_DISTINCT);
        Assert.assertEquals(aggregation.getFields(), Arrays.asList("foo", "abc"));
        Assert.assertEquals(aggregation.getName(), "count");
        Assert.assertTrue(aggregation.getStrategy(config) instanceof ThetaSketchingStrategy);
    }

    @Test(expectedExceptions = BulletException.class,
          expectedExceptionsMessageRegExp = "COUNT DISTINCT requires at least one field\\.")
    public void testConstructorMissingFieldsThrows() {
        new CountDistinct(Collections.emptyList(), "count");
    }

    @Test
    public void testToString() {
        CountDistinct aggregation = new CountDistinct(Arrays.asList("foo", "abc"), "count");

        Assert.assertEquals(aggregation.toString(), "{size: null, type: COUNT_DISTINCT, fields: [foo, abc], name: count}");
    }
}
