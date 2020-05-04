/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class TopKAggregationTest {
    private BulletConfig config = new BulletConfig();

    @Test
    public void testTopKAggregation() {
        TopKAggregation aggregation = new TopKAggregation(Collections.singletonMap("abc", "def"), null);
        aggregation.configure(config);

        Assert.assertEquals(aggregation.getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "def"));
        Assert.assertTrue(aggregation.getStrategy(config) instanceof TopK);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullFieldsThrows() {
        new TopKAggregation(null, null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "TOP K requires at least one field\\.")
    public void testConstructorEmptyFieldsThrows() {
        new TopKAggregation(Collections.emptyMap(), null);
    }

    @Test
    public void testGetThreshold() {
        TopKAggregation aggregation = new TopKAggregation(Collections.singletonMap("abc", "def"), null);

        Assert.assertNull(aggregation.getThreshold());

        aggregation.setThreshold(100L);

        Assert.assertEquals(aggregation.getThreshold(), (Long) 100L);
    }

    @Test
    public void testGetName() {
        TopKAggregation aggregation = new TopKAggregation(Collections.singletonMap("abc", "def"), null);

        Assert.assertEquals(aggregation.getName(), TopKAggregation.DEFAULT_NAME);

        aggregation.setName("count");

        Assert.assertEquals(aggregation.getName(), "count");
    }

    @Test
    public void testGetToString() {
        TopKAggregation aggregation = new TopKAggregation(Collections.singletonMap("abc", "def"), 500);
        aggregation.setThreshold(100L);
        aggregation.setName("count");

        Assert.assertEquals(aggregation.toString(), "{size: 500, type: TOP_K, fields: {abc=def}, threshold: 100, name: count}");
    }
}
