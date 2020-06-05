/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.querying.aggregations.FrequentItemsSketchingStrategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class TopKTest {
    private static final String COUNT_NAME = "count";
    private BulletConfig config = new BulletConfig();

    @Test
    public void testTopKAggregation() {
        TopK aggregation = new TopK(Collections.singletonMap("abc", "def"), null, null, COUNT_NAME);
        aggregation.configure(config);

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "def"));
        Assert.assertTrue(aggregation.getStrategy(config) instanceof FrequentItemsSketchingStrategy);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullFieldsThrows() {
        new TopK(null, null, null, null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "TOP K requires at least one field\\.")
    public void testConstructorEmptyFieldsThrows() {
        new TopK(Collections.emptyMap(), null, null, null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullNameThrows() {
        new TopK(Collections.singletonMap("abc", "def"), null, null, null);
    }

    @Test
    public void testGetThreshold() {
        TopK aggregation = new TopK(Collections.singletonMap("abc", "def"), null, 100L, COUNT_NAME);
        Assert.assertEquals(aggregation.getThreshold(), (Long) 100L);
    }

    @Test
    public void testGetName() {
        TopK aggregation = new TopK(Collections.singletonMap("abc", "def"), null, null, COUNT_NAME);
        Assert.assertEquals(aggregation.getName(), COUNT_NAME);
    }

    @Test
    public void testGetToString() {
        TopK aggregation = new TopK(Collections.singletonMap("abc", "def"), 500, 100L, COUNT_NAME);
        Assert.assertEquals(aggregation.toString(), "{size: 500, type: TOP_K, fieldsToNames: {abc=def}, threshold: 100, name: count}");
    }
}
