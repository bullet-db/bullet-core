/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.querying.AggregationOperations.AggregationType;
import com.yahoo.bullet.record.BulletRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.getListBytes;
import static com.yahoo.bullet.parsing.QueryUtils.getRunningQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeAggregationQuery;
import static java.util.Collections.emptyMap;

public class AggregationQueryTest {
    @Test(expectedExceptions = JsonParseException.class)
    public void testBadJSON() {
        getRunningQuery("{", emptyMap());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullConfig() {
        getRunningQuery("{}", null);
    }

    @Test
    public void testQueryAsString() {
        AggregationQuery query = getRunningQuery("{}", emptyMap());
        Assert.assertEquals(query.toString(), "{}", null);
    }

    @Test
    public void testAggregateIsNotNull() {
        AggregationQuery query = getRunningQuery("{}", emptyMap());
        Assert.assertNotNull(query.getData());
        query = getRunningQuery("{'aggregation': {}}", emptyMap());
        Assert.assertNotNull(query.getData());
        query = getRunningQuery("{'aggregation': null}", emptyMap());
        Assert.assertNotNull(query.getData());
    }

    @Test
    public void testCreationTime() {
        long startTime = System.currentTimeMillis();
        AggregationQuery query = getRunningQuery("{'aggregation' : {}}", emptyMap());
        long creationTime = query.getStartTime();
        long endTime = System.currentTimeMillis();
        Assert.assertTrue(creationTime >= startTime && creationTime <= endTime);
    }

    @Test
    public void testAggregationTime() {
        AggregationQuery query = getRunningQuery("{'aggregation' : {}}", emptyMap());
        long creationTime = query.getStartTime();
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, BulletConfig.DEFAULT_AGGREGATION_SIZE).forEach((x) -> query.consume(record));
        Assert.assertEquals(query.getData().getRecords().size(), (int) BulletConfig.DEFAULT_AGGREGATION_SIZE);
        long lastAggregationTime = query.getLastAggregationTime();
        Assert.assertTrue(creationTime <= lastAggregationTime);
    }

    @Test
    public void testDefaultLimiting() {
        AggregationQuery query = getRunningQuery("{'aggregation' : {}}", emptyMap());
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, BulletConfig.DEFAULT_AGGREGATION_SIZE - 1).forEach(x -> Assert.assertFalse(query.consume(record)));
        Assert.assertTrue(query.consume(record));
        Assert.assertEquals((Object) query.getData().getRecords().size(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
    }

    @Test
    public void testCustomLimiting() {
        AggregationQuery query = getRunningQuery(makeAggregationQuery(AggregationType.RAW, 10), emptyMap());
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, 9).forEach(x -> Assert.assertFalse(query.consume(record)));
        Assert.assertTrue(query.consume(record));
        Assert.assertEquals(query.getData().getRecords().size(), 10);
    }

    @Test
    public void testSizeUpperBound() {
        AggregationQuery query = getRunningQuery(makeAggregationQuery(AggregationType.RAW, 1000), emptyMap());
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, BulletConfig.DEFAULT_RAW_AGGREGATION_MAX_SIZE - 1).forEach(x -> Assert.assertFalse(query.consume(record)));
        Assert.assertTrue(query.consume(record));
        Assert.assertEquals((Object) query.getData().getRecords().size(), BulletConfig.DEFAULT_RAW_AGGREGATION_MAX_SIZE);
    }

    @Test
    public void testConfiguredUpperBound() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.AGGREGATION_MAX_SIZE, 2000);
        config.put(BulletConfig.RAW_AGGREGATION_MAX_SIZE, 200);
        AggregationQuery query = getRunningQuery(makeAggregationQuery(AggregationType.RAW, 1000), config);

        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, 199).forEach(x -> Assert.assertFalse(query.consume(record)));
        Assert.assertTrue(query.consume(record));
        Assert.assertEquals(query.getData().getRecords().size(), 200);
    }
}
