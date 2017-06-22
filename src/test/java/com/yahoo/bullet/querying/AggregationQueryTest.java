/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.aggregations.Raw;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.getListBytes;
import static com.yahoo.bullet.parsing.QueryUtils.getAggregationQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeAggregationQuery;
import static java.util.Collections.emptyMap;

public class AggregationQueryTest {
    @Test(expectedExceptions = JsonParseException.class)
    public void testBadJSON() {
        getAggregationQuery("{", emptyMap());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullConfig() {
        getAggregationQuery("{}", null);
    }

    @Test
    public void testQueryAsString() {
        AggregationQuery query = getAggregationQuery("{}", emptyMap());
        Assert.assertEquals(query.toString(), "{}", null);
    }

    @Test
    public void testAggregateIsNotNull() {
        AggregationQuery query = getAggregationQuery("{}", emptyMap());
        Assert.assertNotNull(query.getData());
        query = getAggregationQuery("{'aggregation': {}}", emptyMap());
        Assert.assertNotNull(query.getData());
        query = getAggregationQuery("{'aggregation': null}", emptyMap());
        Assert.assertNotNull(query.getData());
    }

    @Test
    public void testCreationTime() {
        long startTime = System.currentTimeMillis();
        AggregationQuery query = getAggregationQuery("{'aggregation' : {}}", emptyMap());
        long creationTime = query.getStartTime();
        long endTime = System.currentTimeMillis();
        Assert.assertTrue(creationTime >= startTime && creationTime <= endTime);
    }

    @Test
    public void testAggregationTime() {
        AggregationQuery query = getAggregationQuery("{'aggregation' : {}}", emptyMap());
        long creationTime = query.getStartTime();
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, Aggregation.DEFAULT_SIZE).forEach((x) -> query.consume(record));
        Assert.assertEquals(query.getData().getRecords().size(), (int) Aggregation.DEFAULT_SIZE);
        long lastAggregationTime = query.getLastAggregationTime();
        Assert.assertTrue(creationTime <= lastAggregationTime);
    }

    @Test
    public void testDefaultLimiting() {
        AggregationQuery query = getAggregationQuery("{'aggregation' : {}}", emptyMap());
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, Aggregation.DEFAULT_SIZE - 1).forEach(x -> Assert.assertFalse(query.consume(record)));
        Assert.assertTrue(query.consume(record));
        Assert.assertEquals((Integer) query.getData().getRecords().size(), Aggregation.DEFAULT_SIZE);
    }

    @Test
    public void testCustomLimiting() {
        AggregationQuery query = getAggregationQuery(makeAggregationQuery(AggregationType.RAW, 10), emptyMap());
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, 9).forEach(x -> Assert.assertFalse(query.consume(record)));
        Assert.assertTrue(query.consume(record));
        Assert.assertEquals(query.getData().getRecords().size(), 10);
    }

    @Test
    public void testSizeUpperBound() {
        AggregationQuery query = getAggregationQuery(makeAggregationQuery(AggregationType.RAW, 1000), emptyMap());
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, Raw.DEFAULT_MAX_SIZE - 1).forEach(x -> Assert.assertFalse(query.consume(record)));
        Assert.assertTrue(query.consume(record));
        Assert.assertEquals((Integer) query.getData().getRecords().size(), Raw.DEFAULT_MAX_SIZE);
    }

    @Test
    public void testConfiguredUpperBound() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.AGGREGATION_MAX_SIZE, 2000);
        config.put(BulletConfig.RAW_AGGREGATION_MAX_SIZE, 200);

        AggregationQuery query = getAggregationQuery(makeAggregationQuery(AggregationType.RAW, 1000), config);
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, 199).forEach(x -> Assert.assertFalse(query.consume(record)));
        Assert.assertTrue(query.consume(record));
        Assert.assertEquals(query.getData().getRecords().size(), 200);
    }
}
