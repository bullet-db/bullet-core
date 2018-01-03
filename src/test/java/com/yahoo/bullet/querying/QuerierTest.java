/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.FilterClauseTest;
import com.yahoo.bullet.parsing.ParsingError;
import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.querying.AggregationOperations.AggregationType;
import com.yahoo.bullet.querying.FilterOperations.FilterType;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.yahoo.bullet.TestHelpers.getListBytes;
import static com.yahoo.bullet.parsing.QueryUtils.getRunningQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeAggregationQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeProjectionFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeRawFullQuery;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QuerierTest {
    private class FailingStrategy implements Strategy {
        int consumptionFailure = 0;
        int combiningFailure = 0;
        int serializingFailure = 0;
        int aggregationFailure = 0;

        @Override
        public void consume(BulletRecord data) {
            consumptionFailure++;
            throw new RuntimeException("Consuming record test failure");
        }

        @Override
        public void combine(byte[] serializedAggregation) {
            combiningFailure++;
            throw new RuntimeException("Combining serialized aggregation test failure");
        }

        @Override
        public byte[] getData() {
            serializingFailure++;
            throw new RuntimeException("Serializing aggregation test failure");
        }

        @Override
        public Clip getResult() {
            aggregationFailure++;
            throw new RuntimeException("Getting aggregation test failure");
        }

        @Override
        public Metadata getMetadata() {
            aggregationFailure++;
            throw new RuntimeException("Getting aggregation test failure");
        }

        @Override
        public List<BulletRecord> getRecords() {
            aggregationFailure++;
            throw new RuntimeException("Getting aggregation test failure");
        }

        @Override
        public Optional<List<BulletError>> initialize() {
            return Optional.empty();
        }

        @Override
        public void reset() {
        }
    }

    private static Stream<BulletRecord> makeStream(int count) {
        return IntStream.range(0, count).mapToObj(x -> RecordBox.get().getRecord());
    }

    private static ArrayList<BulletRecord> makeList(int count) {
        return makeStream(count).collect(Collectors.toCollection(ArrayList::new));
    }

    private static Map<String, Object> configWithNoTimestamp() {
        return singletonMap(BulletConfig.RECORD_INJECT_TIMESTAMP, false);
    }

    public static int size(BulletRecord record) {
        int size = 0;
        for (Object ignored : record) {
            size++;
        }
        return size;
    }

    @Test
    public void testDefaults() {
        Query query = new Query();
        BulletConfig config = new BulletConfig();
        config.validate();
        query.configure(config);

        Assert.assertEquals((Object) query.getAggregation().getSize(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
        Assert.assertTrue(query.isAcceptingData());
        Assert.assertEquals(query.getAggregate().getRecords(), emptyList());
    }

    @Test
    public void testFiltering() {
        Query query = new Query();
        query.setFilters(singletonList(FilterClauseTest.getFieldFilter(FilterType.EQUALS, "foo", "bar")));
        query.configure(new BulletConfig());

        Assert.assertTrue(query.filter(RecordBox.get().add("field", "foo").getRecord()));
        Assert.assertTrue(query.filter(RecordBox.get().add("field", "bar").getRecord()));
        Assert.assertFalse(query.filter(RecordBox.get().add("field", "baz").getRecord()));
    }

    @Test
    public void testReceiveTimestampNoProjection() {
        Long start = System.currentTimeMillis();

        Query query = new Query();
        query.setProjection(null);
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RECORD_INJECT_TIMESTAMP, true);
        config.validate();
        query.configure(config);

        BulletRecord input = RecordBox.get().add("field", "foo").add("mid", "123").getRecord();
        BulletRecord actual = query.project(input);

        Long end = System.currentTimeMillis();

        Assert.assertEquals(size(actual), 3);
        Assert.assertEquals(actual.get("field"), "foo");
        Assert.assertEquals(actual.get("mid"), "123");

        Long recordedTimestamp = (Long) actual.get(BulletConfig.DEFAULT_RECORD_INJECT_TIMESTAMP_KEY);
        Assert.assertTrue(recordedTimestamp >= start);
        Assert.assertTrue(recordedTimestamp <= end);
    }

    @Test
    public void testReceiveTimestamp() {
        Long start = System.currentTimeMillis();

        Query query = new Query();
        Projection projection = new Projection();
        projection.setFields(singletonMap("field", "bid"));
        query.setProjection(projection);
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RECORD_INJECT_TIMESTAMP, true);
        config.validate();
        query.configure(config);

        BulletRecord input = RecordBox.get().add("field", "foo").add("mid", "123").getRecord();
        BulletRecord actual = query.project(input);

        Long end = System.currentTimeMillis();

        Assert.assertEquals(size(actual), 2);
        Assert.assertEquals(actual.get("bid"), "foo");

        Long recordedTimestamp = (Long) actual.get(BulletConfig.DEFAULT_RECORD_INJECT_TIMESTAMP_KEY);
        Assert.assertTrue(recordedTimestamp >= start);
        Assert.assertTrue(recordedTimestamp <= end);
    }

    @Test
    public void testMeetingDefaultQuery() {
        Query query = new Query();
        query.configure(new BulletConfig());

        Assert.assertTrue(makeStream(BulletConfig.DEFAULT_AGGREGATION_SIZE - 1).map(query::filter).allMatch(x -> x));
        // Check that we only get the default number out
        makeList(BulletConfig.DEFAULT_AGGREGATION_SIZE + 2).forEach(query::aggregate);
        Assert.assertEquals((Object) query.getAggregate().getRecords().size(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
    }

    @Test
    public void testAggregationExceptions() {
        Aggregation aggregation = mock(Aggregation.class);
        FailingStrategy failure = new FailingStrategy();
        when(aggregation.getStrategy()).thenReturn(failure);

        Query query = new Query();
        query.setAggregation(aggregation);

        query.aggregate(RecordBox.get().getRecord());
        query.aggregate(new byte[0]);

        Assert.assertNull(query.getSerializedAggregate());
        Clip actual = query.getAggregate();

        Assert.assertNotNull(actual.getMeta());
        Assert.assertEquals(actual.getRecords().size(), 0);

        Map<String, Object> actualMeta = actual.getMeta().asMap();

        Assert.assertEquals(actualMeta.size(), 1);
        Assert.assertNotNull(actualMeta.get(Metadata.ERROR_KEY));

        ParsingError expectedError = ParsingError.makeError("Getting aggregation test failure",
                                              Querier.AGGREGATION_FAILURE_RESOLUTION);
        Assert.assertEquals(actualMeta.get(Metadata.ERROR_KEY), singletonList(expectedError));

        Assert.assertEquals(failure.consumptionFailure, 1);
        Assert.assertEquals(failure.combiningFailure, 1);
        Assert.assertEquals(failure.serializingFailure, 1);
        Assert.assertEquals(failure.aggregationFailure, 1);
    }

    @Test
    public void testFilteringProjection() {
        FilterQuery query = getRunningQuery(makeProjectionFilterQuery("map_field.id", Arrays.asList("1", "23"),
                                                                      FilterType.EQUALS, Pair.of("map_field.id", "mid")),
                                                                      configWithNoTimestamp());
        RecordBox boxA = RecordBox.get().addMap("map_field", Pair.of("id", "3"));
        Assert.assertFalse(query.consume(boxA.getRecord()));
        Assert.assertNull(query.getData());

        RecordBox boxB = RecordBox.get().addMap("map_field", Pair.of("id", "23"));
        RecordBox expected = RecordBox.get().add("mid", "23");
        Assert.assertTrue(query.consume(boxB.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(expected.getRecord()));
    }

    @Test
    public void testNoAggregationAttempted() {
        FilterQuery query = getRunningQuery(makeRawFullQuery("map_field.id", Arrays.asList("1", "23"), FilterType.EQUALS,
                                                             AggregationType.RAW, BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE,
                                                             Pair.of("map_field.id", "mid")), configWithNoTimestamp());

        RecordBox boxA = RecordBox.get().addMap("map_field", Pair.of("id", "23"));
        RecordBox expectedA = RecordBox.get().add("mid", "23");
        Assert.assertTrue(query.consume(boxA.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(expectedA.getRecord()));

        RecordBox boxB = RecordBox.get().addMap("map_field", Pair.of("id", "3"));
        Assert.assertFalse(query.consume(boxB.getRecord()));
        Assert.assertNull(query.getData());

        RecordBox boxC = RecordBox.get().addMap("map_field", Pair.of("id", "1"));
        RecordBox expectedC = RecordBox.get().add("mid", "1");
        Assert.assertTrue(query.consume(boxC.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(expectedC.getRecord()));
    }

    @Test
    public void testMaximumEmitted() {
        FilterQuery query = getRunningQuery(makeAggregationQuery(AggregationType.RAW, 2), configWithNoTimestamp());
        RecordBox box = RecordBox.get();
        Assert.assertTrue(query.consume(box.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(box.getRecord()));
        Assert.assertTrue(query.consume(box.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(box.getRecord()));
        for (int i = 0; i < 10; ++i) {
            Assert.assertFalse(query.consume(box.getRecord()));
            Assert.assertNull(query.getData());
        }
    }

    @Test
    public void testMaximumEmittedWithNonMatchingRecords() {
        FilterQuery query = getRunningQuery(makeRawFullQuery("mid", Arrays.asList("1", "23"), FilterType.EQUALS,
                                                             AggregationType.RAW, 2, Pair.of("mid", "mid")),
                                           configWithNoTimestamp());
        RecordBox boxA = RecordBox.get().add("mid", "23");
        RecordBox expectedA = RecordBox.get().add("mid", "23");

        RecordBox boxB = RecordBox.get().add("mid", "42");

        Assert.assertTrue(query.consume(boxA.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(expectedA.getRecord()));

        Assert.assertFalse(query.consume(boxB.getRecord()));
        Assert.assertNull(query.getData());

        Assert.assertFalse(query.consume(boxB.getRecord()));
        Assert.assertNull(query.getData());

        Assert.assertTrue(query.consume(boxA.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(expectedA.getRecord()));

        for (int i = 0; i < 10; ++i) {
            Assert.assertFalse(query.consume(boxA.getRecord()));
            Assert.assertNull(query.getData());
            Assert.assertFalse(query.consume(boxB.getRecord()));
            Assert.assertNull(query.getData());
        }
    }

    @Test(expectedExceptions = BulletException.class)
    public void testValidationFail() throws BulletException {
        new FilterQuery("{ 'aggregation': { 'type': null } }", new BulletConfig());
    }

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
