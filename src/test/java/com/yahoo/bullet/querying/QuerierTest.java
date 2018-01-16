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
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.querying.AggregationOperations.AggregationType;
import com.yahoo.bullet.querying.FilterOperations.FilterType;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.windowing.Scheme;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.yahoo.bullet.TestHelpers.getListBytes;
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
    private class FailingScheme extends Scheme {
        int consumptionFailure = 0;
        int combiningFailure = 0;
        int serializingFailure = 0;
        int aggregationFailure = 0;

        public FailingScheme(Strategy aggregation, Window window, BulletConfig config) {
            super(aggregation, window, config);
        }

        @Override
        public void consume(BulletRecord data) {
            consumptionFailure++;
            throw new RuntimeException("Consuming failure");
        }

        @Override
        public void combine(byte[] data) {
            combiningFailure++;
            throw new RuntimeException("Combining serialized data failure");
        }

        @Override
        public byte[] getData() {
            serializingFailure++;
            throw new RuntimeException("Serializing data failure");
        }

        @Override
        public Clip getResult() {
            aggregationFailure++;
            throw new RuntimeException("Getting result failure");
        }

        @Override
        protected Map<String, Object> getMetadata(Map<String, String> metadataKeys) {
            return null;
        }

        @Override
        public Meta getMetadata() {
            aggregationFailure++;
            throw new RuntimeException("Getting metadata failure");
        }

        @Override
        public List<BulletRecord> getRecords() {
            aggregationFailure++;
            throw new RuntimeException("Getting records failure");
        }

        @Override
        public Optional<List<BulletError>> initialize() {
            return Optional.empty();
        }

        @Override
        public void reset() {
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public boolean isClosedForPartition() {
            return false;
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

    private static int size(BulletRecord record) {
        int size = 0;
        for (Object ignored : record) {
            size++;
        }
        return size;
    }

    private static Querier make(Query query, BulletConfig config) {
        try {
            return new Querier(new RunningQuery("", query), config.validate());
        } catch (BulletException e) {
            throw new RuntimeException(e);
        }
    }

    private static Querier make(Query query) {
        BulletConfig config = new BulletConfig();
        query.configure(config);
        return make(query, config);
    }

    private static Querier make(String query, Map<String, Object> configuration) {
        try {
            BulletConfig config = new BulletConfig();
            configuration.forEach(config::set);
            config.validate();

            return new Querier("", query, config);
        } catch (BulletException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDefaults() {
        Querier querier = make(new Query());

        Query query = querier.getRunningQuery().getQuery();
        Assert.assertEquals((Object) query.getAggregation().getSize(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isExceedingRateLimit());
        Assert.assertFalse(querier.isDone());
        Assert.assertEquals(querier.getResult().getRecords(), emptyList());
    }

    @Test(expectedExceptions = BulletException.class)
    public void testValidationFail() throws BulletException {
        new Querier("", "{ 'aggregation': { 'type': null } }", new BulletConfig());
    }

    @Test(expectedExceptions = JsonParseException.class)
    public void testBadJSON() {
        make("{", emptyMap());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullConfig() throws BulletException {
        new Querier("", "{}", null);
    }

    @Test
    public void testQueryAsString() {
        Querier query = make("{}", emptyMap());
        Assert.assertEquals(query.toString(), "{}", null);
    }

    @Test
    public void testTimes() {
        long startTime = System.currentTimeMillis();
        Querier querier = make("{'aggregation' : {}}", emptyMap());
        Map<String, Object> meta = querier.getMetadata().asMap();
        long creationTime = ((Number) meta.get(Meta.Concept.QUERY_RECEIVE_TIME.getName())).longValue();
        long resultTime = ((Number) meta.get(Meta.Concept.RESULT_EMIT_TIME.getName())).longValue();
        long endTime = System.currentTimeMillis();
        Assert.assertTrue(creationTime >= startTime && creationTime <= endTime);
        Assert.assertTrue(resultTime >= creationTime && resultTime <= endTime);
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
        Querier querier = make(query, config);

        BulletRecord input = RecordBox.get().add("field", "foo").add("mid", "123").getRecord();
        querier.consume(input);
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertFalse(querier.isClosed());

        List<BulletRecord> records = querier.getRecords();
        Assert.assertEquals(records.size(), 1);
        BulletRecord actual = records.get(0);

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
        Querier querier = make(query, config);

        BulletRecord input = RecordBox.get().add("field", "foo").add("mid", "123").getRecord();
        querier.consume(input);
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertFalse(querier.isClosed());

        List<BulletRecord> records = querier.getRecords();
        Assert.assertEquals(records.size(), 1);
        BulletRecord actual = records.get(0);

        Long end = System.currentTimeMillis();

        Assert.assertEquals(size(actual), 2);
        Assert.assertEquals(actual.get("bid"), "foo");

        Long recordedTimestamp = (Long) actual.get(BulletConfig.DEFAULT_RECORD_INJECT_TIMESTAMP_KEY);
        Assert.assertTrue(recordedTimestamp >= start);
        Assert.assertTrue(recordedTimestamp <= end);
    }

    @Test
    public void testMeetingWindowSize() {
        Querier querier = make(new Query());

        makeStream(BulletConfig.DEFAULT_AGGREGATION_SIZE - 1).forEach(querier::consume);
        Assert.assertFalse(querier.isClosed());
        makeStream(1).forEach(querier::consume);
        Assert.assertTrue(querier.isClosed());
        Assert.assertEquals((Object) querier.getRecords().size(), BulletConfig.DEFAULT_AGGREGATION_SIZE);

        querier.reset();
        makeStream(BulletConfig.DEFAULT_AGGREGATION_SIZE - 1).forEach(querier::consume);
        byte[] dataChunkA = querier.getData();
        querier.reset();
        makeStream(2).forEach(querier::consume);
        byte[] dataChunkB = querier.getData();
        querier.reset();

        querier.combine(dataChunkA);
        Assert.assertFalse(querier.isClosed());
        querier.combine(dataChunkB);
        Assert.assertTrue(querier.isClosed());

        // We miss one event
        Assert.assertEquals((Object) querier.getRecords().size(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
    }

    @Test
    public void testFiltering() {
        Query query = new Query();
        query.setFilters(singletonList(FilterClauseTest.getFieldFilter(FilterType.EQUALS, "foo", "bar")));
        Querier querier = make(query);

        querier.consume(RecordBox.get().add("field", "foo").getRecord());
        Assert.assertTrue(querier.isClosedForPartition());
        querier.reset();

        querier.consume(RecordBox.get().add("field", "bar").getRecord());
        Assert.assertTrue(querier.isClosedForPartition());
        querier.reset();

        querier.consume(RecordBox.get().add("field", "baz").getRecord());
        Assert.assertFalse(querier.isClosedForPartition());
    }

    @Test
    public void testFilteringProjection() {
        Querier querier = make(makeProjectionFilterQuery("map_field.id", Arrays.asList("1", "23"),
                                                         FilterType.EQUALS, Pair.of("map_field.id", "mid")),
                                                                      configWithNoTimestamp());
        RecordBox boxA = RecordBox.get().addMap("map_field", Pair.of("id", "3"));
        querier.consume(boxA.getRecord());
        Assert.assertFalse(querier.isClosedForPartition());
        Assert.assertNull(querier.getData());

        RecordBox boxB = RecordBox.get().addMap("map_field", Pair.of("id", "23"));
        RecordBox expected = RecordBox.get().add("mid", "23");
        querier.consume(boxB.getRecord());
        Assert.assertFalse(querier.isClosedForPartition());
        Assert.assertEquals(querier.getData(), getListBytes(expected.getRecord()));
    }

    @Test
    public void testExceptionWrapping() {
        FailingScheme failingScheme = new FailingScheme(null, null, new BulletConfig());
        Querier querier = make(new Query());
        querier.setWindow(failingScheme);

        querier.consume(RecordBox.get().getRecord());
        querier.combine(new byte[0]);

        Assert.assertNull(querier.getData());
        Clip actual = querier.getResult();

        Assert.assertNotNull(actual.getMeta());
        Assert.assertEquals(actual.getRecords().size(), 0);

        Map<String, Object> actualMeta = actual.getMeta().asMap();

        Assert.assertEquals(actualMeta.size(), 1);
        Assert.assertNotNull(actualMeta.get(Meta.ERROR_KEY));

        BulletError expected = BulletError.makeError("Getting window result failure", Querier.AGGREGATION_FAILURE_RESOLUTION);
        Assert.assertEquals(actualMeta.get(Meta.ERROR_KEY), singletonList(expected));

        Assert.assertEquals(failingScheme.consumptionFailure, 1);
        Assert.assertEquals(failingScheme.combiningFailure, 1);
        Assert.assertEquals(failingScheme.serializingFailure, 1);
        Assert.assertEquals(failingScheme.aggregationFailure, 1);
    }

    @Test
    public void testBasicWindowMaximumEmitted() {
        Querier querier = make(makeAggregationQuery(AggregationType.RAW, 2), configWithNoTimestamp());
        RecordBox box = RecordBox.get();

        querier.consume(box.getRecord());
        Assert.assertFalse(querier.isClosed());
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertFalse(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), getListBytes(box.getRecord()));

        querier.consume(box.getRecord());
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), getListBytes(box.getRecord(), box.getRecord()));

        // Nothing else is consumed because window is closed
        makeStream(10).forEach(querier::consume);
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), getListBytes(box.getRecord(), box.getRecord()));
    }

    @Test
    public void testBasicWindowMaximumEmittedWithNonMatchingRecords() {
        Querier querier = make(makeRawFullQuery("mid", Arrays.asList("1", "23"), FilterType.EQUALS, AggregationType.RAW,
                                                2, Pair.of("mid", "mid")), configWithNoTimestamp());
        RecordBox boxA = RecordBox.get().add("mid", "23");

        RecordBox expectedA = RecordBox.get().add("mid", "23");

        RecordBox boxB = RecordBox.get().add("mid", "42");

        querier.consume(boxA.getRecord());
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), getListBytes(expectedA.getRecord()));

        querier.reset();

        querier.consume(boxB.getRecord());
        Assert.assertFalse(querier.isClosedForPartition());
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertFalse(querier.haveData());
        Assert.assertNull(querier.getData());

        querier.consume(boxA.getRecord());
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), getListBytes(boxA.getRecord(), boxA.getRecord()));

        // Nothing else is consumed because window is closed
        makeStream(10).forEach(querier::consume);
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), getListBytes(boxA.getRecord(), boxA.getRecord()));
    }

    @Test
    public void testAggregateIsNotNull() {
        Querier querier = make("{}", emptyMap());
        Assert.assertNotNull(querier.getData());
        querier = make("{'aggregation': {}}", emptyMap());
        Assert.assertNotNull(querier.getData());
        querier = make("{'aggregation': null}", emptyMap());
        Assert.assertNotNull(querier.getData());
    }
}
