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
import com.yahoo.bullet.parsing.AggregationUtils;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.ProjectionUtils;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.parsing.WindowUtils;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.windowing.Reactive;
import com.yahoo.bullet.windowing.Scheme;
import com.yahoo.bullet.windowing.Tumbling;
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
import static com.yahoo.bullet.parsing.FilterUtils.getFieldFilter;
import static com.yahoo.bullet.parsing.QueryUtils.makeAggregationQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeProjectionFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeRawFullQuery;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class QuerierTest {
    private static class FailingScheme extends Scheme {
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

    private static Querier make(String id, String query, BulletConfig config) {
        try {
            return new Querier(id, query, config);
        } catch (BulletException e) {
            throw new RuntimeException(e);
        }
    }
    private static Querier make(String id, Query query, BulletConfig config) {
        try {
            return new Querier(new RunningQuery(id, query), config);
        } catch (BulletException e) {
            throw new RuntimeException(e);
        }
    }

    private static Querier make(String query, BulletConfig config) {
        return make("", query, config);
    }

    private static Querier make(Query query, BulletConfig config) {
        return make("", query, config);
    }

    private static Querier make(Query query) {
        BulletConfig config = new BulletConfig();
        query.configure(config);
        return make(query, config);
    }

    private static Querier make(String query, Map<String, Object> configuration) {
        return make("", query, configuration);
    }

    private static Querier make(String id, String query, Map<String, Object> configuration) {
        BulletConfig config = new BulletConfig();
        configuration.forEach(config::set);
        config.validate();
        return make(id, query, config);
    }

    @Test
    public void testDefaults() {
        Querier querier = make(new Query());

        Query query = querier.getRunningQuery().getQuery();
        Assert.assertEquals((Object) query.getAggregation().getSize(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
        Assert.assertFalse(querier.isClosedForPartition());
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertFalse(querier.isExceedingRateLimit());
        Assert.assertNull(querier.getRateLimitError());
        Assert.assertTrue(querier.isTimeBasedWindow());
        Assert.assertEquals(querier.getResult().getRecords(), emptyList());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullConfig() throws BulletException {
        new Querier("", "{}", null);
    }

    @Test(expectedExceptions = JsonParseException.class)
    public void testBadJSON() {
        make("{", new BulletConfig().validate());
    }

    @Test(expectedExceptions = BulletException.class)
    public void testMissingAggregationType() throws BulletException {
        new Querier("", "{ 'aggregation': { 'type': null }}", new BulletConfig().validate());
    }

    @Test(expectedExceptions = BulletException.class)
    public void testBadAggregation() throws BulletException {
        new Querier("", "{'aggregation': {'type': 'COUNT DISTINCT'}}", new BulletConfig().validate());
    }

    @Test
    public void testQueryAsString() {
        Querier querier = make("ahdf3", "{}", emptyMap());
        Assert.assertEquals(querier.toString(), "ahdf3 : {}");

        BulletConfig config = new BulletConfig().validate();
        Query query = new Query();
        query.configure(config);
        query.initialize();
        querier = make("ahdf3", query, config);
        Assert.assertEquals(querier.toString(), "ahdf3 : " + query.toString());
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

        Clip finalResult = querier.finish();
        Assert.assertNotNull(finalResult);
        Assert.assertTrue(finalResult.getRecords().isEmpty());

        meta = finalResult.getMeta().asMap();
        creationTime = ((Number) meta.get(Meta.Concept.QUERY_RECEIVE_TIME.getName())).longValue();
        resultTime = ((Number) meta.get(Meta.Concept.RESULT_EMIT_TIME.getName())).longValue();
        long finishTime = ((Number) meta.get(Meta.Concept.QUERY_FINISH_TIME.getName())).longValue();
        endTime = System.currentTimeMillis();
        Assert.assertTrue(creationTime >= startTime && creationTime <= finishTime);
        Assert.assertTrue(resultTime >= startTime && resultTime <= finishTime);
        Assert.assertTrue(finishTime <= endTime);
    }

    @Test
    public void testReceiveTimestampNoProjection() {
        Long start = System.currentTimeMillis();

        Query query = new Query();
        query.setProjection(null);
        query.setWindow(WindowUtils.makeReactiveWindow());

        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RECORD_INJECT_TIMESTAMP, true);
        config.validate();
        query.configure(config);
        Querier querier = make(query, config);

        BulletRecord input = RecordBox.get().add("field", "foo").add("mid", "123").getRecord();
        querier.consume(input);
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertTrue(querier.isClosed());

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
        query.setProjection(ProjectionUtils.makeProjection("field", "bid"));
        query.setWindow(WindowUtils.makeReactiveWindow());

        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RECORD_INJECT_TIMESTAMP, true);
        config.validate();
        query.configure(config);
        Querier querier = make(query, config);

        BulletRecord input = RecordBox.get().add("field", "foo").add("mid", "123").getRecord();
        querier.consume(input);
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertTrue(querier.isClosed());

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
        Querier querier = make("{}", emptyMap());

        makeStream(BulletConfig.DEFAULT_RAW_AGGREGATION_MAX_SIZE - 1).forEach(querier::consume);
        Assert.assertFalse(querier.isClosed());
        makeStream(1).forEach(querier::consume);
        Assert.assertTrue(querier.isClosed());
        Assert.assertEquals((Object) querier.getRecords().size(), BulletConfig.DEFAULT_RAW_AGGREGATION_MAX_SIZE);

        querier.reset();
        makeStream(BulletConfig.DEFAULT_RAW_AGGREGATION_MAX_SIZE - 1).forEach(querier::consume);
        byte[] dataChunkA = querier.getData();
        querier.reset();
        makeStream(2).forEach(querier::consume);
        byte[] dataChunkB = querier.getData();
        querier.reset();

        querier.combine(dataChunkA);
        Assert.assertFalse(querier.isClosed());
        querier.combine(dataChunkB);
        Assert.assertTrue(querier.isClosed());

        // We miss one record
        Assert.assertEquals((Object) querier.getRecords().size(), BulletConfig.DEFAULT_RAW_AGGREGATION_MAX_SIZE);
    }

    @Test
    public void testFiltering() {
        Query query = new Query();
        query.setFilters(singletonList(getFieldFilter(Clause.Operation.EQUALS, "foo", "bar")));
        query.setWindow(WindowUtils.makeReactiveWindow());
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
                                                         Clause.Operation.EQUALS, Pair.of("map_field.id", "mid")),
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

        Assert.assertNull(querier.getRecords());

        Meta meta = querier.getMetadata();
        Map<String, Object> actualMeta = meta.asMap();
        Assert.assertNotNull(actualMeta.get(Meta.ERROR_KEY));
        BulletError expected = BulletError.makeError("Getting metadata failure", Querier.TRY_AGAIN_LATER);
        Assert.assertEquals(actualMeta.get(Meta.ERROR_KEY), singletonList(expected));

        Clip actual = querier.getResult();
        Assert.assertNotNull(actual.getMeta());
        Assert.assertEquals(actual.getRecords().size(), 0);
        actualMeta = actual.getMeta().asMap();
        Assert.assertEquals(actualMeta.size(), 1);
        Assert.assertNotNull(actualMeta.get(Meta.ERROR_KEY));
        expected = BulletError.makeError("Getting result failure", Querier.TRY_AGAIN_LATER);
        Assert.assertEquals(actualMeta.get(Meta.ERROR_KEY), singletonList(expected));

        Assert.assertEquals(failingScheme.consumptionFailure, 1);
        Assert.assertEquals(failingScheme.combiningFailure, 1);
        Assert.assertEquals(failingScheme.serializingFailure, 1);
        Assert.assertEquals(failingScheme.aggregationFailure, 3);
    }

    @Test
    public void testBasicWindowMaximumEmitted() {
        Querier querier = make(makeAggregationQuery(Aggregation.Type.RAW, 2), configWithNoTimestamp());

        byte[] expected = getListBytes(RecordBox.get().getRecord());
        byte[] expectedTwice = getListBytes(RecordBox.get().getRecord(), RecordBox.get().getRecord());

        querier.consume(RecordBox.get().getRecord());
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isClosedForPartition());
        Assert.assertFalse(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), expected);

        querier.consume(RecordBox.get().getRecord());
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), expectedTwice);

        // Nothing else is consumed because window is closed
        makeStream(10).forEach(querier::consume);
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), expectedTwice);
    }

    @Test
    public void testBasicWindowMaximumEmittedWithNonMatchingRecords() {
        Querier querier = make(makeRawFullQuery("mid", Arrays.asList("1", "23"), Clause.Operation.EQUALS, Aggregation.Type.RAW,
                                                2, Pair.of("mid", "mid")), configWithNoTimestamp());

        byte[] expected = getListBytes(RecordBox.get().add("mid", "23").getRecord());
        byte[] expectedTwice = getListBytes(RecordBox.get().add("mid", "23").getRecord(),
                                            RecordBox.get().add("mid", "23").getRecord());

        querier.consume(RecordBox.get().add("mid", "23").getRecord());
        Assert.assertFalse(querier.isClosedForPartition());
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), expected);

        // Doesn't match
        querier.consume(RecordBox.get().add("mid", "42").getRecord());
        Assert.assertFalse(querier.isClosedForPartition());
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), expected);

        querier.consume(RecordBox.get().add("mid", "23").getRecord());
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), expectedTwice);

        // Nothing else is consumed because window is closed
        IntStream.range(0, 10).mapToObj(i -> RecordBox.get().add("mid", "23").getRecord()).forEach(querier::consume);
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.haveData());
        Assert.assertEquals(querier.getData(), expectedTwice);

        querier.reset();
        Assert.assertFalse(querier.isClosedForPartition());
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertFalse(querier.haveData());
        Assert.assertNull(querier.getData());
    }

    @Test
    public void testAggregateIsNotNull() {
        Querier querier = make("{}", emptyMap());
        Assert.assertNotNull(querier.getResult());
        querier = make("{'aggregation': {}}", emptyMap());
        Assert.assertNotNull(querier.getResult());
        querier = make("{'aggregation': null}", emptyMap());
        Assert.assertNotNull(querier.getResult());
    }

    @Test
    public void testMerging() {
        Querier querierA = make(makeAggregationQuery(Aggregation.Type.RAW, 2), configWithNoTimestamp());
        Querier querierB = make(makeAggregationQuery(Aggregation.Type.RAW, 2), configWithNoTimestamp());

        byte[] expected = getListBytes(RecordBox.get().getRecord());
        byte[] expectedTwice = getListBytes(RecordBox.get().getRecord(), RecordBox.get().getRecord());

        querierA.consume(RecordBox.get().getRecord());
        Assert.assertFalse(querierA.isClosed());
        Assert.assertFalse(querierA.isClosedForPartition());
        Assert.assertFalse(querierA.isDone());
        Assert.assertTrue(querierA.haveData());
        Assert.assertEquals(querierA.getData(), expected);

        querierB.consume(RecordBox.get().getRecord());
        Assert.assertFalse(querierB.isClosed());
        Assert.assertFalse(querierB.isClosedForPartition());
        Assert.assertFalse(querierB.isDone());
        Assert.assertTrue(querierB.haveData());
        Assert.assertEquals(querierB.getData(), expected);

        querierA.merge(querierB);
        Assert.assertTrue(querierA.isClosed());
        Assert.assertTrue(querierA.isClosedForPartition());
        Assert.assertTrue(querierA.isDone());
        Assert.assertTrue(querierA.haveData());
        Assert.assertEquals(querierA.getData(), expectedTwice);
    }

    @Test
    public void testRateLimiting() throws Exception {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RATE_LIMIT_ENABLE, true);
        config.set(BulletConfig.RATE_LIMIT_TIME_INTERVAL, 1);
        config.set(BulletConfig.RATE_LIMIT_MAX_EMIT_COUNT, 1);
        config.validate();

        Querier querier = make("{}", config);
        Assert.assertFalse(querier.isExceedingRateLimit());
        Assert.assertNull(querier.getRateLimitError());


        IntStream.range(0, 1000).forEach(i -> querier.getRecords());

        // To make sure it's time to check again
        Thread.sleep(1);

        Assert.assertTrue(querier.isExceedingRateLimit());
        Assert.assertNotNull(querier.getRateLimitError());
    }

    @Test
    public void testRateLimitDisabled() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RATE_LIMIT_ENABLE, false);
        config.set(BulletConfig.RATE_LIMIT_TIME_INTERVAL, 1);
        config.set(BulletConfig.RATE_LIMIT_MAX_EMIT_COUNT, 1);
        config.validate();

        Querier querier = make("{}", config);
        IntStream.range(0, 1000).forEach(i -> querier.getRecords());
        Assert.assertFalse(querier.isExceedingRateLimit());
        Assert.assertNull(querier.getRateLimitError());
    }

    @Test
    public void testMetadataDisabled() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RESULT_METADATA_ENABLE, false);
        // Should clear out the default metadata
        config.validate();

        Query query = new Query();
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.COUNT_DISTINCT);
        aggregation.setFields(singletonMap("foo", "bar"));
        query.setAggregation(aggregation);
        query.setWindow(WindowUtils.makeWindow(Window.Unit.RECORD, 2));
        query.configure(config);
        Querier querier = make(query, config);

        querier.consume(RecordBox.get().add("foo", "A").getRecord());

        Assert.assertTrue(querier.getMetadata().asMap().isEmpty());
    }

    @Test
    public void testRawQueriesWithNonTimeWindowsAreForcedToReactive() {
        BulletConfig config = new BulletConfig().validate();
        Query query = new Query();
        query.setWindow(WindowUtils.makeWindow(Window.Unit.RECORD, 2));
        query.configure(config);
        Querier querier = make(query, config);

        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isClosedForPartition());
        Assert.assertFalse(querier.isTimeBasedWindow());

        querier.consume(RecordBox.get().getRecord());

        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isClosedForPartition());
        Assert.assertEquals(querier.getWindow().getClass(), Reactive.class);
    }

    @Test
    public void testRawQueriesWithTimeWindowsAreNotChanged() {
        BulletConfig config = new BulletConfig().validate();
        Query query = new Query();
        query.setWindow(WindowUtils.makeWindow(Window.Unit.TIME, Integer.MAX_VALUE));
        query.configure(config);
        Querier querier = make(query, config);

        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isClosedForPartition());
        Assert.assertTrue(querier.isTimeBasedWindow());

        querier.consume(RecordBox.get().getRecord());

        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isClosedForPartition());
        Assert.assertEquals(querier.getWindow().getClass(), Tumbling.class);
    }
}
