/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.common.BulletConfigTest;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.WindowUtils;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.aggregations.CountDistinct;
import com.yahoo.bullet.query.aggregations.GroupAll;
import com.yahoo.bullet.query.aggregations.GroupBy;
import com.yahoo.bullet.query.aggregations.Raw;
import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.ListExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.querying.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.windowing.AdditiveTumbling;
import com.yahoo.bullet.windowing.Basic;
import com.yahoo.bullet.windowing.Scheme;
import com.yahoo.bullet.windowing.Tumbling;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.yahoo.bullet.TestHelpers.getListBytes;
import static org.mockito.Mockito.spy;

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
        public void start() {
        }

        @Override
        public void reset() {
        }

        @Override
        public void resetForPartition() {
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

    private static Querier make(Querier.Mode mode, String id, Query query, BulletConfig config) {
        return new Querier(mode, id, query, config);
    }

    private static Querier make(Querier.Mode mode, Query query, BulletConfig config) {
        return make(mode, "", query, config);
    }

    private static Querier make(Querier.Mode mode, Query query) {
        BulletConfig config = new BulletConfig();
        query.configure(config);
        return make(mode, query, config);
    }

    private static Querier make(Querier.Mode mode, String id, Query query, Map<String, Object> configuration) {
        BulletConfig config = new BulletConfig();
        configuration.forEach(config::set);
        config.validate();
        return make(mode, id, query, config);
    }

    private static Query makeRawQuery() {
        return new Query(new Projection(), null, new Raw(null), null, new Window(), null);
    }

    private static RunningQuery makeCountQueryWithAllWindow(BulletConfig config, int emitInterval) {
        GroupAll groupAll = new GroupAll(Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "COUNT")));
        Window window = WindowUtils.makeWindow(Window.Unit.TIME, emitInterval, Window.Unit.ALL, null);

        Query query = new Query(new Projection(), null, groupAll, null, window, null);
        query.configure(config);

        RunningQuery runningQuery = spy(new RunningQuery("", query));
        Mockito.doReturn(false).when(runningQuery).isTimedOut();

        return runningQuery;
    }

    @Test
    public void testDefaults() {
        Querier querier = make(Querier.Mode.ALL, new Query(new Projection(), null, new Raw(null), null, new Window(), null));

        RunningQuery runningQuery = querier.getRunningQuery();
        Query query = querier.getQuery();
        Assert.assertSame(runningQuery.getQuery(), query);
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) BulletConfig.DEFAULT_AGGREGATION_SIZE);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertFalse(querier.isExceedingRateLimit());
        Assert.assertNull(querier.getRateLimitError());
        // RAW query without window should buffer
        Assert.assertTrue(querier.shouldBuffer());
        Assert.assertEquals(querier.getResult().getRecords(), Collections.emptyList());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullConfig() {
        new Querier("", new Query(new Projection(), null, new Raw(null), null, new Window(), null), null);
    }

    @Test
    public void testQueryAsString() {
        BulletConfig config = new BulletConfig();
        Query query = makeRawQuery();
        query.configure(config);

        Querier querier = make(Querier.Mode.ALL, "foo", query, config);
        Assert.assertEquals(querier.toString(), "foo : " + query.toString());
    }

    @Test
    public void testAggregateIsNotNull() {
        BulletConfig config = new BulletConfig();
        Query query = makeRawQuery();
        query.configure(config);

        Querier querier = make(Querier.Mode.ALL, query, config);
        Assert.assertNotNull(querier.getResult());
    }

    @Test
    public void testDisabledQueryMeta() {
        BulletConfig defaults = new BulletConfig();
        defaults.set(BulletConfig.RESULT_METADATA_METRICS, Collections.emptyMap());

        Query query = makeRawQuery();
        query.configure(defaults);

        Querier querier = make(Querier.Mode.ALL, "", query, defaults);
        Assert.assertTrue(querier.getMetadata().asMap().isEmpty());

        Clip result = querier.finish();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.getRecords().isEmpty());
        Assert.assertTrue(result.getMeta().asMap().isEmpty());
    }

    @Test
    public void testTimes() {
        BulletConfig defaults = new BulletConfig();
        Map<String, String> names = (Map<String, String>) defaults.get(BulletConfig.RESULT_METADATA_METRICS);

        long startTime = System.currentTimeMillis();
        Querier querier = make(Querier.Mode.ALL, makeRawQuery());
        Map<String, Object> meta = querier.getMetadata().asMap();
        Assert.assertEquals(meta.size(), 2);
        Map<String, Object> queryMeta = (Map<String, Object>) meta.get(names.get(Meta.Concept.QUERY_METADATA.getName()));
        Map<String, Object> windowMeta = (Map<String, Object>) meta.get(names.get(Meta.Concept.WINDOW_METADATA.getName()));
        long creationTime = ((Number) queryMeta.get(names.get(Meta.Concept.QUERY_RECEIVE_TIME.getName()))).longValue();
        long emitTime = ((Number) windowMeta.get(names.get(Meta.Concept.WINDOW_EMIT_TIME.getName()))).longValue();
        long endTime = System.currentTimeMillis();
        Assert.assertTrue(creationTime >= startTime && creationTime <= endTime);
        Assert.assertTrue(emitTime >= startTime && emitTime <= endTime);

        Clip finalResult = querier.finish();
        Assert.assertNotNull(finalResult);
        Assert.assertTrue(finalResult.getRecords().isEmpty());

        meta = finalResult.getMeta().asMap();
        queryMeta = (Map<String, Object>) meta.get(names.get(Meta.Concept.QUERY_METADATA.getName()));
        windowMeta = (Map<String, Object>) meta.get(names.get(Meta.Concept.WINDOW_METADATA.getName()));
        creationTime = ((Number) queryMeta.get(names.get(Meta.Concept.QUERY_RECEIVE_TIME.getName()))).longValue();
        emitTime = ((Number) windowMeta.get(names.get(Meta.Concept.WINDOW_EMIT_TIME.getName()))).longValue();
        long finishTime = ((Number) queryMeta.get(names.get(Meta.Concept.QUERY_FINISH_TIME.getName()))).longValue();
        endTime = System.currentTimeMillis();
        Assert.assertTrue(creationTime >= startTime && creationTime <= finishTime);
        Assert.assertTrue(emitTime >= startTime && emitTime <= finishTime);
        Assert.assertTrue(finishTime <= endTime);
    }

    @Test
    public void testMeetingWindowSize() {
        Querier querier = make(Querier.Mode.ALL, makeRawQuery());

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
        Expression filter = new BinaryExpression(new FieldExpression("field"),
                                                 new ListExpression(Arrays.asList(new ValueExpression("foo"),
                                                                                  new ValueExpression("bar"))),
                                                 Operation.EQUALS_ANY);
        Window window = WindowUtils.makeSlidingWindow(1);
        Query query = new Query(new Projection(), filter, new Raw(null), null, window, null);

        Querier querier = make(Querier.Mode.PARTITION, query);

        querier.consume(RecordBox.get().add("field", "foo").getRecord());
        Assert.assertTrue(querier.isClosed());
        querier.reset();

        querier.consume(RecordBox.get().add("field", "bar").getRecord());
        Assert.assertTrue(querier.isClosed());
        querier.reset();

        querier.consume(RecordBox.get().add("field", "baz").getRecord());
        Assert.assertFalse(querier.isClosed());
    }

    @Test
    public void testFilteringProjection() {
        Projection projection = new Projection(Collections.singletonList(new Field("mid", new FieldExpression("map_field", "id"))), false);
        Expression filter = new BinaryExpression(new FieldExpression("map_field", "id"),
                                                 new ListExpression(Arrays.asList(new ValueExpression("1"),
                                                                                  new ValueExpression("23"))),
                                                 Operation.EQUALS_ANY);
        Query query = new Query(projection, filter, new Raw(null), null, new Window(), null);

        Querier querier = make(Querier.Mode.PARTITION, query);
        RecordBox boxA = RecordBox.get().addMap("map_field", Pair.of("id", "3"));
        querier.consume(boxA.getRecord());
        Assert.assertFalse(querier.isClosed());
        Assert.assertNull(querier.getData());

        RecordBox boxB = RecordBox.get().addMap("map_field", Pair.of("id", "23"));
        RecordBox expected = RecordBox.get().add("mid", "23");
        querier.consume(boxB.getRecord());
        Assert.assertFalse(querier.isClosed());
        Assert.assertEquals(querier.getData(), getListBytes(expected.getRecord()));
    }

    @Test
    public void testExceptionWrapping() {
        FailingScheme failingScheme = new FailingScheme(null, null, new BulletConfig());
        Querier querier = make(Querier.Mode.ALL, makeRawQuery());
        querier.setWindow(failingScheme);

        querier.consume(RecordBox.get().getRecord());

        querier.combine(new byte[0]);

        Assert.assertNull(querier.getData());

        Assert.assertNull(querier.getRecords());

        Meta meta = querier.getMetadata();
        Map<String, Object> actualMeta = meta.asMap();
        Assert.assertNotNull(actualMeta.get(Meta.ERROR_KEY));
        BulletError expected = BulletError.makeError("Getting metadata failure", Querier.TRY_AGAIN_LATER);
        Assert.assertEquals(actualMeta.get(Meta.ERROR_KEY).toString(), Collections.singletonList(expected).toString());

        Clip actual = querier.getResult();
        Assert.assertNotNull(actual.getMeta());
        Assert.assertEquals(actual.getRecords().size(), 0);
        actualMeta = actual.getMeta().asMap();
        Assert.assertEquals(actualMeta.size(), 1);
        Assert.assertNotNull(actualMeta.get(Meta.ERROR_KEY));
        expected = BulletError.makeError("Getting result failure", Querier.TRY_AGAIN_LATER);
        Assert.assertEquals(actualMeta.get(Meta.ERROR_KEY).toString(), Collections.singletonList(expected).toString());

        Assert.assertEquals(failingScheme.consumptionFailure, 1);
        Assert.assertEquals(failingScheme.combiningFailure, 1);
        Assert.assertEquals(failingScheme.serializingFailure, 1);
        Assert.assertEquals(failingScheme.aggregationFailure, 3);
    }

    @Test
    public void testBasicWindowMaximumEmitted() {
        Query query = new Query(new Projection(), null, new Raw(2), null, new Window(), null);

        Querier querier = make(Querier.Mode.PARTITION, query);

        byte[] expected = getListBytes(RecordBox.get().getRecord());
        byte[] expectedTwice = getListBytes(RecordBox.get().getRecord(), RecordBox.get().getRecord());

        querier.consume(RecordBox.get().getRecord());
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertTrue(querier.hasNewData());
        Assert.assertEquals(querier.getData(), expected);

        querier.consume(RecordBox.get().getRecord());
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.hasNewData());
        Assert.assertEquals(querier.getData(), expectedTwice);

        // Nothing else is consumed because RAW is closed
        makeStream(10).forEach(querier::consume);
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.hasNewData());
        Assert.assertEquals(querier.getData(), expectedTwice);
    }

    @Test
    public void testBasicWindowMaximumEmittedWithNonMatchingRecords() {
        Expression filter = new BinaryExpression(new FieldExpression("mid"),
                                                 new ListExpression(Arrays.asList(new ValueExpression("1"),
                                                                                  new ValueExpression("23"))),
                                                 Operation.EQUALS_ANY);
        Query query = new Query(new Projection(), filter, new Raw(2), null, new Window(), null);

        Querier querier = make(Querier.Mode.PARTITION, query);

        byte[] expected = getListBytes(RecordBox.get().add("mid", "23").getRecord());
        byte[] expectedTwice = getListBytes(RecordBox.get().add("mid", "23").getRecord(),
                                            RecordBox.get().add("mid", "23").getRecord());

        querier.consume(RecordBox.get().add("mid", "23").getRecord());
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertTrue(querier.hasNewData());
        Assert.assertEquals(querier.getData(), expected);

        // Doesn't match
        querier.consume(RecordBox.get().add("mid", "42").getRecord());
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertTrue(querier.hasNewData());
        Assert.assertEquals(querier.getData(), expected);

        querier.consume(RecordBox.get().add("mid", "23").getRecord());
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.hasNewData());
        Assert.assertEquals(querier.getData(), expectedTwice);

        // Nothing else is consumed because RAW is closed
        IntStream.range(0, 10).mapToObj(i -> RecordBox.get().add("mid", "23").getRecord()).forEach(querier::consume);
        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.isDone());
        Assert.assertTrue(querier.hasNewData());
        Assert.assertEquals(querier.getData(), expectedTwice);

        querier.reset();
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertFalse(querier.hasNewData());
        Assert.assertNull(querier.getData());
    }

    @Test
    public void testLogicFilterNot() {
        // legacy test
        Expression filter = new BinaryExpression(new FieldExpression("field"), new ValueExpression("abc"), Operation.NOT_EQUALS);
        Query query = new Query(new Projection(), filter, new Raw(null), null, new Window(), null);

        Querier querier = make(Querier.Mode.PARTITION, query);

        querier.consume(RecordBox.get().add("field", "abc").getRecord());
        Assert.assertFalse(querier.hasNewData());

        querier.consume(RecordBox.get().add("field", "ddd").getRecord());
        Assert.assertTrue(querier.hasNewData());
    }

    @Test
    public void testLogicFilterAnd() {
        // legacy test
        Expression filter = new BinaryExpression(new BinaryExpression(new FieldExpression("field"), new ValueExpression("abc"), Operation.EQUALS),
                                                 new BinaryExpression(new FieldExpression("id"), new ValueExpression("1"), Operation.EQUALS),
                                                 Operation.AND);
        Query query = new Query(new Projection(), filter, new Raw(null), null, new Window(), null);

        Querier querier = make(Querier.Mode.PARTITION, query);

        querier.consume(RecordBox.get().add("field", "abc").add("id", "2").getRecord());
        Assert.assertFalse(querier.hasNewData());

        querier.consume(RecordBox.get().add("field", "abc").add("id", "1").getRecord());
        Assert.assertTrue(querier.hasNewData());
    }

    @Test
    public void testLogicFilterOr() {
        // legacy test
        Expression filter = new BinaryExpression(new BinaryExpression(new FieldExpression("field"), new ValueExpression("abc"), Operation.EQUALS),
                                                 new BinaryExpression(new FieldExpression("id"), new ValueExpression("1"), Operation.EQUALS),
                                                 Operation.OR);
        Query query = new Query(new Projection(), filter, new Raw(null), null, new Window(), null);

        Querier querier = make(Querier.Mode.PARTITION, query);

        querier.consume(RecordBox.get().add("field", "abc").add("id", "2").getRecord());
        Assert.assertTrue(querier.hasNewData());

        querier.consume(RecordBox.get().add("field", "abc").add("id", "1").getRecord());
        Assert.assertTrue(querier.hasNewData());
    }

    @Test
    public void testMerging() {
        Query query = new Query(new Projection(), null, new Raw(2), null, new Window(), null);

        Querier querierA = make(Querier.Mode.PARTITION, query);
        Querier querierB = make(Querier.Mode.PARTITION, query);

        byte[] expected = getListBytes(RecordBox.get().getRecord());
        byte[] expectedTwice = getListBytes(RecordBox.get().getRecord(), RecordBox.get().getRecord());

        querierA.consume(RecordBox.get().getRecord());
        Assert.assertFalse(querierA.isClosed());
        Assert.assertFalse(querierA.isDone());
        Assert.assertTrue(querierA.hasNewData());
        Assert.assertEquals(querierA.getData(), expected);

        querierB.consume(RecordBox.get().getRecord());
        Assert.assertFalse(querierB.isClosed());
        Assert.assertFalse(querierB.isDone());
        Assert.assertTrue(querierB.hasNewData());
        Assert.assertEquals(querierB.getData(), expected);


        Querier querierC = make(Querier.Mode.ALL, query);
        querierC.merge(querierA);
        querierC.merge(querierB);
        Assert.assertTrue(querierC.isClosed());
        Assert.assertTrue(querierC.isDone());
        Assert.assertTrue(querierC.hasNewData());
        Assert.assertEquals(querierC.getData(), expectedTwice);
    }

    @Test
    public void testRateLimiting() throws Exception {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RATE_LIMIT_ENABLE, true);
        config.set(BulletConfig.RATE_LIMIT_TIME_INTERVAL, 1);
        config.set(BulletConfig.RATE_LIMIT_MAX_EMIT_COUNT, 1);
        config.validate();

        Query query = makeRawQuery();
        query.configure(config);

        Querier querier = make(Querier.Mode.ALL, "", query, config);
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

        Query query = makeRawQuery();
        query.configure(config);

        Querier querier = make(Querier.Mode.ALL, "", query, config);
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

        CountDistinct aggregation = new CountDistinct(Collections.singletonList("foo"), "count");
        Window window = WindowUtils.makeTumblingWindow(1);
        Query query = new Query(new Projection(), null, aggregation, null, window, null);
        query.configure(config);

        Querier querier = make(Querier.Mode.PARTITION, query, config);

        querier.consume(RecordBox.get().add("foo", "A").getRecord());

        Assert.assertTrue(querier.getMetadata().asMap().isEmpty());
    }

    @Test
    public void testRawQueriesWithTimeWindowsAreNotChanged() {
        BulletConfig config = new BulletConfig();
        Window window = WindowUtils.makeTumblingWindow(Integer.MAX_VALUE);
        Query query = new Query(new Projection(), null, new Raw(null), null, window, null);
        query.configure(config);

        Querier querier = make(Querier.Mode.PARTITION, query, config);

        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.shouldBuffer());

        querier.consume(RecordBox.get().getRecord());

        Assert.assertFalse(querier.isClosed());
        Assert.assertEquals(querier.getWindow().getClass(), Tumbling.class);
    }

    @Test
    public void testRawQueriesWithoutWindowsThatAreClosedAreRecordBased() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RAW_AGGREGATION_MAX_SIZE, 10);
        config.validate();

        Query query = makeRawQuery();
        query.configure(config);

        Querier querier = make(Querier.Mode.ALL, query, config);

        Assert.assertFalse(querier.isClosed());
        Assert.assertTrue(querier.shouldBuffer());
        Assert.assertEquals(querier.getWindow().getClass(), Basic.class);

        querier.consume(RecordBox.get().getRecord());

        Assert.assertFalse(querier.isClosed());
        Assert.assertTrue(querier.shouldBuffer());

        IntStream.range(0, 9).forEach(i -> querier.consume(RecordBox.get().getRecord()));

        Assert.assertTrue(querier.isClosed());
        Assert.assertTrue(querier.shouldBuffer());
    }

    @Test
    public void testRawQueriesWithoutWindowsThatAreTimedOutAreStillRecordBased() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RAW_AGGREGATION_MAX_SIZE, 10);
        config.validate();

        Query query = makeRawQuery();
        query.configure(config);

        RunningQuery runningQuery = spy(new RunningQuery("", query));
        Mockito.doAnswer(AdditionalAnswers.returnsElementsOf(Arrays.asList(false, true))).when(runningQuery).isTimedOut();

        Querier querier = new Querier(Querier.Mode.ALL, runningQuery, config);

        Assert.assertFalse(querier.isClosed());
        Assert.assertTrue(querier.shouldBuffer());
        Assert.assertEquals(querier.getWindow().getClass(), Basic.class);

        IntStream.range(0, 9).forEach(i -> querier.consume(RecordBox.get().getRecord()));

        Assert.assertFalse(querier.isClosed());
        // Now runningQuery is timed out but query is not done.
        Assert.assertTrue(querier.shouldBuffer());
    }

    @Test
    public void testAdditiveWindowsResetInPartitionMode() {
        BulletConfig config = new BulletConfig();
        RunningQuery runningQuery = makeCountQueryWithAllWindow(config, 2000);

        Querier querier = new Querier(Querier.Mode.PARTITION, runningQuery, config);

        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.shouldBuffer());
        Assert.assertEquals(querier.getWindow().getClass(), AdditiveTumbling.class);

        IntStream.range(0, 10).forEach(i -> querier.consume(RecordBox.get().getRecord()));

        Assert.assertFalse(querier.isClosed());

        List<BulletRecord> result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        BulletRecord record = result.get(0);
        Assert.assertEquals(record.typedGet(GroupOperation.GroupOperationType.COUNT.getName()).getValue(), 10L);

        IntStream.range(0, 10).forEach(i -> querier.consume(RecordBox.get().getRecord()));
        result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        record = result.get(0);
        Assert.assertEquals(record.typedGet(GroupOperation.GroupOperationType.COUNT.getName()).getValue(), 20L);

        // This will reset
        querier.reset();

        IntStream.range(0, 5).forEach(i -> querier.consume(RecordBox.get().getRecord()));
        result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        record = result.get(0);
        Assert.assertEquals(record.typedGet(GroupOperation.GroupOperationType.COUNT.getName()).getValue(), 5L);
    }

    @Test
    public void testAdditiveWindowsDoNotResetInAllMode() {
        BulletConfig config = new BulletConfig();
        RunningQuery runningQuery = makeCountQueryWithAllWindow(config, 2000);

        Querier querier = new Querier(Querier.Mode.ALL, runningQuery, config);

        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.shouldBuffer());
        Assert.assertEquals(querier.getWindow().getClass(), AdditiveTumbling.class);

        IntStream.range(0, 10).forEach(i -> querier.consume(RecordBox.get().getRecord()));

        Assert.assertFalse(querier.isClosed());

        List<BulletRecord> result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        BulletRecord record = result.get(0);
        Assert.assertEquals(record.typedGet(GroupOperation.GroupOperationType.COUNT.getName()).getValue(), 10L);

        IntStream.range(0, 10).forEach(i -> querier.consume(RecordBox.get().getRecord()));
        result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        record = result.get(0);
        Assert.assertEquals(record.typedGet(GroupOperation.GroupOperationType.COUNT.getName()).getValue(), 20L);

        // This will not reset
        querier.reset();

        IntStream.range(0, 5).forEach(i -> querier.consume(RecordBox.get().getRecord()));
        result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        record = result.get(0);
        Assert.assertEquals(record.typedGet(GroupOperation.GroupOperationType.COUNT.getName()).getValue(), 25L);
    }

    @Test
    public void testRestarting() throws Exception {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RESULT_METADATA_ENABLE, true);
        config.validate();

        Query query = new Query(new Projection(), null, new Raw(null), null, WindowUtils.makeTumblingWindow(1), null);
        query.configure(config);
        RunningQuery runningQuery = new RunningQuery("", query);

        Querier querier = new Querier(Querier.Mode.ALL, runningQuery, config);

        querier.consume(RecordBox.get().getRecord());
        Assert.assertEquals(querier.getRecords().size(), 1);

        long timeNow = System.currentTimeMillis();
        Meta meta = querier.getMetadata();
        Map<String, String> mapping = BulletConfigTest.allMetadataAsMap();
        Map<String, Object> queryMeta = (Map<String, Object>) meta.asMap().get(mapping.get(Meta.Concept.QUERY_METADATA.getName()));
        Map<String, Object> windowMeta = (Map<String, Object>) meta.asMap().get(mapping.get(Meta.Concept.WINDOW_METADATA.getName()));
        Assert.assertEquals(windowMeta.get(mapping.get(Meta.Concept.WINDOW_NUMBER.getName())), 1L);
        long startTime = (Long) queryMeta.get(mapping.get(Meta.Concept.QUERY_RECEIVE_TIME.getName()));
        long windowEmitTime = (Long) windowMeta.get(mapping.get(Meta.Concept.WINDOW_EMIT_TIME.getName()));
        Assert.assertTrue(startTime <= timeNow);
        Assert.assertTrue(windowEmitTime >= timeNow);

        Thread.sleep(1);

        querier.restart();
        Assert.assertEquals(querier.getRecords().size(), 1);
        meta = querier.getMetadata();
        queryMeta = (Map<String, Object>) meta.asMap().get(mapping.get(Meta.Concept.QUERY_METADATA.getName()));
        Assert.assertEquals(windowMeta.get(mapping.get(Meta.Concept.WINDOW_NUMBER.getName())), 1L);
        long newStartTime = (Long) queryMeta.get(mapping.get(Meta.Concept.QUERY_RECEIVE_TIME.getName()));
        Assert.assertTrue(newStartTime > startTime);

        querier.reset();
        meta = querier.getMetadata();
        windowMeta = (Map<String, Object>) meta.asMap().get(mapping.get(Meta.Concept.WINDOW_METADATA.getName()));
        long newEmitTime = (Long) windowMeta.get(mapping.get(Meta.Concept.WINDOW_EMIT_TIME.getName()));
        Assert.assertEquals(windowMeta.get(mapping.get(Meta.Concept.WINDOW_NUMBER.getName())), 2L);
        Assert.assertTrue(newEmitTime >  windowEmitTime);
    }

    @Test
    public void testOrderBy() {
        Expression filter = new UnaryExpression(new FieldExpression("a"), Operation.IS_NOT_NULL);
        OrderBy orderBy = new OrderBy(Collections.singletonList(new OrderBy.SortItem("a", OrderBy.Direction.DESC)));
        Query query = new Query(new Projection(), filter, new Raw(500), Collections.singletonList(orderBy), new Window(), null);

        Querier querier = make(Querier.Mode.ALL, query);

        IntStream.range(0, 4).forEach(i -> querier.consume(RecordBox.get().add("a", 10 - i).add("b", i + 10).getRecord()));

        List<BulletRecord> result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 4);
        Assert.assertEquals(result.get(0).typedGet("a").getValue(), 10);
        Assert.assertEquals(result.get(0).typedGet("b").getValue(), 10);
        Assert.assertEquals(result.get(1).typedGet("a").getValue(), 9);
        Assert.assertEquals(result.get(1).typedGet("b").getValue(), 11);
        Assert.assertEquals(result.get(2).typedGet("a").getValue(), 8);
        Assert.assertEquals(result.get(2).typedGet("b").getValue(), 12);
        Assert.assertEquals(result.get(3).typedGet("a").getValue(), 7);
        Assert.assertEquals(result.get(3).typedGet("b").getValue(), 13);
    }

    @Test
    public void testComputationAndCulling() {
        Projection projection = new Projection(Arrays.asList(new Field("a", new FieldExpression("a")),
                                                             new Field("b", new FieldExpression("b"))), false);
        Expression filter = new UnaryExpression(new FieldExpression("a"), Operation.IS_NOT_NULL);
        Expression expression = new BinaryExpression(new FieldExpression("a"), new ValueExpression(2L), Operation.ADD);
        Computation computation = new Computation(Collections.singletonList(new Field("newName", expression)));
        Culling culling = new Culling(Collections.singleton("a"));
        Query query = new Query(projection, filter, new Raw(500), Arrays.asList(computation, culling), new Window(), null);

        Querier querier = make(Querier.Mode.ALL, query);

        IntStream.range(0, 4).forEach(i -> querier.consume(RecordBox.get().add("a", i).add("b", i).getRecord()));

        List<BulletRecord> result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 4);
        Assert.assertEquals(result.get(0).typedGet("newName").getValue(), 2L);
        Assert.assertFalse(result.get(0).hasField("a"));
        Assert.assertEquals(result.get(0).typedGet("b").getValue(), 0);
        Assert.assertEquals(result.get(1).typedGet("newName").getValue(), 3L);
        Assert.assertFalse(result.get(1).hasField("a"));
        Assert.assertEquals(result.get(1).typedGet("b").getValue(), 1);
        Assert.assertEquals(result.get(2).typedGet("newName").getValue(), 4L);
        Assert.assertFalse(result.get(2).hasField("a"));
        Assert.assertEquals(result.get(2).typedGet("b").getValue(), 2);
        Assert.assertEquals(result.get(3).typedGet("newName").getValue(), 5L);
        Assert.assertFalse(result.get(3).hasField("a"));
        Assert.assertEquals(result.get(3).typedGet("b").getValue(), 3);
    }
}
