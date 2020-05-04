/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.windowing.Scheme;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

//import static com.yahoo.bullet.parsing.FilterUtils.getFieldFilter;
//import static com.yahoo.bullet.parsing.QueryUtils.makeAggregationQuery;
//import static com.yahoo.bullet.parsing.QueryUtils.makeComputation;
//import static com.yahoo.bullet.parsing.QueryUtils.makeFilter;
//import static com.yahoo.bullet.parsing.QueryUtils.makeOrderBy;
//import static com.yahoo.bullet.parsing.QueryUtils.makeProjectionFilterQuery;
//import static com.yahoo.bullet.parsing.QueryUtils.makeRawFullQuery;
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

    private static ArrayList<BulletRecord> makeList(int count) {
        return makeStream(count).collect(Collectors.toCollection(ArrayList::new));
    }

    private static int size(BulletRecord record) {
        int size = 0;
        for (Object ignored : record) {
            size++;
        }
        return size;
    }
/*
    private static Querier make(Querier.Mode mode, String id, String query, BulletConfig config) {
        Querier querier = new Querier(mode, id, query, config);
        Optional<List<BulletError>> errors = querier.initialize();
        if (errors.isPresent()) {
            throw new RuntimeException(errors.toString());
        }
        return querier;
    }

    private static Querier make(Querier.Mode mode, String id, Query query, BulletConfig config) {
        Querier querier = new Querier(mode, new RunningQuery(id, query), config);
        Optional<List<BulletError>> errors = querier.initialize();
        if (errors.isPresent()) {
            throw new RuntimeException(errors.toString());
        }
        return querier;
    }

    private static Querier make(Querier.Mode mode, Query query, BulletConfig config) {
        return make(mode, "", query, config);
    }

    private static Querier make(Querier.Mode mode, Query query) {
        BulletConfig config = new BulletConfig();
        query.configure(config);
        return make(mode, query, config);
    }

    private static Querier make(Querier.Mode mode, String query) {
        BulletConfig config = new BulletConfig();
        return make(mode, "", query, config);
    }

    private static Querier make(Querier.Mode mode, String id, String query, Map<String, Object> configuration) {
        BulletConfig config = new BulletConfig();
        configuration.forEach(config::set);
        config.validate();
        return make(mode, id, query, config);
    }

    private static Querier make(Querier.Mode mode, String query, Map<String, Object> configuration) {
        return make(mode, "", query, configuration);
    }

    private static RunningQuery makeCountQueryWithAllWindow(BulletConfig config, int emitInterval) {
        Query query = new Query();
        query.setWindow(WindowUtils.makeWindow(Window.Unit.TIME, emitInterval, Window.Unit.ALL, null));

        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.GROUP);
        Map<String, String> count = Collections.singletonMap(GroupOperation.OPERATION_TYPE,
                                                             GroupOperation.GroupOperationType.COUNT.getName());
        aggregation.setAttributes(Collections.singletonMap(GroupOperation.OPERATIONS, Collections.singletonList(count)));
        query.setAggregation(aggregation);
        query.configure(config);

        RunningQuery runningQuery = spy(new RunningQuery("", query));
        doReturn(false).when(runningQuery).isTimedOut();
        return runningQuery;
    }

    @Test
    public void testDefaults() {
        Querier querier = make(Querier.Mode.ALL, new Query());

        RunningQuery runningQuery = querier.getRunningQuery();
        Query query = querier.getQuery();
        Assert.assertSame(runningQuery.getQuery(), query);
        Assert.assertEquals((Object) query.getAggregation().getSize(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.RAW);
        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.isDone());
        Assert.assertFalse(querier.isExceedingRateLimit());
        Assert.assertNull(querier.getRateLimitError());
        // RAW query without window should buffer
        Assert.assertTrue(querier.shouldBuffer());
        Assert.assertEquals(querier.getResult().getRecords(), emptyList());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullConfig() {
        new Querier("", "{}", null);
    }

    @Test(expectedExceptions = JsonParseException.class)
    public void testBadJSON() {
        make(Querier.Mode.ALL, "", "{", new BulletConfig());
    }

    @Test
    public void testMissingAggregationType() {
        Querier querier = new Querier("", "{ 'aggregation': { 'type': null }}", new BulletConfig());
        Optional<List<BulletError>> errors = querier.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().size(), 1);
        Assert.assertEquals(errors.get().get(0), Aggregation.TYPE_NOT_SUPPORTED_ERROR);
    }

    @Test
    public void testBadAggregation() {
        Querier querier = new Querier("", "{'aggregation': {'type': 'COUNT DISTINCT'}}", new BulletConfig());
        Optional<List<BulletError>> errors = querier.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().size(), 1);
        Assert.assertEquals(errors.get().get(0), SketchingStrategy.REQUIRES_FIELD_ERROR);
    }

    @Test
    public void testQueryAsString() {
        Querier querier = make(Querier.Mode.ALL, "ahdf3", "{}", emptyMap());
        Assert.assertEquals(querier.toString(), "ahdf3 : {}");

        BulletConfig config = new BulletConfig();
        Query query = new Query();
        query.configure(config);
        query.initialize();
        querier = make(Querier.Mode.ALL, "ahdf3", query, config);
        Assert.assertEquals(querier.toString(), "ahdf3 : " + query.toString());
    }

    @Test
    public void testAggregateIsNotNull() {
        Querier querier = make(Querier.Mode.ALL, "{}", emptyMap());
        Assert.assertNotNull(querier.getResult());
        querier = make(Querier.Mode.ALL, "{'aggregation': {}}", emptyMap());
        Assert.assertNotNull(querier.getResult());
        querier = make(Querier.Mode.ALL, "{'aggregation': null}", emptyMap());
        Assert.assertNotNull(querier.getResult());
    }

    @Test
    public void testDisabledQueryMeta() {
        BulletConfig defaults = new BulletConfig();
        defaults.set(BulletConfig.RESULT_METADATA_METRICS, emptyMap());

        Querier querier = make(Querier.Mode.ALL, "", "{'aggregation' : {}}", defaults);
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
        Querier querier = make(Querier.Mode.ALL, "", "{'aggregation' : {}}", emptyMap());
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
        Querier querier = make(Querier.Mode.ALL, "", "{}", emptyMap());

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
        //query.setFilters(singletonList(getFieldFilter(Clause.Operation.EQUALS, "foo", "bar")));
        query.setWindow(WindowUtils.makeSlidingWindow(1));
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
        Querier querier = make(Querier.Mode.PARTITION,
                               makeProjectionFilterQuery("map_field.id", Arrays.asList("1", "23"),
                                                         Clause.Operation.EQUALS, Pair.of("map_field.id", "mid")));
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
        Querier querier = make(Querier.Mode.ALL, new Query());
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
        Querier querier = make(Querier.Mode.PARTITION,
                               makeAggregationQuery(Aggregation.Type.RAW, 2));

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
        Querier querier = make(Querier.Mode.PARTITION,
                               makeRawFullQuery("mid", Arrays.asList("1", "23"), Clause.Operation.EQUALS, Aggregation.Type.RAW,
                                                2, Pair.of("mid", "mid")));

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
        Clause clause = getFieldFilter(Clause.Operation.EQUALS, "abc");
        String query = "{'filters' : [" + makeFilter(singletonList(clause), Clause.Operation.NOT) + "]}";
        Querier querier = make(Querier.Mode.PARTITION, query);

        querier.consume(RecordBox.get().add("field", "abc").getRecord());
        Assert.assertFalse(querier.hasNewData());

        querier.consume(RecordBox.get().add("field", "ddd").getRecord());
        Assert.assertTrue(querier.hasNewData());
    }

    @Test
    public void testLogicFilterAnd() {
        Clause clause1 = getFieldFilter(Clause.Operation.EQUALS, "abc");
        Clause clause2 = getFieldFilter("id", Clause.Operation.EQUALS, "1");
        String query = "{'filters' : [" + makeFilter(Arrays.asList(clause1, clause2), Clause.Operation.AND) + "]}";
        Querier querier = make(Querier.Mode.PARTITION, query);

        querier.consume(RecordBox.get().add("field", "abc").add("id", "2").getRecord());
        Assert.assertFalse(querier.hasNewData());

        querier.consume(RecordBox.get().add("field", "abc").add("id", "1").getRecord());
        Assert.assertTrue(querier.hasNewData());
    }

    @Test
    public void testLogicFilterOr() {
        Clause clause1 = getFieldFilter(Clause.Operation.EQUALS, "abc");
        Clause clause2 = getFieldFilter("id", Clause.Operation.EQUALS, "1");
        String query = "{'filters' : [" + makeFilter(Arrays.asList(clause1, clause2), Clause.Operation.OR) + "]}";
        Querier querier = make(Querier.Mode.PARTITION, query);

        querier.consume(RecordBox.get().add("field", "abc").add("id", "2").getRecord());
        Assert.assertTrue(querier.hasNewData());

        querier.consume(RecordBox.get().add("field", "abc").add("id", "1").getRecord());
        Assert.assertTrue(querier.hasNewData());
    }

    @Test(expectedExceptions = JsonParseException.class, expectedExceptionsMessageRegExp = ".*Expected STRING but was BEGIN_OBJECT at.*")
    public void testStringFilterClauseMixWithObjectFilterCaluse() {
        String query = "{'filters' : [{'operation': '==', 'field': 'field', 'values': ['1', {'kind': 'VALUE', 'value': '2'}]}]}";
        make(Querier.Mode.PARTITION, query);
    }

    @Test(expectedExceptions = JsonParseException.class, expectedExceptionsMessageRegExp = ".*Expected BEGIN_OBJECT but was STRING at.*")
    public void testObjectFilterClauseMixWithStringFilterCaluse() {
        String query = "{'filters' : [{'operation': '==', 'field': 'field', 'values': [{'kind': 'VALUE', 'value': '2'}, '1']}]}";
        make(Querier.Mode.PARTITION, query);
    }

    @Test
    public void testMerging() {
        Querier querierA = make(Querier.Mode.PARTITION, makeAggregationQuery(Aggregation.Type.RAW, 2));
        Querier querierB = make(Querier.Mode.PARTITION, makeAggregationQuery(Aggregation.Type.RAW, 2));

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


        Querier querierC = make(Querier.Mode.ALL, makeAggregationQuery(Aggregation.Type.RAW, 2));
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

        Querier querier = make(Querier.Mode.ALL, "", "{}", config);
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

        Querier querier = make(Querier.Mode.ALL, "", "{}", config);
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
        query.setWindow(WindowUtils.makeWindow(Window.Unit.TIME, 1));
        query.configure(config);
        Querier querier = make(Querier.Mode.PARTITION, query, config);

        querier.consume(RecordBox.get().add("foo", "A").getRecord());

        Assert.assertTrue(querier.getMetadata().asMap().isEmpty());
    }

    @Test
    public void testRawQueriesWithAllIncludeWindowsAreErrors() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        query.setWindow(WindowUtils.makeWindow(Window.Unit.TIME, 1, Window.Unit.ALL, null));
        query.configure(config);
        Querier querier = new Querier(new RunningQuery("", query), config);
        Optional<List<BulletError>> errors = querier.initialize();

        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Query.NO_RAW_ALL));
    }

    @Test
    public void testRawQueriesWithTimeWindowsAreNotChanged() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        query.setWindow(WindowUtils.makeWindow(Window.Unit.TIME, Integer.MAX_VALUE));
        query.configure(config);
        Querier querier = make(Querier.Mode.PARTITION, query, config);

        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.shouldBuffer());

        querier.consume(RecordBox.get().getRecord());

        Assert.assertFalse(querier.isClosed());
        Assert.assertEquals(querier.getWindow().getClass(), Tumbling.class);
    }

    @Test
    public void testNonRawQueriesWithRecordWindowsAreErrors() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.COUNT_DISTINCT);
        query.setAggregation(aggregation);
        query.setWindow(WindowUtils.makeWindow(Window.Unit.RECORD, 1));
        query.configure(config);
        Querier querier = new Querier(new RunningQuery("", query), config);
        Optional<List<BulletError>> errors = querier.initialize();

        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Query.ONLY_RAW_RECORD));
    }

    @Test
    public void testRawQueriesWithoutWindowsThatAreClosedAreRecordBased() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RAW_AGGREGATION_MAX_SIZE, 10);
        config.validate();

        Query query = new Query();
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.RAW);
        query.setAggregation(aggregation);
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

        Query query = new Query();
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.RAW);
        query.setAggregation(aggregation);
        query.configure(config);

        RunningQuery runningQuery = spy(new RunningQuery("", query));
        doAnswer(returnsElementsOf(asList(false, true))).when(runningQuery).isTimedOut();

        Querier querier = new Querier(Querier.Mode.ALL, runningQuery, config);
        querier.initialize();

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
        querier.initialize();

        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.shouldBuffer());
        Assert.assertEquals(querier.getWindow().getClass(), AdditiveTumbling.class);

        IntStream.range(0, 10).forEach(i -> querier.consume(RecordBox.get().getRecord()));

        Assert.assertFalse(querier.isClosed());

        List<BulletRecord> result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        BulletRecord record = result.get(0);
        Assert.assertEquals(record.get(GroupOperation.GroupOperationType.COUNT.getName()), 10L);

        IntStream.range(0, 10).forEach(i -> querier.consume(RecordBox.get().getRecord()));
        result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        record = result.get(0);
        Assert.assertEquals(record.get(GroupOperation.GroupOperationType.COUNT.getName()), 20L);

        // This will reset
        querier.reset();

        IntStream.range(0, 5).forEach(i -> querier.consume(RecordBox.get().getRecord()));
        result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        record = result.get(0);
        Assert.assertEquals(record.get(GroupOperation.GroupOperationType.COUNT.getName()), 5L);
    }

    @Test
    public void testAdditiveWindowsDoNotResetInAllMode() {
        BulletConfig config = new BulletConfig();
        RunningQuery runningQuery = makeCountQueryWithAllWindow(config, 2000);

        Querier querier = new Querier(Querier.Mode.ALL, runningQuery, config);
        querier.initialize();

        Assert.assertFalse(querier.isClosed());
        Assert.assertFalse(querier.shouldBuffer());
        Assert.assertEquals(querier.getWindow().getClass(), AdditiveTumbling.class);

        IntStream.range(0, 10).forEach(i -> querier.consume(RecordBox.get().getRecord()));

        Assert.assertFalse(querier.isClosed());

        List<BulletRecord> result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        BulletRecord record = result.get(0);
        Assert.assertEquals(record.get(GroupOperation.GroupOperationType.COUNT.getName()), 10L);

        IntStream.range(0, 10).forEach(i -> querier.consume(RecordBox.get().getRecord()));
        result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        record = result.get(0);
        Assert.assertEquals(record.get(GroupOperation.GroupOperationType.COUNT.getName()), 20L);

        // This will not reset
        querier.reset();

        IntStream.range(0, 5).forEach(i -> querier.consume(RecordBox.get().getRecord()));
        result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 1);
        record = result.get(0);
        Assert.assertEquals(record.get(GroupOperation.GroupOperationType.COUNT.getName()), 25L);
    }

    @Test
    public void testRestarting() throws Exception {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RESULT_METADATA_ENABLE, true);
        config.validate();

        Query query = new Query();
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.RAW);
        query.setAggregation(aggregation);
        query.setWindow(WindowUtils.makeWindow(Window.Unit.TIME, 1));
        query.configure(config);
        RunningQuery runningQuery = new RunningQuery("", query);

        Querier querier = new Querier(Querier.Mode.ALL, runningQuery, config);
        querier.initialize();

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
    public void testPostAggregationWithErrors() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        PostAggregation postAggregation = new OrderBy();
        postAggregation.setType(PostAggregation.Type.ORDER_BY);
        query.setPostAggregations(singletonList(postAggregation));
        query.configure(config);
        Querier querier = new Querier(new RunningQuery("", query), config);
        Optional<List<BulletError>> errors = querier.initialize();

        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(OrderBy.ORDERBY_REQUIRES_FIELDS_ERROR));
    }

    @Test
    public void testOrderBy() {
        String query = makeRawFullQuery("a", Arrays.asList("null"), Clause.Operation.NOT_EQUALS, Aggregation.Type.RAW, 500,
                                        Collections.singletonList(makeOrderBy(new OrderBy.SortItem("a", OrderBy.Direction.DESC))), Pair.of("b", "b"));
        Querier querier = make(Querier.Mode.ALL, query);
        querier.initialize();

        IntStream.range(0, 4).forEach(i -> querier.consume(RecordBox.get().add("a", 10 - i).add("b", i + 10).getRecord()));

        List<BulletRecord> result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 4);
        Assert.assertEquals(result.get(0).get("b"), 10);
        Assert.assertFalse(result.get(0).hasField("a"));
        Assert.assertEquals(result.get(1).get("b"), 11);
        Assert.assertFalse(result.get(1).hasField("a"));
        Assert.assertEquals(result.get(2).get("b"), 12);
        Assert.assertFalse(result.get(2).hasField("a"));
        Assert.assertEquals(result.get(3).get("b"), 13);
        Assert.assertFalse(result.get(3).hasField("a"));
    }

    @Test
    public void testComputation() {
        Expression expression = ExpressionUtils.makeBinaryExpression(Expression.Operation.ADD,
                                                                     ExpressionUtils.makeLeafExpression(new Value(Value.Kind.FIELD, "a", Type.INTEGER)),
                                                                     ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "2", Type.LONG)));
        String query = makeRawFullQuery("a", Arrays.asList("null"), Clause.Operation.NOT_EQUALS, Aggregation.Type.RAW, 500, Collections.singletonList(makeComputation(expression, "newName")), Pair.of("b", "b"));
        Querier querier = make(Querier.Mode.ALL, query);
        querier.initialize();

        IntStream.range(0, 4).forEach(i -> querier.consume(RecordBox.get().add("a", i).add("b", i).getRecord()));

        List<BulletRecord> result = querier.getResult().getRecords();
        Assert.assertEquals(result.size(), 4);
        Assert.assertEquals(result.get(0).get("newName"), 2L);
        Assert.assertFalse(result.get(0).hasField("a"));
        Assert.assertEquals(result.get(0).get("b"), 0);
        Assert.assertEquals(result.get(1).get("newName"), 3L);
        Assert.assertFalse(result.get(1).hasField("a"));
        Assert.assertEquals(result.get(1).get("b"), 1);
        Assert.assertEquals(result.get(2).get("newName"), 4L);
        Assert.assertFalse(result.get(2).hasField("a"));
        Assert.assertEquals(result.get(2).get("b"), 2);
        Assert.assertEquals(result.get(3).get("newName"), 5L);
        Assert.assertFalse(result.get(3).hasField("a"));
        Assert.assertEquals(result.get(3).get("b"), 3);
    }
    */
}
