/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.FilterOperations.FilterType;
import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryTest {

    private class FailingStrategy implements Strategy {
        public int consumptionFailure = 0;
        public int combiningFailure = 0;
        public int serializingFailure = 0;
        public int aggregationFailure = 0;

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
        public byte[] getSerializedAggregation() {
            serializingFailure++;
            throw new RuntimeException("Serializing aggregation test failure");
        }

        @Override
        public Clip getAggregation() {
            aggregationFailure++;
            throw new RuntimeException("Getting aggregation test failure");
        }

        @Override
        public List<Error> initialize() {
            return null;
        }
    }

    public static Stream<BulletRecord> makeStream(int count) {
        return IntStream.range(0, count).mapToObj(x -> RecordBox.get().getRecord());
    }

    public static ArrayList<BulletRecord> makeList(int count) {
        return makeStream(count).collect(Collectors.toCollection(ArrayList::new));
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

        Assert.assertNull(query.getProjection());
        Assert.assertNull(query.getFilters());
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_DURATION);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertEquals((Object) query.getAggregation().getSize(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
        Assert.assertTrue(query.isAcceptingData());
        Assert.assertEquals(query.getAggregate().getRecords(), emptyList());
    }

    @Test
    public void testExtractField() {
        BulletRecord record = RecordBox.get().add("field", "foo").add("map_field.foo", "bar")
                                             .addMap("map_field", Pair.of("foo", "baz"))
                                             .addList("list_field", singletonMap("foo", "baz"))
                                             .getRecord();

        Assert.assertNull(Query.extractField(null, record));
        Assert.assertNull(Query.extractField("", record));
        Assert.assertNull(Query.extractField("id", record));
        Assert.assertEquals(Query.extractField("map_field.foo", record), "baz");
        Assert.assertNull(Query.extractField("list_field.bar", record));
    }

    @Test
    public void testNumericExtraction() {
        BulletRecord record = RecordBox.get().add("foo", "1.20").add("bar", 42L)
                                             .addMap("map_field", Pair.of("foo", 21.0))
                                             .getRecord();

        Assert.assertNull(Query.extractFieldAsNumber(null, record));
        Assert.assertNull(Query.extractFieldAsNumber("", record));
        Assert.assertNull(Query.extractFieldAsNumber("id", record));
        Assert.assertEquals(Query.extractFieldAsNumber("foo", record), ((Number) 1.20).doubleValue());
        Assert.assertEquals(Query.extractFieldAsNumber("bar", record), ((Number) 42).longValue());
        Assert.assertEquals(Query.extractFieldAsNumber("map_field.foo", record), ((Number) 21).doubleValue());
    }

    @Test
    public void testAggregationForced() {
        Query query = new Query();
        query.setAggregation(null);
        Assert.assertNull(query.getProjection());
        Assert.assertNull(query.getFilters());
        // If you had null for aggregation
        Assert.assertNull(query.getAggregation());
        query.configure(new BulletConfig());

        Assert.assertTrue(query.isAcceptingData());
        Assert.assertEquals(query.getAggregate().getRecords(), emptyList());
    }

    @Test
    public void testDuration() {
        BulletConfig config = new BulletConfig();

        Query query = new Query();
        query.configure(config);
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_DURATION);

        query.setDuration(-1000);
        query.configure(config);
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_DURATION);

        query.setDuration(0);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Integer) 0);

        query.setDuration(1);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Integer) 1);

        query.setDuration(BulletConfig.DEFAULT_SPECIFICATION_DURATION);
        query.configure(config);
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_DURATION);

        query.setDuration(BulletConfig.DEFAULT_SPECIFICATION_MAX_DURATION);
        query.configure(config);
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_MAX_DURATION);

        query.setDuration(BulletConfig.DEFAULT_SPECIFICATION_MAX_DURATION * 2);
        query.configure(config);
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_MAX_DURATION);
    }

    @Test
    public void testCustomDuration() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.SPECIFICATION_DEFAULT_DURATION, 200);
        config.set(BulletConfig.SPECIFICATION_MAX_DURATION, 1000);
        config.validate();

        Query query = new Query();

        query.setDuration(null);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Integer) 200);

        query.setDuration(-1000);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Integer) 200);

        query.setDuration(0);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Integer) 0);

        query.setDuration(1);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Integer) 1);

        query.setDuration(200);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Integer) 200);

        query.setDuration(1000);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Integer) 1000);

        query.setDuration(2000);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Integer) 1000);
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
    public void testAggregationDefault() {
        Query query = new Query();
        Aggregation aggregation = new Aggregation();
        aggregation.setType(null);
        aggregation.setSize(BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE - 1);
        query.setAggregation(aggregation);

        Assert.assertNull(aggregation.getType());
        query.configure(new BulletConfig());

        // Query no longer fixes type
        Assert.assertNull(aggregation.getType());
        Assert.assertEquals(aggregation.getSize(), new Integer(BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE - 1));
    }

    @Test
    public void testMeetingDefaultSpecification() {
        Query query = new Query();
        query.configure(new BulletConfig());

        Assert.assertTrue(makeStream(BulletConfig.DEFAULT_AGGREGATION_SIZE - 1).map(query::filter).allMatch(x -> x));
        // Check that we only get the default number out
        makeList(BulletConfig.DEFAULT_AGGREGATION_SIZE + 2).forEach(query::aggregate);
        Assert.assertEquals((Object) query.getAggregate().getRecords().size(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
    }

    @Test
    public void testValidate() {
        Query query = new Query();
        Aggregation mockAggregation = mock(Aggregation.class);
        Optional<List<Error>> aggregationErrors = Optional.of(asList(Error.of("foo", new ArrayList<>()),
                                                                     Error.of("bar", new ArrayList<>())));
        when(mockAggregation.initialize()).thenReturn(aggregationErrors);
        query.setAggregation(mockAggregation);

        Clause mockClauseA = mock(Clause.class);
        Clause mockClauseB = mock(Clause.class);
        when(mockClauseA.initialize()).thenReturn(Optional.of(singletonList(Error.of("baz", new ArrayList<>()))));
        when(mockClauseB.initialize()).thenReturn(Optional.of(singletonList(Error.of("qux", new ArrayList<>()))));
        query.setFilters(asList(mockClauseA, mockClauseB));

        Projection mockProjection = mock(Projection.class);
        when(mockProjection.initialize()).thenReturn(Optional.of(singletonList(Error.of("quux", new ArrayList<>()))));
        query.setProjection(mockProjection);

        Optional<List<Error>> errorList = query.initialize();
        Assert.assertTrue(errorList.isPresent());
        Assert.assertEquals(errorList.get().size(), 5);
    }

    @Test
    public void testValidateNullValues() {
        Query query = new Query();
        query.setProjection(null);
        query.setFilters(null);
        query.setAggregation(null);
        Optional<List<Error>> errorList = query.initialize();
        Assert.assertFalse(errorList.isPresent());
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

        Error expectedError = Error.makeError("Getting aggregation test failure",
                                              Query.AGGREGATION_FAILURE_RESOLUTION);
        Assert.assertEquals(actualMeta.get(Metadata.ERROR_KEY), singletonList(expectedError));

        Assert.assertEquals(failure.consumptionFailure, 1);
        Assert.assertEquals(failure.combiningFailure, 1);
        Assert.assertEquals(failure.serializingFailure, 1);
        Assert.assertEquals(failure.aggregationFailure, 1);
    }

    @Test
    public void testToString() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        query.configure(config);

        Assert.assertEquals(query.toString(),
                "{filters: null, projection: null, " +
                        "aggregation: {size: 1, type: RAW, fields: null, attributes: null}, duration: 30000}");

        query.setFilters(singletonList(FilterClauseTest.getFieldFilter(FilterType.EQUALS, "foo", "bar")));
        Projection projection = new Projection();
        projection.setFields(singletonMap("field", "bid"));
        query.setProjection(projection);
        query.configure(config);

        Assert.assertEquals(query.toString(),
                            "{filters: [{operation: EQUALS, field: field, values: [foo, bar]}], " +
                            "projection: {fields: {field=bid}}, " +
                            "aggregation: {size: 1, type: RAW, fields: null, attributes: null}, duration: 30000}");
    }
}
