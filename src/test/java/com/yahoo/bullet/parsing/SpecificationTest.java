/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.FilterOperations.FilterType;
import com.yahoo.bullet.operations.aggregations.Strategy;
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

public class SpecificationTest {

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
        Specification specification = new Specification();
        BulletConfig config = new BulletConfig();
        config.validate();
        specification.configure(config);

        Assert.assertNull(specification.getProjection());
        Assert.assertNull(specification.getFilters());
        Assert.assertEquals((Object) specification.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_DURATION);
        Assert.assertEquals(specification.getAggregation().getType(), AggregationType.RAW);
        Assert.assertEquals((Object) specification.getAggregation().getSize(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
        Assert.assertTrue(specification.isAcceptingData());
        Assert.assertEquals(specification.getAggregate().getRecords(), emptyList());
    }

    @Test
    public void testExtractField() {
        BulletRecord record = RecordBox.get().add("field", "foo").add("map_field.foo", "bar")
                                             .addMap("map_field", Pair.of("foo", "baz"))
                                             .addList("list_field", singletonMap("foo", "baz"))
                                             .getRecord();

        Assert.assertNull(Specification.extractField(null, record));
        Assert.assertNull(Specification.extractField("", record));
        Assert.assertNull(Specification.extractField("id", record));
        Assert.assertEquals(Specification.extractField("map_field.foo", record), "baz");
        Assert.assertNull(Specification.extractField("list_field.bar", record));
    }

    @Test
    public void testNumericExtraction() {
        BulletRecord record = RecordBox.get().add("foo", "1.20").add("bar", 42L)
                                             .addMap("map_field", Pair.of("foo", 21.0))
                                             .getRecord();

        Assert.assertNull(Specification.extractFieldAsNumber(null, record));
        Assert.assertNull(Specification.extractFieldAsNumber("", record));
        Assert.assertNull(Specification.extractFieldAsNumber("id", record));
        Assert.assertEquals(Specification.extractFieldAsNumber("foo", record), ((Number) 1.20).doubleValue());
        Assert.assertEquals(Specification.extractFieldAsNumber("bar", record), ((Number) 42).longValue());
        Assert.assertEquals(Specification.extractFieldAsNumber("map_field.foo", record), ((Number) 21).doubleValue());
    }

    @Test
    public void testAggregationForced() {
        Specification specification = new Specification();
        specification.setAggregation(null);
        Assert.assertNull(specification.getProjection());
        Assert.assertNull(specification.getFilters());
        // If you had null for aggregation
        Assert.assertNull(specification.getAggregation());
        specification.configure(new BulletConfig());

        Assert.assertTrue(specification.isAcceptingData());
        Assert.assertEquals(specification.getAggregate().getRecords(), emptyList());
    }

    @Test
    public void testDuration() {
        BulletConfig config = new BulletConfig();

        Specification specification = new Specification();
        specification.configure(config);
        Assert.assertEquals((Object) specification.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_DURATION);

        specification.setDuration(-1000);
        specification.configure(config);
        Assert.assertEquals((Object) specification.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_DURATION);

        specification.setDuration(0);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 0);

        specification.setDuration(1);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 1);

        specification.setDuration(BulletConfig.DEFAULT_SPECIFICATION_DURATION);
        specification.configure(config);
        Assert.assertEquals((Object) specification.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_DURATION);

        specification.setDuration(BulletConfig.DEFAULT_SPECIFICATION_MAX_DURATION);
        specification.configure(config);
        Assert.assertEquals((Object) specification.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_MAX_DURATION);

        specification.setDuration(BulletConfig.DEFAULT_SPECIFICATION_MAX_DURATION * 2);
        specification.configure(config);
        Assert.assertEquals((Object) specification.getDuration(), BulletConfig.DEFAULT_SPECIFICATION_MAX_DURATION);
    }

    @Test
    public void testCustomDuration() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.SPECIFICATION_DEFAULT_DURATION, 200);
        config.set(BulletConfig.SPECIFICATION_MAX_DURATION, 1000);
        config.validate();

        Specification specification = new Specification();

        specification.setDuration(null);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 200);

        specification.setDuration(-1000);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 200);

        specification.setDuration(0);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 0);

        specification.setDuration(1);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 1);

        specification.setDuration(200);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 200);

        specification.setDuration(1000);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 1000);

        specification.setDuration(2000);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 1000);
    }

    @Test
    public void testFiltering() {
        Specification specification = new Specification();
        specification.setFilters(singletonList(FilterClauseTest.getFieldFilter(FilterType.EQUALS, "foo", "bar")));
        specification.configure(new BulletConfig());

        Assert.assertTrue(specification.filter(RecordBox.get().add("field", "foo").getRecord()));
        Assert.assertTrue(specification.filter(RecordBox.get().add("field", "bar").getRecord()));
        Assert.assertFalse(specification.filter(RecordBox.get().add("field", "baz").getRecord()));
    }

    @Test
    public void testReceiveTimestampNoProjection() {
        Long start = System.currentTimeMillis();

        Specification specification = new Specification();
        specification.setProjection(null);
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RECORD_INJECT_TIMESTAMP, true);
        config.validate();
        specification.configure(config);

        BulletRecord input = RecordBox.get().add("field", "foo").add("mid", "123").getRecord();
        BulletRecord actual = specification.project(input);

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

        Specification specification = new Specification();
        Projection projection = new Projection();
        projection.setFields(singletonMap("field", "bid"));
        specification.setProjection(projection);
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RECORD_INJECT_TIMESTAMP, true);
        config.validate();
        specification.configure(config);

        BulletRecord input = RecordBox.get().add("field", "foo").add("mid", "123").getRecord();
        BulletRecord actual = specification.project(input);

        Long end = System.currentTimeMillis();

        Assert.assertEquals(size(actual), 2);
        Assert.assertEquals(actual.get("bid"), "foo");

        Long recordedTimestamp = (Long) actual.get(BulletConfig.DEFAULT_RECORD_INJECT_TIMESTAMP_KEY);
        Assert.assertTrue(recordedTimestamp >= start);
        Assert.assertTrue(recordedTimestamp <= end);
    }

    @Test
    public void testAggregationDefault() {
        Specification specification = new Specification();
        Aggregation aggregation = new Aggregation();
        aggregation.setType(null);
        aggregation.setSize(BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE - 1);
        specification.setAggregation(aggregation);

        Assert.assertNull(aggregation.getType());
        specification.configure(new BulletConfig());

        // Specification no longer fixes type
        Assert.assertNull(aggregation.getType());
        Assert.assertEquals(aggregation.getSize(), new Integer(BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE - 1));
    }

    @Test
    public void testMeetingDefaultSpecification() {
        Specification specification = new Specification();
        specification.configure(new BulletConfig());

        Assert.assertTrue(makeStream(BulletConfig.DEFAULT_AGGREGATION_SIZE - 1).map(specification::filter).allMatch(x -> x));
        // Check that we only get the default number out
        makeList(BulletConfig.DEFAULT_AGGREGATION_SIZE + 2).forEach(specification::aggregate);
        Assert.assertEquals((Object) specification.getAggregate().getRecords().size(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
    }

    @Test
    public void testValidate() {
        Specification specification = new Specification();
        Aggregation mockAggregation = mock(Aggregation.class);
        Optional<List<Error>> aggregationErrors = Optional.of(asList(Error.of("foo", new ArrayList<>()),
                                                                     Error.of("bar", new ArrayList<>())));
        when(mockAggregation.validate()).thenReturn(aggregationErrors);
        specification.setAggregation(mockAggregation);

        Clause mockClauseA = mock(Clause.class);
        Clause mockClauseB = mock(Clause.class);
        when(mockClauseA.validate()).thenReturn(Optional.of(singletonList(Error.of("baz", new ArrayList<>()))));
        when(mockClauseB.validate()).thenReturn(Optional.of(singletonList(Error.of("qux", new ArrayList<>()))));
        specification.setFilters(asList(mockClauseA, mockClauseB));

        Projection mockProjection = mock(Projection.class);
        when(mockProjection.validate()).thenReturn(Optional.of(singletonList(Error.of("quux", new ArrayList<>()))));
        specification.setProjection(mockProjection);

        Optional<List<Error>> errorList = specification.validate();
        Assert.assertTrue(errorList.isPresent());
        Assert.assertEquals(errorList.get().size(), 5);
    }

    @Test
    public void testValidateNullValues() {
        Specification specification = new Specification();
        specification.setProjection(null);
        specification.setFilters(null);
        specification.setAggregation(null);
        Optional<List<Error>> errorList = specification.validate();
        Assert.assertFalse(errorList.isPresent());
    }

    @Test
    public void testAggregationExceptions() {
        Aggregation aggregation = mock(Aggregation.class);
        FailingStrategy failure = new FailingStrategy();
        when(aggregation.getStrategy()).thenReturn(failure);

        Specification specification = new Specification();
        specification.setAggregation(aggregation);

        specification.aggregate(RecordBox.get().getRecord());
        specification.aggregate(new byte[0]);

        Assert.assertNull(specification.getSerializedAggregate());
        Clip actual = specification.getAggregate();

        Assert.assertNotNull(actual.getMeta());
        Assert.assertEquals(actual.getRecords().size(), 0);

        Map<String, Object> actualMeta = actual.getMeta().asMap();

        Assert.assertEquals(actualMeta.size(), 1);
        Assert.assertNotNull(actualMeta.get(Metadata.ERROR_KEY));

        Error expectedError = Error.makeError("Getting aggregation test failure",
                                              Specification.AGGREGATION_FAILURE_RESOLUTION);
        Assert.assertEquals(actualMeta.get(Metadata.ERROR_KEY), singletonList(expectedError));

        Assert.assertEquals(failure.consumptionFailure, 1);
        Assert.assertEquals(failure.combiningFailure, 1);
        Assert.assertEquals(failure.serializingFailure, 1);
        Assert.assertEquals(failure.aggregationFailure, 1);
    }

    @Test
    public void testToString() {
        BulletConfig config = new BulletConfig();
        Specification specification = new Specification();
        specification.configure(config);

        Assert.assertEquals(specification.toString(),
                "{filters: null, projection: null, " +
                        "aggregation: {size: 1, type: RAW, fields: null, attributes: null}, duration: 30000}");

        specification.setFilters(singletonList(FilterClauseTest.getFieldFilter(FilterType.EQUALS, "foo", "bar")));
        Projection projection = new Projection();
        projection.setFields(singletonMap("field", "bid"));
        specification.setProjection(projection);
        specification.configure(config);

        Assert.assertEquals(specification.toString(),
                            "{filters: [{operation: EQUALS, field: field, values: [foo, bar]}], " +
                            "projection: {fields: {field=bid}}, " +
                            "aggregation: {size: 1, type: RAW, fields: null, attributes: null}, duration: 30000}");
    }
}
