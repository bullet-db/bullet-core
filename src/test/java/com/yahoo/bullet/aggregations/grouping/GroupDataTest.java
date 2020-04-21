/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations.grouping;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;

public class GroupDataTest {
    private static BulletRecordProvider provider = new BulletConfig().getBulletRecordProvider();

    public static GroupData make(Map<String, String> groupFields, GroupOperation... operations) {
        return new GroupData(groupFields, new HashSet<>(asList(operations)));
    }

    public static GroupData make(GroupOperation... operations) {
        return make(null, operations);
    }

    @Test
    public void testNameExtraction() {
        GroupOperation operation;

        operation = new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "bar");
        Assert.assertEquals(GroupData.getResultName(operation), "bar");

        operation = new GroupOperation(GroupOperation.GroupOperationType.COUNT, "foo", "bar");
        Assert.assertEquals(GroupData.getResultName(operation), "bar");

        operation = new GroupOperation(GroupOperation.GroupOperationType.AVG, "foo", "bar");
        Assert.assertEquals(GroupData.getResultName(operation), "bar");
    }

    @Test
    public void testNullRecordCount() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));
        data.consume(RecordBox.get().add("foo", "bar").getRecord());

        // We do not expect to send in null records so the count is incremented.
        BulletRecord expected = RecordBox.get().add("count", 1L).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNoRecordCount() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));

        // Count should be 0 if there was no data presented.
        BulletRecord expected = RecordBox.get().add("count", 0L).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testSingleCounting() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "foo"));
        BulletRecord expected = RecordBox.get().add("foo", 0L).getRecord();

        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().getRecord());
        expected = RecordBox.get().add("foo", 1L).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testMultiCounting() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();

        IntStream.range(0, 10).forEach(i -> data.consume(someRecord));

        BulletRecord expected = RecordBox.get().add("count", 10L).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testCountingMoreThanMaximum() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();

        IntStream.range(0, 2 * BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE).forEach(i -> data.consume(someRecord));

        BulletRecord expected = RecordBox.get().add("count",
                                                    2L * BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testMergingMetric() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.COUNT, "shouldBeIgnored", "myCount"),
                              new GroupOperation(GroupOperation.GroupOperationType.MIN, "groupField", "myMin"),
                              new GroupOperation(GroupOperation.GroupOperationType.MAX, "groupField", "myMax"),
                              new GroupOperation(GroupOperation.GroupOperationType.SUM, "groupField", "mySum"),
                              new GroupOperation(GroupOperation.GroupOperationType.AVG, "groupField", "myAvg"));
        asList(0.0, -8.8, 51.0).stream().map(x -> RecordBox.get().add("groupField", x).getRecord())
                                        .forEach(data::consume);

        GroupData another = make(new GroupOperation(GroupOperation.GroupOperationType.COUNT, "alsoIgnored", "myCount"),
                                 new GroupOperation(GroupOperation.GroupOperationType.MIN, "groupField", "myMin"),
                                 new GroupOperation(GroupOperation.GroupOperationType.MAX, "groupField", "myMax"),
                                 new GroupOperation(GroupOperation.GroupOperationType.SUM, "groupField", "mySum"),
                                 new GroupOperation(GroupOperation.GroupOperationType.AVG, "groupField", "myAvg"));
        asList(1.1, 4.4, -44.0, 12345.67, 3.3).stream().map(x -> RecordBox.get().add("groupField", x).getRecord())
                                                       .forEach(data::consume);
        byte[] serialized = SerializerDeserializer.toBytes(another);

        data.combine(serialized);

        BulletRecord expected = RecordBox.get().add("myCount", 8L).add("myMin", -44.0)
                                               .add("myMax", 12345.67).add("mySum", 12352.67)
                                               .add("myAvg", 1544.08375).getRecord();
        Assert.assertTrue(expected.equals(data.getMetricsAsBulletRecord(provider)));
    }

    @Test
    public void testGroupMultipleFields() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.COUNT, "shouldBeIgnored", "myCount"),
                new GroupOperation(GroupOperation.GroupOperationType.MIN, "minField", "myMin"),
                new GroupOperation(GroupOperation.GroupOperationType.MIN, "groupField", "minGroupField"),
                new GroupOperation(GroupOperation.GroupOperationType.MAX, "maxField", "myMax"),
                new GroupOperation(GroupOperation.GroupOperationType.MAX, "groupField", "maxGroupField"),
                new GroupOperation(GroupOperation.GroupOperationType.SUM, "groupField", "sumGroupField"));
        List<Double> minColumnValues = asList(0.0, -8.8, 51.0);
        List<Double> maxColumnValues = asList(4.4, 88.51, -8.44);
        List<Double> groupColumnValues = asList(123.45, -884451.8851, 3.14);
        List<BulletRecord> records = new ArrayList<>();
        for (int i = 0; i < minColumnValues.size(); i++) {
            RecordBox recordBox = RecordBox.get();
            recordBox.add("minField", minColumnValues.get(i));
            recordBox.add("maxField", maxColumnValues.get(i));
            recordBox.add("groupField", groupColumnValues.get(i));
            records.add(recordBox.getRecord());
        }

        records.stream().forEach(data::consume);

        BulletRecord expected = RecordBox.get().add("myCount", 3L).add("myMin", -8.8)
                .add("minGroupField", -884451.8851).add("myMax", 88.51).add("maxGroupField", 123.45)
                .add("sumGroupField", -884325.2951).getRecord();
        Assert.assertTrue(expected.equals(data.getMetricsAsBulletRecord(provider)));
    }

    @Test
    public void testMergingRawMetricFail() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 10).forEach(i -> data.consume(someRecord));

        // Not a serialized GroupData
        data.combine(String.valueOf(242).getBytes());

        // Unchanged count
        BulletRecord expected = RecordBox.get().add("count", 10L).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testMergingSupportedAndUnSupportedOperation() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 10).forEach(i -> data.consume(someRecord));

        GroupData another = make(new GroupOperation(GroupOperation.GroupOperationType.AVG, "foo", "bar"));
        IntStream.range(0, 21).forEach(i -> another.consume(someRecord));
        byte[] serialized = SerializerDeserializer.toBytes(another);

        // This should combine since we only merge known GroupOperations from the other GroupData
        data.combine(serialized);

        // AVG should not have influenced other counts.
        BulletRecord expected = RecordBox.get().add("count", 10L).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNullRecordMin() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.MIN, "abc", "min"));
        data.consume(RecordBox.get().add("foo", "bar").getRecord());

        BulletRecord expected = RecordBox.get().addNull("min").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNoRecordMin() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.MIN, "foo", "min"));

        // MIN will return null if no records are observed
        BulletRecord expected = RecordBox.get().addNull("min").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testSingleMin() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.MIN, "someField", "foo"));
        BulletRecord expected = RecordBox.get().addNull("foo").getRecord();

        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", 8.8).getRecord());
        expected = RecordBox.get().add("foo", 8.8).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testMultiMin() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.MIN, "someField", "foo"));
        List<Double> numbers = asList(0.0, 8.8, -88.0, 51.0, 4.0, -4.0, 1234567.89, -51.0);

        numbers.stream().map(x -> RecordBox.get().add("someField", x).getRecord()).forEach(data::consume);

        BulletRecord expected = RecordBox.get().add("foo", -88.0).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNonNumericMin() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.MIN, "someField", "foo"));
        BulletRecord expected = RecordBox.get().addNull("foo").getRecord();

        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", "nonNumericValue").getRecord());
        expected = RecordBox.get().addNull("foo").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", 8.8).getRecord());
        expected = RecordBox.get().add("foo", 8.8).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", "nonNumericValue").getRecord());
        expected = RecordBox.get().add("foo", 8.8).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNullRecordMax() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.MAX, "abc", "max"));
        data.consume(RecordBox.get().add("foo", "bar").getRecord());

        BulletRecord expected = RecordBox.get().addNull("max").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNoRecordMax() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.MAX, "foo", "max"));

        // MAX will return null if no records are observed
        BulletRecord expected = RecordBox.get().addNull("max").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testSingleMax() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.MAX, "someField", "foo"));
        BulletRecord expected = RecordBox.get().addNull("foo").getRecord();

        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", 8.8).getRecord());
        expected = RecordBox.get().add("foo", 8.8).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testMultiMax() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.MAX, "someField", "foo"));
        List<Double> numbers = asList(0.0, 8.8, -88.0, 51.0, 4.0, -4.0, 1234567.89, -51.0);

        numbers.stream().map(x -> RecordBox.get().add("someField", x).getRecord()).forEach(data::consume);

        BulletRecord expected = RecordBox.get().add("foo", 1234567.89).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNonNumericMax() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.MAX, "someField", "foo"));
        BulletRecord expected = RecordBox.get().addNull("foo").getRecord();

        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", "nonNumericValue").getRecord());
        expected = RecordBox.get().addNull("foo").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", 8.8).getRecord());
        expected = RecordBox.get().add("foo", 8.8).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", "nonNumericValue").getRecord());
        expected = RecordBox.get().add("foo", 8.8).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNullRecordSum() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.SUM, "abc", "sum"));
        data.consume(RecordBox.get().add("foo", "bar").getRecord());

        BulletRecord expected = RecordBox.get().addNull("sum").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNoRecordSum() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.SUM, "foo", "sum"));

        // SUM will return null if no records are observed
        BulletRecord expected = RecordBox.get().addNull("sum").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testSingleSum() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.SUM, "someField", "foo"));
        BulletRecord expected = RecordBox.get().addNull("foo").getRecord();

        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", 8.8).getRecord());
        expected = RecordBox.get().add("foo", 8.8).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testMultiSum() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.SUM, "someField", "foo"));
        List<Double> numbers = asList(0.0, 8.8, -88.0, 51.0, 4.0, -4.0, 1234567.89, -51.0);

        numbers.stream().map(x -> RecordBox.get().add("someField", x).getRecord()).forEach(data::consume);

        BulletRecord expected = RecordBox.get().add("foo", 1234488.69).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNonNumericSum() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.SUM, "someField", "foo"));
        BulletRecord expected = RecordBox.get().addNull("foo").getRecord();

        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", "nonNumericValue").getRecord());
        expected = RecordBox.get().addNull("foo").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", 8.8).getRecord());
        expected = RecordBox.get().add("foo", 8.8).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", "nonNumericValue").getRecord());
        expected = RecordBox.get().add("foo", 8.8).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", 51.4).getRecord());
        expected = RecordBox.get().add("foo", 60.2).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testMultiSumOfLongs() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.SUM, "someField", "foo"));
        List<Long> numbers = asList(0L, 8L, -88L, 51L, 4L);

        numbers.stream().map(x -> RecordBox.get().add("someField", x).getRecord()).forEach(data::consume);

        BulletRecord expected = RecordBox.get().add("foo", -25.0).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNullRecordAvg() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.AVG, "someField", "avg"));
        data.consume(RecordBox.get().add("foo", "bar").getRecord());

        BulletRecord expected = RecordBox.get().addNull("avg").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNoRecordAvg() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.AVG, "someField", "avg"));

        // AVG will return null if no records are observed
        BulletRecord expected = RecordBox.get().addNull("avg").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testSingleAvg() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.AVG, "someField", "foo"));
        BulletRecord expected = RecordBox.get().addNull("foo").getRecord();

        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", 8.8).getRecord());
        expected = RecordBox.get().add("foo", 8.8).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testMultiAvg() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.AVG, "someField", "foo"));
        List<Double> numbers = asList(0.0, 8.8, -88.0, 51.0, 4.0, -4.0, 1234567.89, -51.0);

        numbers.stream().map(x -> RecordBox.get().add("someField", x).getRecord()).forEach(data::consume);

        BulletRecord expected = RecordBox.get().add("foo", 154311.08625).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testNonNumericsCountedAvg() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.AVG, "someField", "foo"));
        BulletRecord expected = RecordBox.get().addNull("foo").getRecord();

        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", "nonNumericValue").getRecord());
        expected = RecordBox.get().addNull("foo").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", 8.6).getRecord());
        data.consume(RecordBox.get().add("someField", 51.4).getRecord());
        // Only the numerics are averaged
        expected = RecordBox.get().add("foo", 30.0).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", "nonNumericValue").getRecord());
        expected = RecordBox.get().add("foo", 30.0).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", 0).getRecord());
        data.consume(RecordBox.get().add("someField", -20).getRecord());
        expected = RecordBox.get().add("foo", 10.0).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testIgnoreNullsAvg() {
        GroupData data = make(new GroupOperation(GroupOperation.GroupOperationType.AVG, "someField", "foo"));
        BulletRecord expected = RecordBox.get().addNull("foo").getRecord();

        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().addNull("foo").getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", 8.8).getRecord());
        data.consume(RecordBox.get().add("someField", 51.4).getRecord());
        expected = RecordBox.get().add("foo", 30.1).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("foo", 30.1).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);

        data.consume(RecordBox.get().add("someField", -4.4).getRecord());
        expected = RecordBox.get().add("foo", 18.6).getRecord();
        Assert.assertEquals(data.getMetricsAsBulletRecord(provider), expected);
    }

    @Test
    public void testGroupFieldsInData() {
        Map<String, String> fields = new HashMap<>();
        fields.put("fieldA", "foo");
        fields.put("fieldB", "bar");

        GroupData data = make(fields, new GroupOperation(GroupOperation.GroupOperationType.SUM, "someField", "sum"));

        BulletRecord expectedUnmapped = RecordBox.get().addNull("sum").getRecord();

        Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put("fieldA", "newFieldNameA");
        fieldMapping.put("fieldB", "fieldB");
        BulletRecord expected = RecordBox.get().add("newFieldNameA", "foo").add("fieldB", "bar").addNull("sum").getRecord();

        Assert.assertTrue(data.getMetricsAsBulletRecord(provider).equals(expectedUnmapped));
        Assert.assertTrue(data.getAsBulletRecord(fieldMapping, provider).equals(expected));

        data.consume(RecordBox.get().add("someField", 21.0).getRecord());
        data.consume(RecordBox.get().add("someField", 21.0).getRecord());
        data.consume(RecordBox.get().addNull("someField").getRecord());

        expected = RecordBox.get().add("foo", "foo").add("bar", "bar").add("sum", 42.0).getRecord();
        Assert.assertTrue(data.getAsBulletRecord(fields, provider).equals(expected));
    }

    @Test
    public void testGroupFieldsInDataNameClash() {
        Map<String, String> fields = new HashMap<>();
        fields.put("fieldA", "foo");
        fields.put("fieldB", "bar");

        GroupData data = make(fields, new GroupOperation(GroupOperation.GroupOperationType.SUM, "someField", "fieldB"));

        BulletRecord expected = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar").getRecord();

        Assert.assertTrue(data.getAsBulletRecord(provider).equals(expected));
        Assert.assertTrue(data.getAsBulletRecord(Collections.emptyMap(), provider).equals(expected));

        data.consume(RecordBox.get().add("someField", 21.0).getRecord());
        data.consume(RecordBox.get().add("someField", 21.0).getRecord());

        expected = RecordBox.get().add("fieldB", 42.0).getRecord();
        Assert.assertTrue(data.getMetricsAsBulletRecord(provider).equals(expected));
    }

    @Test
    public void testCastingNonNumericCastableMetrics() {
        Map<String, String> fields = new HashMap<>();
        fields.put("fieldA", "foo");
        fields.put("fieldB", "bar");
        GroupData data = make(fields, new GroupOperation(GroupOperation.GroupOperationType.SUM, "someField", "sum"),
                              new GroupOperation(GroupOperation.GroupOperationType.AVG, "otherField", "avg"));
        BulletRecord record;

        record = RecordBox.get().add("someField", "48.2").add("otherField", "17").getRecord();
        data.consume(record);
        record = RecordBox.get().add("someField", "35.8").add("otherField", "67").getRecord();
        data.consume(record);

        BulletRecord expected = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar")
                                               .add("sum", 84.0).add("avg", 42.0).getRecord();

        BulletRecord actual = data.getAsBulletRecord(provider);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCastingNonNumericNotCastableMetrics() {
        Map<String, String> fields = new HashMap<>();
        fields.put("fieldA", "foo");
        fields.put("fieldB", "bar");
        GroupData data = make(fields, new GroupOperation(GroupOperation.GroupOperationType.SUM, "someField", "sum"),
                              new GroupOperation(GroupOperation.GroupOperationType.AVG, "otherField", "avg"));
        BulletRecord record;

        record = RecordBox.get().add("someField", "48.2").add("otherField", "17").getRecord();
        data.consume(record);
        // otherField is null
        record = RecordBox.get().add("someField", "35.8").addNull("otherField").getRecord();
        data.consume(record);
        // Null someField and otherField is now a map
        record = RecordBox.get().addNull("someField").addMap("otherField", Pair.of("Now", "A map")).getRecord();
        data.consume(record);
        BulletRecord expected = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar")
                                               .add("sum", 84.0).add("avg", 17.0).getRecord();

        BulletRecord actual = data.getAsBulletRecord(provider);
        Assert.assertEquals(actual, expected);
    }
}
