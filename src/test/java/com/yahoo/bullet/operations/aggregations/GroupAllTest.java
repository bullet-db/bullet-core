/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.AggregationUtils;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.parsing.AggregationUtils.makeGroupOperation;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupAllTest {
    @SafeVarargs
    public static GroupAll makeGroupAll(Map<String, String>... groupOperations) {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationType.GROUP);
        // Does not matter
        aggregation.setSize(1);
        aggregation.setAttributes(makeAttributes(groupOperations));
        aggregation.configure(new BulletConfig());
        GroupAll all = new GroupAll(aggregation);
        all.initialize();
        return all;
    }

    public static GroupAll makeGroupAll(List<GroupOperation> groupOperations) {
        Aggregation aggregation = mock(Aggregation.class);
        List<Map<String, String>> operations =  groupOperations.stream().map(AggregationUtils::makeGroupOperation)
                                                               .collect(Collectors.toList());

        when(aggregation.getAttributes()).thenReturn(makeAttributes(operations));
        GroupAll all = new GroupAll(aggregation);
        all.initialize();
        return all;
    }

    public static GroupAll makeGroupAll(GroupOperation... groupOperations) {
        return makeGroupAll(asList(groupOperations));
    }

    @Test
    public void testInitialize() {
        GroupAll groupAll = makeGroupAll(Collections.emptyMap());
        List<Error> errors = groupAll.initialize();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR);

        groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.AVG, null, null));
        errors = groupAll.initialize();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Error.makeError(GroupOperation.GROUP_OPERATION_REQUIRES_FIELD +
                                                              GroupOperationType.AVG,
                                                           GroupOperation.OPERATION_REQUIRES_FIELD_RESOLUTION));

        groupAll = makeGroupAll(new GroupOperation(GroupOperationType.COUNT, null, null),
                                new GroupOperation(GroupOperationType.AVG, "foo", null));

        Assert.assertNull(groupAll.initialize());
    }

    @Test
    public void testNoRecordCount() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.COUNT, null, null));

        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add(GroupOperationType.COUNT.getName(), 0L).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testNullRecordCount() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.COUNT, null, "count"));
        groupAll.consume(RecordBox.get().add("foo", "bar").getRecord());
        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation().getRecords();

        // We do not expect to send in null records so the count is incremented.
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("count", 1L).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCounting() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.COUNT, null, "count"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();

        IntStream.range(0, 10).forEach(i -> groupAll.consume(someRecord));

        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("count", 10).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCountingMoreThanMaximum() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.COUNT, null, null));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();

        IntStream.range(0, 2 * BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE).forEach(i -> groupAll.consume(someRecord));

        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add(GroupOperationType.COUNT.getName(),
                                                    2L * BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCombiningMetrics() {
        List<GroupOperation> operations = asList(new GroupOperation(GroupOperationType.COUNT, null, "myCount"),
                                                 new GroupOperation(GroupOperationType.MIN, "minField", "myMin"),
                                                 new GroupOperation(GroupOperationType.AVG, "groupField", "groupAvg"),
                                                 new GroupOperation(GroupOperationType.MIN, "groupField", "groupMin"),
                                                 new GroupOperation(GroupOperationType.SUM, "groupField", "groupSum"));

        GroupAll groupAll = makeGroupAll(operations);
        groupAll.consume(RecordBox.get().add("minField", -8.8).add("groupField", 3.14).getRecord());
        groupAll.consume(RecordBox.get().add("minField", 0.0).addNull("groupField").getRecord());
        groupAll.consume(RecordBox.get().add("minField", 51.44).add("groupField", -4.88).getRecord());

        GroupAll another = makeGroupAll(operations);
        another.consume(RecordBox.get().add("minField", -8.8).add("groupField", 12345.67).getRecord());
        another.consume(RecordBox.get().addNull("minField").add("groupField", 2.718).getRecord());
        another.consume(RecordBox.get().add("minField", -51.0).addNull("groupField").getRecord());
        another.consume(RecordBox.get().add("minField", 0).add("groupField", 1).getRecord());
        another.consume(RecordBox.get().add("minField", 44.8).add("groupField", -51.44).getRecord());
        byte[] serialized = another.getSerializedAggregation();

        groupAll.combine(serialized);

        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation().getRecords();
        Assert.assertEquals(aggregate.size(), 1);

        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("myCount", 8L).add("myMin", -51.0).add("groupAvg", 2049.368)
                                               .add("groupMin", -51.44).add("groupSum", 12296.208).getRecord();
        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void testCombiningMetricsFail() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.COUNT, null, null));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 10).forEach(i -> groupAll.consume(someRecord));

        // Not a serialized GroupData
        groupAll.combine(String.valueOf(242).getBytes());

        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        // Unchanged count
        BulletRecord expected = RecordBox.get().add(GroupOperationType.COUNT.getName(), 10L).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testMin() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.MIN, "someField", "min"));
        Assert.assertNotNull(groupAll.getSerializedAggregation());

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("min").getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", -4.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", -8.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("min", -8.8).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("min", -8.8).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", -51.44).getRecord());
        expected = RecordBox.get().add("min", -51.44).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        Assert.assertEquals(groupAll.getAggregation().getRecords().size(), 1);
    }

    @Test
    public void testMax() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.MAX, "someField", "max"));
        Assert.assertNotNull(groupAll.getSerializedAggregation());

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("max").getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", -4.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", -8.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("max", 51.44).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("max", 51.44).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", 88.0).getRecord());
        expected = RecordBox.get().add("max", 88.0).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        Assert.assertEquals(groupAll.getAggregation().getRecords().size(), 1);
    }

    @Test
    public void testSum() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.SUM, "someField", "sum"));
        Assert.assertNotNull(groupAll.getSerializedAggregation());

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("sum").getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", -4.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", -8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("sum", 38.64).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("sum", 38.64).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", 88.0).getRecord());
        expected = RecordBox.get().add("sum", 126.64).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        Assert.assertEquals(groupAll.getAggregation().getRecords().size(), 1);
    }

    @Test
    public void testAvg() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.AVG, "someField", "avg"));
        Assert.assertNotNull(groupAll.getSerializedAggregation());

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("avg").getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", -4.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", -8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("avg", 12.88).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("avg", 12.88).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", 88.0).getRecord());
        expected = RecordBox.get().add("avg", 31.66).getRecord();
        Assert.assertEquals(groupAll.getAggregation().getRecords().get(0), expected);

        Assert.assertEquals(groupAll.getAggregation().getRecords().size(), 1);
    }
}
