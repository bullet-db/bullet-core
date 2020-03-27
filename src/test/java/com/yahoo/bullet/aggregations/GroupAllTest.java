/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.AggregationUtils;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT_FIELD;
import static com.yahoo.bullet.query.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.query.AggregationUtils.makeGroupOperation;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupAllTest {
    public static Aggregation makeAggregation(Map<String, Object> attributes) {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.GROUP);
        // Does not matter
        aggregation.setSize(1);
        aggregation.setAttributes(attributes);
        return aggregation;
    }

    @SafeVarargs
    public static GroupAll makeGroupAll(Map<String, String>... groupOperations) {
        GroupAll all = new GroupAll(makeAggregation(makeAttributes(groupOperations)), new BulletConfig());
        all.initialize();
        return all;
    }

    public static GroupAll makeGroupAll(List<GroupOperation> groupOperations) {
        Aggregation aggregation = mock(Aggregation.class);
        List<Map<String, String>> operations =  groupOperations.stream().map(AggregationUtils::makeGroupOperation)
                                                               .collect(Collectors.toList());

        when(aggregation.getAttributes()).thenReturn(makeAttributes(operations));
        GroupAll all = new GroupAll(aggregation, new BulletConfig());
        all.initialize();
        return all;
    }

    public static GroupAll makeGroupAll(GroupOperation... groupOperations) {
        return makeGroupAll(asList(groupOperations));
    }

    @Test
    public void testUnsupportedOperation() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(COUNT_FIELD, "someField", "myCountField"));

        Optional<List<BulletError>> optionalErrors = groupAll.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        List<BulletError> errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR);
    }

    @Test
    public void testAttributeOperationMissing() {
        Aggregation aggregation = makeAggregation(singletonMap(GroupOperation.OPERATIONS, null));
        GroupAll groupAll = new GroupAll(aggregation, new BulletConfig());

        Optional<List<BulletError>> optionalErrors = groupAll.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        List<BulletError> errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR);
    }

    @Test
    public void testAttributeOperationBadFormat() {
        Aggregation aggregation = makeAggregation(singletonMap(GroupOperation.OPERATIONS, asList("foo")));
        GroupAll groupAll = new GroupAll(aggregation, new BulletConfig());

        Optional<List<BulletError>> optionalErrors = groupAll.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        List<BulletError> errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR);
    }

    @Test
    public void testInitialize() {
        GroupAll groupAll = makeGroupAll(Collections.emptyMap());
        Optional<List<BulletError>> optionalErrors = groupAll.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        List<BulletError> errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR);

        groupAll = makeGroupAll(makeGroupOperation(GroupOperation.GroupOperationType.AVG, null, null));
        optionalErrors = groupAll.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), BulletError.makeError(GroupOperation.GROUP_OPERATION_REQUIRES_FIELD +
                                                                     GroupOperation.GroupOperationType.AVG,
                                                                 GroupOperation.OPERATION_REQUIRES_FIELD_RESOLUTION));

        groupAll = makeGroupAll(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, null),
                                new GroupOperation(GroupOperation.GroupOperationType.AVG, "foo", null));

        Assert.assertFalse(groupAll.initialize().isPresent());
    }

    @Test
    public void testNoRecordCount() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperation.GroupOperationType.COUNT, null, null));

        Assert.assertNotNull(groupAll.getData());
        List<BulletRecord> aggregate = groupAll.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add(GroupOperation.GroupOperationType.COUNT.getName(), 0L).getRecord();
        Assert.assertEquals(actual, expected);

        Assert.assertEquals(groupAll.getRecords(), aggregate);
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());
    }

    @Test
    public void testNullRecordCount() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));
        groupAll.consume(RecordBox.get().add("foo", "bar").getRecord());
        Assert.assertNotNull(groupAll.getData());
        List<BulletRecord> aggregate = groupAll.getResult().getRecords();

        // We do not expect to send in null records so the count is incremented.
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("count", 1L).getRecord();
        Assert.assertEquals(actual, expected);

        Assert.assertEquals(groupAll.getRecords(), aggregate);
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());
    }

    @Test
    public void testCounting() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1L).getRecord();

        IntStream.range(0, 10).forEach(i -> groupAll.consume(someRecord));

        Assert.assertNotNull(groupAll.getData());
        List<BulletRecord> aggregate = groupAll.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("count", 10L).getRecord();
        Assert.assertEquals(actual, expected);

        Assert.assertEquals(groupAll.getRecords(), aggregate);
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());
    }

    @Test
    public void testCountingMoreThanMaximum() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperation.GroupOperationType.COUNT, null, null));
        BulletRecord someRecord = RecordBox.get().add("foo", 1L).getRecord();

        IntStream.range(0, 2 * BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE).forEach(i -> groupAll.consume(someRecord));

        Assert.assertNotNull(groupAll.getData());
        List<BulletRecord> aggregate = groupAll.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add(GroupOperation.GroupOperationType.COUNT.getName(),
                                                    2L * BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE).getRecord();
        Assert.assertEquals(actual, expected);

        Assert.assertEquals(groupAll.getRecords(), aggregate);
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());
    }

    @Test
    public void testCombiningMetrics() {
        List<GroupOperation> operations = asList(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "myCount"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.MIN, "minField", "myMin"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.AVG, "groupField", "groupAvg"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.MIN, "groupField", "groupMin"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.SUM, "groupField", "groupSum"));

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
        byte[] serialized = another.getData();

        groupAll.combine(serialized);

        Assert.assertNotNull(groupAll.getData());
        List<BulletRecord> aggregate = groupAll.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);

        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("myCount", 8L).add("myMin", -51.0).add("groupAvg", 2049.368)
                                               .add("groupMin", -51.44).add("groupSum", 12296.208).getRecord();
        Assert.assertTrue(actual.equals(expected));

        Assert.assertEquals(groupAll.getRecords(), aggregate);
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());
    }

    @Test
    public void testCombiningMetricsFail() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperation.GroupOperationType.COUNT, null, null));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 10).forEach(i -> groupAll.consume(someRecord));

        // Not a serialized GroupData
        groupAll.combine(String.valueOf(242).getBytes());

        Assert.assertNotNull(groupAll.getData());
        List<BulletRecord> aggregate = groupAll.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        // Unchanged count
        BulletRecord expected = RecordBox.get().add(GroupOperation.GroupOperationType.COUNT.getName(), 10L).getRecord();
        Assert.assertEquals(actual, expected);

        Assert.assertEquals(groupAll.getRecords(), aggregate);
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());
    }

    @Test
    public void testMin() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperation.GroupOperationType.MIN, "someField", "min"));
        Assert.assertNotNull(groupAll.getData());

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("min").getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", -4.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", -8.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("min", -8.8).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("min", -8.8).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", -51.44).getRecord());
        expected = RecordBox.get().add("min", -51.44).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        Assert.assertEquals(groupAll.getResult().getRecords().size(), 1);

        Assert.assertEquals(groupAll.getRecords(), groupAll.getResult().getRecords());
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());
    }

    @Test
    public void testMax() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperation.GroupOperationType.MAX, "someField", "max"));
        Assert.assertNotNull(groupAll.getData());

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("max").getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", -4.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", -8.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("max", 51.44).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("max", 51.44).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", 88.0).getRecord());
        expected = RecordBox.get().add("max", 88.0).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        Assert.assertEquals(groupAll.getResult().getRecords().size(), 1);

        Assert.assertEquals(groupAll.getRecords(), groupAll.getResult().getRecords());
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());
    }

    @Test
    public void testSum() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperation.GroupOperationType.SUM, "someField", "sum"));
        Assert.assertNotNull(groupAll.getData());

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("sum").getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", -4.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", -8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("sum", 38.64).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("sum", 38.64).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", 88.0).getRecord());
        expected = RecordBox.get().add("sum", 126.64).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        Assert.assertEquals(groupAll.getResult().getRecords().size(), 1);

        Assert.assertEquals(groupAll.getRecords(), groupAll.getResult().getRecords());
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());
    }

    @Test
    public void testAvg() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperation.GroupOperationType.AVG, "someField", "avg"));
        Assert.assertNotNull(groupAll.getData());

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("avg").getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", -4.8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", -8).getRecord());
        groupAll.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("avg", 12.88).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("avg", 12.88).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        groupAll.consume(RecordBox.get().add("someField", 88.0).getRecord());
        expected = RecordBox.get().add("avg", 31.66).getRecord();
        Assert.assertEquals(groupAll.getResult().getRecords().get(0), expected);

        Assert.assertEquals(groupAll.getResult().getRecords().size(), 1);

        Assert.assertEquals(groupAll.getRecords(), groupAll.getResult().getRecords());
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());
    }

    @Test
    public void testResetting() {
        List<GroupOperation> operations = asList(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "myCount"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.MIN, "minField", "myMin"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.AVG, "groupField", "groupAvg"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.MIN, "groupField", "groupMin"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.SUM, "groupField", "groupSum"));

        GroupAll groupAll = makeGroupAll(operations);
        groupAll.consume(RecordBox.get().add("minField", -8.8).add("groupField", 3.14).getRecord());
        groupAll.consume(RecordBox.get().add("minField", 0.0).addNull("groupField").getRecord());
        groupAll.consume(RecordBox.get().add("minField", 51.44).add("groupField", -4.88).getRecord());
        groupAll.consume(RecordBox.get().add("minField", -8.8).add("groupField", 12345.67).getRecord());
        groupAll.consume(RecordBox.get().addNull("minField").add("groupField", 2.718).getRecord());
        groupAll.consume(RecordBox.get().add("minField", -51.0).addNull("groupField").getRecord());
        groupAll.consume(RecordBox.get().add("minField", 0).add("groupField", 1).getRecord());
        groupAll.consume(RecordBox.get().add("minField", 44.8).add("groupField", -51.44).getRecord());

        Assert.assertNotNull(groupAll.getData());
        List<BulletRecord> aggregate = groupAll.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);

        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("myCount", 8L).add("myMin", -51.0).add("groupAvg", 2049.368)
                                               .add("groupMin", -51.44).add("groupSum", 12296.208).getRecord();
        Assert.assertTrue(actual.equals(expected));
        Assert.assertEquals(groupAll.getRecords(), aggregate);
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());

        groupAll.reset();

        groupAll.consume(RecordBox.get().add("minField", -8.8).add("groupField", 3.14).getRecord());
        groupAll.consume(RecordBox.get().add("minField", 0.0).addNull("groupField").getRecord());

        Assert.assertNotNull(groupAll.getData());
        aggregate = groupAll.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);

        actual = aggregate.get(0);
        expected = RecordBox.get().add("myCount", 2L).add("myMin", -8.8).add("groupAvg", 3.14)
                                  .add("groupMin", 3.14).add("groupSum", 3.14).getRecord();
        Assert.assertTrue(actual.equals(expected));
        Assert.assertEquals(groupAll.getRecords(), aggregate);
        Assert.assertEquals(groupAll.getMetadata().asMap(), groupAll.getResult().getMeta().asMap());
    }
}
