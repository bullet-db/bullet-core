/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.aggregations;

import com.yahoo.bullet.query.aggregations.GroupAll;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;

public class GroupAllStrategyTest {
    public static GroupAllStrategy makeGroupAll(List<GroupOperation> groupOperations) {
        GroupAll aggregation = new GroupAll(new HashSet<>(groupOperations));
        return (GroupAllStrategy) aggregation.getStrategy(new BulletConfig());
    }

    public static GroupAllStrategy makeGroupAll(GroupOperation... groupOperations) {
        return makeGroupAll(asList(groupOperations));
    }

    @Test
    public void testNoRecordCount() {
        GroupAllStrategy strategy = makeGroupAll(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));

        Assert.assertNotNull(strategy.getData());
        List<BulletRecord> aggregate = strategy.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("count", 0L).getRecord();
        Assert.assertEquals(actual, expected);

        Assert.assertEquals(strategy.getRecords(), aggregate);
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());
    }

    @Test
    public void testNullRecordCount() {
        GroupAllStrategy strategy = makeGroupAll(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));
        strategy.consume(RecordBox.get().add("foo", "bar").getRecord());
        Assert.assertNotNull(strategy.getData());
        List<BulletRecord> aggregate = strategy.getResult().getRecords();

        // We do not expect to send in null records so the count is incremented.
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("count", 1L).getRecord();
        Assert.assertEquals(actual, expected);

        Assert.assertEquals(strategy.getRecords(), aggregate);
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());
    }

    @Test
    public void testCounting() {
        GroupAllStrategy strategy = makeGroupAll(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1L).getRecord();

        IntStream.range(0, 10).forEach(i -> strategy.consume(someRecord));

        Assert.assertNotNull(strategy.getData());
        List<BulletRecord> aggregate = strategy.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("count", 10L).getRecord();
        Assert.assertEquals(actual, expected);

        Assert.assertEquals(strategy.getRecords(), aggregate);
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());
    }

    @Test
    public void testCountingMoreThanMaximum() {
        GroupAllStrategy strategy = makeGroupAll(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1L).getRecord();

        IntStream.range(0, 2 * BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE).forEach(i -> strategy.consume(someRecord));

        Assert.assertNotNull(strategy.getData());
        List<BulletRecord> aggregate = strategy.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("count",
                                                    2L * BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE).getRecord();
        Assert.assertEquals(actual, expected);

        Assert.assertEquals(strategy.getRecords(), aggregate);
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());
    }

    @Test
    public void testCombiningMetrics() {
        List<GroupOperation> operations = asList(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "myCount"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.MIN, "minField", "myMin"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.AVG, "groupField", "groupAvg"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.MIN, "groupField", "groupMin"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.SUM, "groupField", "groupSum"));

        GroupAllStrategy strategy = makeGroupAll(operations);
        strategy.consume(RecordBox.get().add("minField", -8.8).add("groupField", 3.14).getRecord());
        strategy.consume(RecordBox.get().add("minField", 0.0).addNull("groupField").getRecord());
        strategy.consume(RecordBox.get().add("minField", 51.44).add("groupField", -4.88).getRecord());

        GroupAllStrategy another = makeGroupAll(operations);
        another.consume(RecordBox.get().add("minField", -8.8).add("groupField", 12345.67).getRecord());
        another.consume(RecordBox.get().addNull("minField").add("groupField", 2.718).getRecord());
        another.consume(RecordBox.get().add("minField", -51.0).addNull("groupField").getRecord());
        another.consume(RecordBox.get().add("minField", 0).add("groupField", 1).getRecord());
        another.consume(RecordBox.get().add("minField", 44.8).add("groupField", -51.44).getRecord());
        byte[] serialized = another.getData();

        strategy.combine(serialized);

        Assert.assertNotNull(strategy.getData());
        List<BulletRecord> aggregate = strategy.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);

        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("myCount", 8L).add("myMin", -51.0).add("groupAvg", 2049.368)
                                               .add("groupMin", -51.44).add("groupSum", 12296.208).getRecord();
        Assert.assertTrue(actual.equals(expected));

        Assert.assertEquals(strategy.getRecords(), aggregate);
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());
    }

    @Test
    public void testCombiningMetricsFail() {
        GroupAllStrategy strategy = makeGroupAll(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 10).forEach(i -> strategy.consume(someRecord));

        // Not a serialized GroupData
        strategy.combine(String.valueOf(242).getBytes());

        Assert.assertNotNull(strategy.getData());
        List<BulletRecord> aggregate = strategy.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        // Unchanged count
        BulletRecord expected = RecordBox.get().add("count", 10L).getRecord();
        Assert.assertEquals(actual, expected);

        Assert.assertEquals(strategy.getRecords(), aggregate);
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());
    }

    @Test
    public void testMin() {
        GroupAllStrategy strategy = makeGroupAll(new GroupOperation(GroupOperation.GroupOperationType.MIN, "someField", "min"));
        Assert.assertNotNull(strategy.getData());

        strategy.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("min").getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().add("someField", -4.8).getRecord());
        strategy.consume(RecordBox.get().add("someField", -8.8).getRecord());
        strategy.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("min", -8.8).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("min", -8.8).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().add("someField", -51.44).getRecord());
        expected = RecordBox.get().add("min", -51.44).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        Assert.assertEquals(strategy.getResult().getRecords().size(), 1);

        Assert.assertEquals(strategy.getRecords(), strategy.getResult().getRecords());
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());
    }

    @Test
    public void testMax() {
        GroupAllStrategy strategy = makeGroupAll(new GroupOperation(GroupOperation.GroupOperationType.MAX, "someField", "max"));
        Assert.assertNotNull(strategy.getData());

        strategy.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("max").getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().add("someField", -4.8).getRecord());
        strategy.consume(RecordBox.get().add("someField", -8.8).getRecord());
        strategy.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("max", 51.44).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("max", 51.44).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().add("someField", 88.0).getRecord());
        expected = RecordBox.get().add("max", 88.0).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        Assert.assertEquals(strategy.getResult().getRecords().size(), 1);

        Assert.assertEquals(strategy.getRecords(), strategy.getResult().getRecords());
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());
    }

    @Test
    public void testSum() {
        GroupAllStrategy strategy = makeGroupAll(new GroupOperation(GroupOperation.GroupOperationType.SUM, "someField", "sum"));
        Assert.assertNotNull(strategy.getData());

        strategy.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("sum").getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().add("someField", -4.8).getRecord());
        strategy.consume(RecordBox.get().add("someField", -8).getRecord());
        strategy.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("sum", 38.64).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("sum", 38.64).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().add("someField", 88.0).getRecord());
        expected = RecordBox.get().add("sum", 126.64).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        Assert.assertEquals(strategy.getResult().getRecords().size(), 1);

        Assert.assertEquals(strategy.getRecords(), strategy.getResult().getRecords());
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());
    }

    @Test
    public void testAvg() {
        GroupAllStrategy strategy = makeGroupAll(new GroupOperation(GroupOperation.GroupOperationType.AVG, "someField", "avg"));
        Assert.assertNotNull(strategy.getData());

        strategy.consume(RecordBox.get().addNull("someField").getRecord());
        BulletRecord expected = RecordBox.get().addNull("avg").getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().add("someField", -4.8).getRecord());
        strategy.consume(RecordBox.get().add("someField", -8).getRecord());
        strategy.consume(RecordBox.get().add("someField", 51.44).getRecord());
        expected = RecordBox.get().add("avg", 12.88).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().addNull("someField").getRecord());
        expected = RecordBox.get().add("avg", 12.88).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        strategy.consume(RecordBox.get().add("someField", 88.0).getRecord());
        expected = RecordBox.get().add("avg", 31.66).getRecord();
        Assert.assertEquals(strategy.getResult().getRecords().get(0), expected);

        Assert.assertEquals(strategy.getResult().getRecords().size(), 1);

        Assert.assertEquals(strategy.getRecords(), strategy.getResult().getRecords());
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());
    }

    @Test
    public void testResetting() {
        List<GroupOperation> operations = asList(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "myCount"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.MIN, "minField", "myMin"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.AVG, "groupField", "groupAvg"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.MIN, "groupField", "groupMin"),
                                                 new GroupOperation(GroupOperation.GroupOperationType.SUM, "groupField", "groupSum"));

        GroupAllStrategy strategy = makeGroupAll(operations);
        strategy.consume(RecordBox.get().add("minField", -8.8).add("groupField", 3.14).getRecord());
        strategy.consume(RecordBox.get().add("minField", 0.0).addNull("groupField").getRecord());
        strategy.consume(RecordBox.get().add("minField", 51.44).add("groupField", -4.88).getRecord());
        strategy.consume(RecordBox.get().add("minField", -8.8).add("groupField", 12345.67).getRecord());
        strategy.consume(RecordBox.get().addNull("minField").add("groupField", 2.718).getRecord());
        strategy.consume(RecordBox.get().add("minField", -51.0).addNull("groupField").getRecord());
        strategy.consume(RecordBox.get().add("minField", 0).add("groupField", 1).getRecord());
        strategy.consume(RecordBox.get().add("minField", 44.8).add("groupField", -51.44).getRecord());

        Assert.assertNotNull(strategy.getData());
        List<BulletRecord> aggregate = strategy.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);

        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("myCount", 8L).add("myMin", -51.0).add("groupAvg", 2049.368)
                                               .add("groupMin", -51.44).add("groupSum", 12296.208).getRecord();
        Assert.assertTrue(actual.equals(expected));
        Assert.assertEquals(strategy.getRecords(), aggregate);
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());

        strategy.reset();

        strategy.consume(RecordBox.get().add("minField", -8.8).add("groupField", 3.14).getRecord());
        strategy.consume(RecordBox.get().add("minField", 0.0).addNull("groupField").getRecord());

        Assert.assertNotNull(strategy.getData());
        aggregate = strategy.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 1);

        actual = aggregate.get(0);
        expected = RecordBox.get().add("myCount", 2L).add("myMin", -8.8).add("groupAvg", 3.14)
                                  .add("groupMin", 3.14).add("groupSum", 3.14).getRecord();
        Assert.assertTrue(actual.equals(expected));
        Assert.assertEquals(strategy.getRecords(), aggregate);
        Assert.assertEquals(strategy.getMetadata().asMap(), strategy.getResult().getMeta().asMap());
    }
}
