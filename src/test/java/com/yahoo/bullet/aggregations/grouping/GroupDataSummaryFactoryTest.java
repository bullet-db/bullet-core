/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.grouping;

import com.yahoo.bullet.querying.AggregationOperations.GroupOperationType;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.tuple.DeserializeResult;
import com.yahoo.sketches.tuple.Summary;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class GroupDataSummaryFactoryTest {
    private static CachingGroupData sampleSumGroupData(double sum) {
        CachingGroupData data;
        data = new CachingGroupData(singletonMap("fieldA", "bar"),
                                    singletonMap(new GroupOperation(GroupOperationType.SUM, "fieldB", "sum"), null));
        BulletRecord record = RecordBox.get().add("fieldA", "bar").add("fieldB", sum).getRecord();
        data.setCachedRecord(record);
        return data;
    }

    @Test
    public void testStaticSummarySetOperations() {
        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        Assert.assertTrue(factory.getSummarySetOperations() == GroupDataSummaryFactory.SUMMARY_OPERATIONS);
    }

    @Test
    public void testStaticSerialization() {
        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        Assert.assertEquals(factory.toByteArray(), GroupDataSummaryFactory.SERIALIZED);
    }

    @Test
    public void testSummaryCreation() {
        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        Summary summaryA = factory.newSummary();
        Summary summaryB = factory.newSummary();
        Assert.assertNotNull(summaryA);
        Assert.assertNotNull(summaryB);
        Assert.assertFalse(summaryA == summaryB);
    }

    @Test
    public void testDeserialization() {
        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        byte[] serialized = factory.toByteArray();
        Memory memory = new NativeMemory(serialized);

        DeserializeResult<GroupDataSummaryFactory> deserialized = GroupDataSummaryFactory.fromMemory(memory);
        Assert.assertNotNull(deserialized.getObject());
        Assert.assertEquals(deserialized.getSize(), GroupDataSummaryFactory.SERIALIZED_SIZE);
    }

    @Test
    public void testStaticDeserialization() {
        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        byte[] serialized = new byte[10];
        Memory memory = new NativeMemory(serialized);

        // This is testing that not matter what you send it, it will deserialize it
        DeserializeResult<GroupDataSummaryFactory> deserialized = GroupDataSummaryFactory.fromMemory(memory);
        Assert.assertNotNull(deserialized.getObject());
        Assert.assertEquals(deserialized.getSize(), GroupDataSummaryFactory.SERIALIZED_SIZE);

        deserialized = GroupDataSummaryFactory.fromMemory(null);
        Assert.assertNotNull(deserialized.getObject());
        Assert.assertEquals(deserialized.getSize(), GroupDataSummaryFactory.SERIALIZED_SIZE);
    }

    @Test
    public void testSummaryDeserialization() {
        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        GroupDataSummary summary = new GroupDataSummary();

        CachingGroupData data = sampleSumGroupData(30.0);
        summary.update(data);

        byte[] serializedSummary = summary.toByteArray();
        Memory memory = new NativeMemory(serializedSummary);

        DeserializeResult result = factory.summaryFromMemory(memory);
        GroupDataSummary deserialized = (GroupDataSummary) result.getObject();

        Assert.assertNotNull(deserialized);

        BulletRecord actual = deserialized.getData().getAsBulletRecord(emptyMap());
        BulletRecord expected = RecordBox.get().add("fieldA", "bar").add("sum", 30.0).getRecord();
        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void testEmptySummaryDeserialization() {
        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        GroupDataSummary summary = new GroupDataSummary();

        byte[] serializedSummary = summary.toByteArray();
        Memory memory = new NativeMemory(serializedSummary);

        DeserializeResult result = factory.summaryFromMemory(memory);
        GroupDataSummary deserialized = (GroupDataSummary) result.getObject();

        Assert.assertNotNull(deserialized);
        Assert.assertNull(deserialized.getData());
    }
}
