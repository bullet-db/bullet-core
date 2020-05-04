/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.grouping;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.tuple.DeserializeResult;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.MAX;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.MIN;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.SUM;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;

public class GroupDataSummaryTest {
    private static BulletRecordProvider provider = new BulletConfig().getBulletRecordProvider();

    public static Map<String, String> makeGroups(List<String> fields) {
        Map<String, String> map = new HashMap<>(fields.size());
        for (int i = 0; i < fields.size(); ++i) {
            map.put("field_" + i, fields.get(i));
        }
        return map;
    }

    public static Map<GroupOperation, Number> makeMetrics(List<GroupOperation.GroupOperationType> types) {
        Map<GroupOperation, Number> map = new HashMap<>(types.size());
        for (int i = 0; i < types.size(); ++i) {
            GroupOperation.GroupOperationType type = types.get(i);
            map.put(new GroupOperation(type, "metric_" + i, type.getName() + "_metric_" + i), null);
        }
        return map;
    }

    private static BulletRecord makeRecord(List<String> fields, List<GroupOperation.GroupOperationType> operations, int base) {
        RecordBox box = RecordBox.get();
        for (int i = 0; i < fields.size(); i++) {
            box.add("field_" + i, fields.get(i));
        }
        for (int i = 0; i < operations.size(); i++) {
            String key = "metric_" + i;
            GroupOperation.GroupOperationType type = operations.get(i);
            Number number = makeNumber(type, base, i, operations.size());
            if (number != null) {
                box.add(key, number);
            }
        }
        return box.getRecord();
    }

    private static Number makeNumber(GroupOperation.GroupOperationType type, int start, int i, int end) {
        switch (type) {
            case COUNT:
                return null;
            case SUM:
                return start + i;
            case MIN:
                return start - i - end;
            case MAX:
                return start + i + end;
            default:
                throw new IllegalArgumentException("Type not supported");
        }
    }

    @Test
    public void testFirstUpdate() {
        List<String> groups = asList("foo", "bar", "baz");
        List<GroupOperation.GroupOperationType> operations = asList(COUNT, MAX, MIN);
        BulletRecord record = makeRecord(groups, operations, 10);
        CachingGroupData data = new CachingGroupData(makeGroups(groups), null, makeMetrics(operations));
        data.setCachedRecord(record);

        GroupDataSummary summary = new GroupDataSummary();
        summary.update(data);

        BulletRecord actual = summary.getData().getAsBulletRecord(emptyMap(), provider);
        // Base is 10 -> MAX is 2nd (10 + 1 + 3), MIN is 3rd (10 - 2 - 3)
        BulletRecord expected = RecordBox.get().add("field_0", "foo").add("field_1", "bar").add("field_2", "baz")
                                               .add("COUNT_metric_0", 1L).add("MAX_metric_1", 14.0).add("MIN_metric_2", 5.0)
                                               .getRecord();
        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void testMultiUpdate() {
        List<String> groups = asList("foo", "bar", "baz");
        List<GroupOperation.GroupOperationType> operations = asList(COUNT, MAX, MIN);
        BulletRecord record = makeRecord(groups, operations, 10);
        CachingGroupData data = new CachingGroupData(makeGroups(groups), null, makeMetrics(operations));
        data.setCachedRecord(record);

        GroupDataSummary summary = new GroupDataSummary();
        summary.update(data);

        // This should stay the same even if we change it...
        data.setGroupFields(null);
        // Base is 5 -> MIN should be changed to 0
        record = makeRecord(groups, operations, 5);
        data.setCachedRecord(record);
        summary.update(data);

        BulletRecord actual = summary.getData().getAsBulletRecord(emptyMap(), provider);
        BulletRecord expected = RecordBox.get().add("field_0", "foo").add("field_1", "bar").add("field_2", "baz")
                                               .add("COUNT_metric_0", 2L).add("MAX_metric_1", 14.0).add("MIN_metric_2", 0.0)
                                               .getRecord();
        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void testMergeNullSummaries() {
        Assert.assertNull(GroupDataSummary.mergeInPlace(null, null));
    }

    @Test
    public void testMergeNullLeftSummary() {
        List<String> groups = asList("foo", "bar");
        List<GroupOperation.GroupOperationType> operations = asList(SUM, MIN);
        CachingGroupData data = new CachingGroupData(makeGroups(groups), null, makeMetrics(operations));

        GroupDataSummary summary = new GroupDataSummary();
        summary.setData(data);

        GroupDataSummary merged = GroupDataSummary.mergeInPlace(null, summary);

        Assert.assertNotNull(merged);

        GroupData mergedData = merged.getData();
        GroupData summaryData = summary.getData();
        Assert.assertNotNull(mergedData);

        // They are the same object
        Assert.assertTrue(mergedData == summaryData);
        Assert.assertTrue(mergedData.groupFields == summaryData.groupFields);
        Assert.assertTrue(mergedData.metrics == summaryData.metrics);
    }

    @Test
    public void testMergeNullRightSummary() {
        List<String> groups = asList("foo", "bar");
        List<GroupOperation.GroupOperationType> operations = asList(SUM, MIN);
        CachingGroupData data = new CachingGroupData(makeGroups(groups), null, makeMetrics(operations));

        GroupDataSummary summary = new GroupDataSummary();
        summary.setData(data);

        GroupDataSummary merged = GroupDataSummary.mergeInPlace(summary, null);

        Assert.assertNotNull(merged);

        GroupData mergedData = merged.getData();
        GroupData summaryData = summary.getData();
        Assert.assertNotNull(mergedData);

        // They are the same object
        Assert.assertTrue(mergedData == summaryData);
        Assert.assertTrue(mergedData.groupFields == summaryData.groupFields);
        Assert.assertTrue(mergedData.metrics == summaryData.metrics);
    }

    @Test
    public void testMergeLeftSummaryWithNullData() {
        List<String> groups = asList("foo", "bar");
        List<GroupOperation.GroupOperationType> operations = asList(SUM, MIN);
        CachingGroupData data = new CachingGroupData(makeGroups(groups), null, makeMetrics(operations));

        GroupDataSummary summaryA = new GroupDataSummary();
        GroupDataSummary summaryB = new GroupDataSummary();

        summaryA.setData(null);
        summaryB.setData(data);

        GroupDataSummary merged = GroupDataSummary.mergeInPlace(summaryA, summaryB);

        Assert.assertNotNull(merged);

        GroupData mergedData = merged.getData();
        GroupData summaryData = summaryB.getData();
        Assert.assertNotNull(mergedData);

        // They are the same object
        Assert.assertTrue(mergedData == summaryData);
        Assert.assertTrue(mergedData.groupFields == summaryData.groupFields);
        Assert.assertTrue(mergedData.metrics == summaryData.metrics);
    }

    @Test
    public void testMergeRightSummaryWithNullData() {
        List<String> groups = asList("foo", "bar");
        List<GroupOperation.GroupOperationType> operations = asList(SUM, MIN);
        CachingGroupData data = new CachingGroupData(makeGroups(groups), null, makeMetrics(operations));

        GroupDataSummary summaryA = new GroupDataSummary();
        GroupDataSummary summaryB = new GroupDataSummary();

        summaryA.setData(data);
        summaryB.setData(null);

        GroupDataSummary merged = GroupDataSummary.mergeInPlace(summaryA, summaryB);

        Assert.assertNotNull(merged);

        GroupData mergedData = merged.getData();
        GroupData summaryData = summaryA.getData();
        Assert.assertNotNull(mergedData);

        // They are the same object
        Assert.assertTrue(mergedData == summaryData);
        Assert.assertTrue(mergedData.groupFields == summaryData.groupFields);
        Assert.assertTrue(mergedData.metrics == summaryData.metrics);
    }

    @Test
    public void testMergeValidSummaries() {
        List<String> groups = asList("foo", "bar");
        List<GroupOperation.GroupOperationType> operations = asList(SUM, MIN);
        CachingGroupData data = new CachingGroupData(makeGroups(groups), null, makeMetrics(operations));

        GroupDataSummary summaryA = new GroupDataSummary();
        GroupDataSummary summaryB = new GroupDataSummary();

        BulletRecord record = makeRecord(groups, operations, 0);
        data.setCachedRecord(record);
        summaryA.update(data);

        record = makeRecord(groups, operations, 15);
        data.setCachedRecord(record);
        summaryA.update(data);

        record = makeRecord(groups, operations, -10);
        data.setCachedRecord(record);
        summaryB.update(data);

        GroupDataSummary merged = GroupDataSummary.mergeInPlace(summaryA, summaryB);
        Assert.assertNotNull(merged);

        GroupData mergedData = merged.getData();
        GroupData summaryAData = summaryA.getData();
        GroupData summaryBData = summaryB.getData();
        Assert.assertNotNull(mergedData);

        // They are the same object
        Assert.assertTrue(mergedData == summaryAData);
        Assert.assertTrue(mergedData.groupFields == summaryAData.groupFields);
        Assert.assertTrue(mergedData.metrics == summaryAData.metrics);

        // They are different objects
        Assert.assertFalse(mergedData == summaryBData);

        BulletRecord actual = mergedData.getAsBulletRecord(emptyMap(), provider);
        BulletRecord expected = RecordBox.get().add("field_0", "foo").add("field_1", "bar")
                                               .add("SUM_metric_0", 5.0).add("MIN_metric_1", -13.0)
                                               .getRecord();

        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void testCopyNullData() {
        GroupDataSummary summary = new GroupDataSummary();
        summary.setData(null);
        GroupDataSummary copy = summary.copy();

        Assert.assertFalse(copy == summary);
        Assert.assertEquals(copy.isInitialized(), summary.isInitialized());
        Assert.assertNull(copy.getData());
        Assert.assertNull(summary.getData());
    }

    @Test
    public void testCopy() {
        List<String> groups = asList("foo", "bar", "baz");
        List<GroupOperation.GroupOperationType> operations = asList(COUNT, MAX, MIN);
        CachingGroupData data = new CachingGroupData(makeGroups(groups), null, makeMetrics(operations));

        GroupDataSummary summary = new GroupDataSummary();
        summary.setData(data);

        GroupDataSummary copy = summary.copy();

        Assert.assertFalse(copy == summary);

        GroupData summaryData = summary.getData();
        GroupData copyData = copy.getData();

        // It is a copy
        Assert.assertFalse(copyData == summaryData);
        Assert.assertFalse(copyData.groupFields == summaryData.groupFields);
        Assert.assertFalse(copyData.metrics == summaryData.metrics);

        // But the values are the same
        Assert.assertEquals(copy.isInitialized(), summary.isInitialized());
        Assert.assertEquals(copyData.groupFields, summaryData.groupFields);
        Assert.assertEquals(copyData.metrics, summaryData.metrics);
    }

    @Test
    public void testSerializationNullData() {
        GroupDataSummary summary = new GroupDataSummary();
        summary.setData(null);

        byte[] serialized = summary.toByteArray();
        Assert.assertNotNull(serialized);

        Memory memory = new NativeMemory(serialized);

        byte initializedByte = memory.getByte(GroupDataSummary.INITIALIZED_POSITION);
        int length = memory.getInt(GroupDataSummary.SIZE_POSITION);
        byte[] dataSerialized = new byte[length];
        memory.getByteArray(GroupDataSummary.DATA_POSITION, dataSerialized, 0, length);
        CachingGroupData data = SerializerDeserializer.fromBytes(dataSerialized);

        Assert.assertTrue(initializedByte == 0);
        Assert.assertTrue(length > 0);
        Assert.assertNull(data);
    }

    @Test
    public void testSerialization() {
        List<String> groups = asList("foo", "bar", "baz");
        List<GroupOperation.GroupOperationType> operations = asList(COUNT, MAX, MIN);
        CachingGroupData data = new CachingGroupData(makeGroups(groups), null, makeMetrics(operations));
        data.setCachedRecord(RecordBox.get().getRecord());

        GroupDataSummary summary = new GroupDataSummary();
        summary.update(data);

        byte[] serialized = summary.toByteArray();
        Assert.assertNotNull(serialized);

        Memory memory = new NativeMemory(serialized);

        byte initializedByte = memory.getByte(GroupDataSummary.INITIALIZED_POSITION);
        int length = memory.getInt(GroupDataSummary.SIZE_POSITION);
        byte[] dataSerialized = new byte[length];
        memory.getByteArray(GroupDataSummary.DATA_POSITION, dataSerialized, 0, length);
        CachingGroupData deserialized = SerializerDeserializer.fromBytes(dataSerialized);

        Assert.assertTrue(initializedByte == 1);
        Assert.assertTrue(length > 0);

        Assert.assertNotNull(deserialized);
        Assert.assertNull(deserialized.getCachedRecord());

        BulletRecord actual = deserialized.getAsBulletRecord(emptyMap(), provider);
        BulletRecord expected = RecordBox.get().add("field_0", "foo").add("field_1", "bar").add("field_2", "baz")
                                               .add("COUNT_metric_0", 1L).addNull("MAX_metric_1").addNull("MIN_metric_2")
                                               .getRecord();

        Assert.assertTrue(actual.equals(expected));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullMemoryDeserialization() {
        GroupDataSummary.fromMemory(null);
    }

    @Test(expectedExceptions = NegativeArraySizeException.class)
    public void testBadMemoryDeserialization() {
        Memory badMemory = new NativeMemory(new byte[16]);
        badMemory.putInt(GroupDataSummary.SIZE_POSITION, -1);
        GroupDataSummary.fromMemory(badMemory);
    }

    @Test
    public void testDeserialization() {
        List<String> groups = asList("foo", "bar", "baz");
        List<GroupOperation.GroupOperationType> operations = asList(COUNT, MAX, MIN);
        CachingGroupData data = new CachingGroupData(makeGroups(groups), null, makeMetrics(operations));

        byte[] serializedData = SerializerDeserializer.toBytes(data);
        int serializedDataLength = serializedData.length;

        Memory memory = new NativeMemory(new byte[GroupDataSummary.DATA_POSITION + serializedDataLength]);
        memory.putByte(GroupDataSummary.INITIALIZED_POSITION, (byte) 1);
        memory.putInt(GroupDataSummary.SIZE_POSITION, serializedDataLength);
        memory.putByteArray(GroupDataSummary.DATA_POSITION, serializedData, 0, serializedDataLength);

        DeserializeResult<GroupDataSummary> deserialized = GroupDataSummary.fromMemory(memory);

        Assert.assertNotNull(deserialized);
        Assert.assertEquals(deserialized.getSize(), GroupDataSummary.DATA_POSITION + serializedDataLength);

        Assert.assertTrue(deserialized.getObject().isInitialized());

        GroupData deserializedData = deserialized.getObject().getData();
        Assert.assertEquals(deserializedData.groupFields, data.groupFields);
        Assert.assertEquals(deserializedData.metrics, data.metrics);
    }
}
