/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.grouping;

import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class CachingGroupDataTest {
    private static final GroupOperation OPERATION = new GroupOperation(GroupOperationType.SUM, "sum", "");

    private static Map<String, String> getSampleGroup() {
        Map<String, String> groups = new HashMap<>();
        groups.put("foo", "bar");
        return groups;
    }

    private static Map<GroupOperation, Number> getSampleMetrics(double value) {
        Map<GroupOperation, Number> metrics = new HashMap<>();
        metrics.put(OPERATION, value);
        return metrics;
    }

    public static CachingGroupData sampleSumGroupData(double sum) {
        return new CachingGroupData(getSampleGroup(), getSampleMetrics(sum));
    }

    @Test
    public void testSelfPartialCopy() {
        CachingGroupData original = sampleSumGroupData(20.0);
        CachingGroupData copy = original.partialCopy();

        copy.groupFields.put("foo", "baz");
        copy.metrics.remove(OPERATION);

        // Group fields are changed
        Assert.assertEquals(original.groupFields.size(), 1);
        Assert.assertEquals(original.groupFields.get("foo"), "baz");
        Assert.assertEquals(copy.groupFields.size(), 1);
        Assert.assertEquals(copy.groupFields.get("foo"), "baz");

        // Metrics are unchanged
        Assert.assertEquals(original.metrics.size(), 1);
        Assert.assertEquals(original.metrics.get(OPERATION), 20.0);
        Assert.assertEquals(copy.metrics.size(), 0);
    }

    @Test
    public void testGroupDataCopyNull() {
        Assert.assertNull(CachingGroupData.copy(null));
    }

    @Test
    public void testGroupDataCopyNullGroups() {
        CachingGroupData original = new CachingGroupData(null, getSampleMetrics(20.0));
        CachingGroupData copy = CachingGroupData.copy(original);

        copy.metrics.remove(OPERATION);
        Assert.assertNull(original.groupFields);
        Assert.assertEquals(original.metrics.size(), 1);
        Assert.assertEquals(original.metrics.get(OPERATION), 20.0);

        Assert.assertNull(copy.groupFields);
        Assert.assertEquals(copy.metrics.size(), 0);
    }

    @Test
    public void testGroupDataCopyNullMetrics() {
        CachingGroupData original = new CachingGroupData(getSampleGroup(), null);
        CachingGroupData copy = CachingGroupData.copy(original);

        copy.groupFields.put("foo", "baz");

        Assert.assertEquals(original.groupFields.size(), 1);
        Assert.assertEquals(original.groupFields.get("foo"), "bar");
        Assert.assertNull(original.metrics);

        Assert.assertEquals(copy.groupFields.size(), 1);
        Assert.assertEquals(copy.groupFields.get("foo"), "baz");
        Assert.assertNull(copy.metrics);
    }

    @Test
    public void testGroupDataCopy() {
        Map<String, String> groups = new HashMap<>();
        groups.put("foo", "bar");

        Map<GroupOperation, Number> metrics = new HashMap<>();
        GroupOperation operation = new GroupOperation(GroupOperationType.SUM, "sum", "");
        metrics.put(operation, 20.0);

        CachingGroupData original = new CachingGroupData(groups, metrics);
        CachingGroupData copy = CachingGroupData.copy(original);

        copy.groupFields.put("foo", "baz");
        copy.metrics.remove(operation);

        Assert.assertEquals(original.groupFields.size(), 1);
        Assert.assertEquals(original.groupFields.get("foo"), "bar");
        Assert.assertEquals(original.metrics.size(), 1);
        Assert.assertEquals(original.metrics.get(operation), 20.0);

        Assert.assertEquals(copy.groupFields.size(), 1);
        Assert.assertEquals(copy.groupFields.get("foo"), "baz");
        Assert.assertEquals(copy.metrics.size(), 0);
    }
}
