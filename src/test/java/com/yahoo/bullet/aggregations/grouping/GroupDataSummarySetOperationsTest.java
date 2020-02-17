/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations.grouping;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class GroupDataSummarySetOperationsTest {
    private static BulletRecordProvider provider = new BulletConfig().getBulletRecordProvider();

    private static CachingGroupData makeSampleGroupData(int sumValue, double minValue) {
        Map<String, String> fields = new HashMap<>();
        fields.put("fieldA", "foo");
        fields.put("fieldB", "bar");

        Map<GroupOperation, Number> metrics = new HashMap<>();
        GroupOperation operationA = new GroupOperation(GroupOperation.GroupOperationType.SUM, "fieldC", "sum");
        GroupOperation operationB = new GroupOperation(GroupOperation.GroupOperationType.MIN, "fieldD", "min");
        metrics.put(operationA, sumValue);
        metrics.put(operationB, minValue);

        return new CachingGroupData(fields, metrics);
    }

    @Test
    public void testUnionLeftNull() {
        GroupDataSummarySetOperations operations = new GroupDataSummarySetOperations();

        GroupDataSummary summary = new GroupDataSummary();
        summary.setData(makeSampleGroupData(15, 0.1));

        GroupDataSummary result = operations.union(null, summary);
        BulletRecord actual = result.getData().getAsBulletRecord(emptyMap(), provider);
        BulletRecord expected = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar")
                                               .add("sum", 15.0).add("min", 0.1).getRecord();

        // In-place
        Assert.assertTrue(summary == result);
        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void testUnionRightNull() {
        GroupDataSummarySetOperations operations = new GroupDataSummarySetOperations();

        GroupDataSummary summary = new GroupDataSummary();
        summary.setData(makeSampleGroupData(15, 0.1));

        GroupDataSummary result = operations.union(summary, null);
        BulletRecord actual = result.getData().getAsBulletRecord(emptyMap(), provider);
        BulletRecord expected = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar")
                                               .add("sum", 15.0).add("min", 0.1).getRecord();

        // In-place
        Assert.assertTrue(summary == result);
        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void testUnion() {
        GroupDataSummarySetOperations operations = new GroupDataSummarySetOperations();

        GroupDataSummary summaryA = new GroupDataSummary();
        summaryA.setData(makeSampleGroupData(15, 0.1));

        GroupDataSummary summaryB = new GroupDataSummary();
        summaryB.setData(makeSampleGroupData(-20, -10.1));

        GroupDataSummary result = operations.union(summaryA, summaryB);
        BulletRecord actual = result.getData().getAsBulletRecord(emptyMap(), provider);
        BulletRecord expected = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar")
                                               .add("sum", -5.0).add("min", -10.1).getRecord();

        Assert.assertTrue(actual.equals(expected));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testIntersection() {
        GroupDataSummarySetOperations operations = new GroupDataSummarySetOperations();
        operations.intersection(null, null);
    }
}
