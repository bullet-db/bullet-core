/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.querying.aggregations.GroupStrategy;
import com.yahoo.bullet.querying.aggregations.TupleSketchingStrategy;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class GroupTest {
    private BulletConfig config = new BulletConfig();

    @Test
    public void testGetType() {
        Group aggregation = new Group(null);

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
    }

    @Test
    public void testFields() {
        Group aggregation = new Group(null);

        Assert.assertEquals(aggregation.getFields(), Collections.emptyList());
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.emptyMap());

        aggregation.setFields(Collections.emptyMap());

        Assert.assertEquals(aggregation.getFields(), Collections.emptyList());
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.emptyMap());

        aggregation.setFields(Collections.singletonMap("abc", "def"));

        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "def"));
    }

    @Test
    public void testOperations() {
        Group aggregation = new Group(null);

        Assert.assertEquals(aggregation.getOperations(), Collections.emptyList());

        aggregation.addGroupOperation(new GroupOperation(GroupOperation.GroupOperationType.SUM, "abc", "sum"));
        aggregation.addGroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count");

        Assert.assertEquals(aggregation.getOperations(), Arrays.asList(new GroupOperation(GroupOperation.GroupOperationType.SUM, "abc", "sum"),
                                                                       new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count")));
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "COUNT_FIELD is not a valid operation\\.")
    public void testAddGroupOperationCountFieldThrows() {
        Group aggregation = new Group(null);

        aggregation.addGroupOperation(GroupOperation.GroupOperationType.COUNT_FIELD, "abc", null);
    }

    @Test
    public void testGetStrategyGroupAll() {
        Group aggregation = new Group(null);
        aggregation.configure(config);

        Assert.assertTrue(aggregation.getStrategy(new BulletConfig()) instanceof GroupStrategy);
    }

    @Test
    public void testGetStrategyGroupBy() {
        Group aggregation = new Group(null);
        aggregation.setFields(Collections.singletonMap("abc", "def"));
        aggregation.configure(config);

        Assert.assertTrue(aggregation.getStrategy(config) instanceof TupleSketchingStrategy);
    }

    @Test
    public void testToString() {
        Group aggregation = new Group(null);
        aggregation.setFields(Collections.singletonMap("abc", "def"));
        aggregation.addGroupOperation(GroupOperation.GroupOperationType.SUM, "abc", "sum");

        Assert.assertEquals(aggregation.toString(), "{size: null, type: GROUP, fields: {abc=def}, operations: [{type: SUM, field: abc, name: sum}]}");
    }
}
