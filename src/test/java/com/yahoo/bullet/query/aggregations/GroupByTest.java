/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.querying.aggregations.TupleSketchingStrategy;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class GroupByTest {
    private BulletConfig config = new BulletConfig();

    @Test
    public void testGetType() {
        GroupBy aggregation = new GroupBy(null, Collections.singletonMap("abc", "def"), Collections.emptySet());

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
    }

    @Test
    public void testGetFields() {
        GroupBy aggregation = new GroupBy(null, Collections.singletonMap("abc", "def"), Collections.emptySet());

        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "def"));
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "GROUP BY requires at least one field\\.")
    public void testMissingFields() {
        new GroupBy(null, Collections.emptyMap(), Collections.emptySet());
    }

    @Test
    public void testOperations() {
        GroupBy aggregation = new GroupBy(null, Collections.singletonMap("abc", "def"), Collections.emptySet());

        Assert.assertEquals(aggregation.getOperations(), Collections.emptySet());

        Set<GroupOperation> operations = new HashSet<>();
        operations.add(new GroupOperation(GroupOperation.GroupOperationType.SUM, "abc", "sum"));
        operations.add(new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"));

        aggregation = new GroupBy(null, Collections.singletonMap("abc", "def"), operations);

        Assert.assertEquals(aggregation.getOperations(), new HashSet<>(Arrays.asList(new GroupOperation(GroupOperation.GroupOperationType.SUM, "abc", "sum"),
                                                                                     new GroupOperation(GroupOperation.GroupOperationType.COUNT, null, "count"))));
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "COUNT_FIELD is not a valid operation\\.")
    public void testCountFieldInvalid() {
        GroupOperation operation = new GroupOperation(GroupOperation.GroupOperationType.COUNT_FIELD, "abc", null);
        new GroupBy(null, Collections.singletonMap("abc", "def"), Collections.singleton(operation));
    }

    @Test
    public void testGetStrategy() {
        GroupBy aggregation = new GroupBy(null, Collections.singletonMap("abc", "def"), Collections.emptySet());
        aggregation.configure(config);

        Assert.assertTrue(aggregation.getStrategy(config) instanceof TupleSketchingStrategy);
    }

    @Test
    public void testToString() {
        GroupOperation operation = new GroupOperation(GroupOperation.GroupOperationType.SUM, "abc", "sum");
        GroupBy aggregation = new GroupBy(null, Collections.singletonMap("abc", "def"), Collections.singleton(operation));

        Assert.assertEquals(aggregation.toString(), "{size: null, type: GROUP, fields: {abc=def}, operations: [{type: SUM, field: abc, name: sum}]}");
    }
}
