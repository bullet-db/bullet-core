/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.aggregations.GroupAllStrategy;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class GroupAllTest {
    private BulletConfig config = new BulletConfig();

    @Test
    public void testGetType() {
        GroupAll aggregation = new GroupAll(Collections.emptySet());

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
    }

    @Test
    public void testGetFields() {
        GroupAll aggregation = new GroupAll(Collections.emptySet());

        Assert.assertEquals(aggregation.getFields(), Collections.emptyList());
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "COUNT_FIELD is not a valid operation\\.")
    public void testCountFieldInvalid() {
        GroupOperation operation = new GroupOperation(GroupOperation.GroupOperationType.COUNT_FIELD, "abc", null);
        new GroupAll(Collections.singleton(operation));
    }

    @Test
    public void testGetStrategy() {
        GroupAll aggregation = new GroupAll(Collections.emptySet());
        aggregation.configure(config);

        Assert.assertTrue(aggregation.getStrategy(new BulletConfig()) instanceof GroupAllStrategy);
    }

    @Test
    public void testToString() {
        GroupOperation operation = new GroupOperation(GroupOperation.GroupOperationType.SUM, "abc", "sum");
        GroupAll aggregation = new GroupAll(Collections.singleton(operation));

        Assert.assertEquals(aggregation.toString(), "{size: null, type: GROUP, operations: [{type: SUM, field: abc, name: sum}]}");
    }
}
