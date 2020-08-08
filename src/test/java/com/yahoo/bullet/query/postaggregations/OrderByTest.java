/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.querying.postaggregations.OrderByStrategy;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class OrderByTest {
    @Test
    public void testOrderBy() {
        OrderBy orderBy = new OrderBy(Arrays.asList(new OrderBy.SortItem(new FieldExpression("1"), OrderBy.Direction.ASC),
                                                    new OrderBy.SortItem(new FieldExpression("2"), OrderBy.Direction.DESC)));

        Assert.assertEquals(orderBy.getFields().size(), 2);
        Assert.assertEquals(orderBy.getType(), PostAggregationType.ORDER_BY);
        Assert.assertTrue(orderBy.getPostStrategy() instanceof OrderByStrategy);
        Assert.assertEquals(orderBy.toString(), "{type: ORDER_BY, fields: [{expression: {field: 1, index: null, key: null, subKey: null, type: null}, direction: ASC}, {expression: {field: 2, index: null, key: null, subKey: null, type: null}, direction: DESC}]}");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullFields() {
        new OrderBy(null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The ORDER BY post-aggregation requires at least one field\\.")
    public void testConstructorMissingFields() {
        new OrderBy(Collections.emptyList());
    }
}
