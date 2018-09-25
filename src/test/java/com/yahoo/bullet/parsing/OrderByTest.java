/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class OrderByTest {
    @Test
    public void testToString() {
        OrderBy aggregation = new OrderBy();
        aggregation.setType(PostAggregation.Type.ORDER_BY);
        Assert.assertEquals(aggregation.toString(), "{type: ORDER_BY, fields: null}");

        aggregation.setFields(Arrays.asList(new OrderBy.SortItem("1", OrderBy.Direction.ASC), new OrderBy.SortItem("2", OrderBy.Direction.DESC)));
        Assert.assertEquals(aggregation.toString(), "{type: ORDER_BY, fields: [{field: 1, direction: ASC}, {field: 2, direction: DESC}]}");
    }

    @Test
    public void testInitializeWithoutType() {
        OrderBy orderBy = new OrderBy();
        Optional<List<BulletError>> errors = orderBy.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), PostAggregation.TYPE_MISSING);
    }

    @Test
    public void testInitializeWithoutSortItems() {
        OrderBy orderBy = new OrderBy();
        orderBy.setType(PostAggregation.Type.ORDER_BY);
        Optional<List<BulletError>> errors = orderBy.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), OrderBy.ORDERBY_REQUIRES_FIELDS_ERROR);
    }

    @Test
    public void testInitializeWithEmptySortItems() {
        OrderBy orderBy = new OrderBy();
        orderBy.setType(PostAggregation.Type.ORDER_BY);
        orderBy.setFields(Collections.emptyList());
        Optional<List<BulletError>> errors = orderBy.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), OrderBy.ORDERBY_REQUIRES_FIELDS_ERROR);
    }

    @Test
    public void testInitializeWithEmptyFields() {
        OrderBy orderBy = new OrderBy();
        orderBy.setType(PostAggregation.Type.ORDER_BY);
        orderBy.setFields(Collections.singletonList(new OrderBy.SortItem()));
        Optional<List<BulletError>> errors = orderBy.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), OrderBy.ORDERBY_REQUIRES_NON_EMPTY_FIELDS_ERROR);
    }

    @Test
    public void testInitialize() {
        OrderBy orderBy = new OrderBy();
        orderBy.setType(PostAggregation.Type.ORDER_BY);
        orderBy.setFields(Arrays.asList(new OrderBy.SortItem("a", OrderBy.Direction.ASC), new OrderBy.SortItem("b", OrderBy.Direction.DESC)));
        Optional<List<BulletError>> errors = orderBy.initialize();
        Assert.assertFalse(errors.isPresent());
        Assert.assertEquals(orderBy.getFields().size(), 2);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "a");
        Assert.assertEquals(orderBy.getFields().get(0).getDirection(), OrderBy.Direction.ASC);
        Assert.assertEquals(orderBy.getFields().get(1).getField(), "b");
        Assert.assertEquals(orderBy.getFields().get(1).getDirection(), OrderBy.Direction.DESC);
    }
}
