/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.grouping;

import org.testng.Assert;
import org.testng.annotations.Test;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.AVG;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;

public class GroupOperationTest {
    @Test
    public void testCustomGroupOperator() {
        GroupOperation.GroupOperator product = (a, b) -> a != null && b != null ? a.doubleValue() * b.doubleValue() : null;
        Assert.assertNull(product.apply(6L, null));
        Assert.assertEquals(product.apply(6L, 2L).longValue(), 12L);
        Assert.assertEquals(product.apply(6.1, 2L).doubleValue(), 12.2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMinUnsupported() {
        GroupOperation.MIN.apply(null, 2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMaxUnsupported() {
        GroupOperation.MAX.apply(null, 2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCountUnsupported() {
        GroupOperation.COUNT.apply(null, 2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testSumUnsupported() {
        GroupOperation.MAX.apply(null, 2);
    }

    @Test
    public void testMin() {
        Assert.assertEquals(GroupOperation.MIN.apply(1, 2).intValue(), 1);
        Assert.assertEquals(GroupOperation.MIN.apply(2.1, 1.2).doubleValue(), 1.2);
        Assert.assertEquals(GroupOperation.MIN.apply(1.0, 1.0).doubleValue(), 1.0);
    }

    @Test
    public void testMax() {
        Assert.assertEquals(GroupOperation.MAX.apply(1, 2).intValue(), 2);
        Assert.assertEquals(GroupOperation.MAX.apply(2.1, 1.2).doubleValue(), 2.1);
        Assert.assertEquals(GroupOperation.MAX.apply(1.0, 1.0).doubleValue(), 1.0);
    }

    @Test
    public void testSum() {
        Assert.assertEquals(GroupOperation.SUM.apply(1, 2).intValue(), 3);
        Assert.assertEquals(GroupOperation.SUM.apply(2.1, 1.2).doubleValue(), 3.3);
        Assert.assertEquals(GroupOperation.SUM.apply(2.0, 41).longValue(), 43L);
    }

    @Test
    public void testCount() {
        Assert.assertEquals(GroupOperation.COUNT.apply(1, 2).intValue(), 3);
        Assert.assertEquals(GroupOperation.COUNT.apply(2.1, 1.2).doubleValue(), 3.0);
        Assert.assertEquals(GroupOperation.COUNT.apply(1.0, 41).longValue(), 42L);
    }
    @Test
    public void testHashCode() {
        GroupOperation a = new GroupOperation(AVG, "foo", "avg1");
        GroupOperation b = new GroupOperation(AVG, "foo", "avg2");
        GroupOperation c = new GroupOperation(AVG, "bar", "avg1");
        GroupOperation d = new GroupOperation(COUNT, "foo", "count1");
        GroupOperation e = new GroupOperation(COUNT, "bar", "count2");

        Assert.assertEquals(a.hashCode(), b.hashCode());
        Assert.assertNotEquals(a.hashCode(), c.hashCode());
        Assert.assertNotEquals(a.hashCode(), d.hashCode());
        Assert.assertNotEquals(c.hashCode(), d.hashCode());
        Assert.assertNotEquals(a.hashCode(), e.hashCode());
        Assert.assertNotEquals(c.hashCode(), e.hashCode());
        Assert.assertEquals(d.hashCode(), e.hashCode());
    }

    @Test
    public void testEquals() {
        GroupOperation a = new GroupOperation(AVG, "foo", "avg1");
        GroupOperation b = new GroupOperation(AVG, "foo", "avg2");
        GroupOperation c = new GroupOperation(AVG, "bar", "avg");
        GroupOperation d = new GroupOperation(COUNT, "foo", "count");
        String e = "foo";

        Assert.assertEquals(a, b);
        Assert.assertNotEquals(a, c);
        Assert.assertNotEquals(a, d);
        Assert.assertNotEquals(c, d);

        Assert.assertFalse(a.equals(e));
    }

    @Test
    public void testToString() {
        GroupOperation a = new GroupOperation(AVG, "foo", "avg");
        GroupOperation b = new GroupOperation(COUNT, "foo", "count");

        Assert.assertEquals(a.toString(), "{type: AVG, field: foo, name: avg}");
        Assert.assertEquals(b.toString(), "{type: COUNT, field: null, name: count}");
    }
}
