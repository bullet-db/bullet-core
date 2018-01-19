/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.grouping;

import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.AVG;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.MAX;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.MIN;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.SUM;
import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.parsing.AggregationUtils.makeGroupOperation;

public class GroupOperationTest {

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
        Assert.assertNotEquals(d.hashCode(), e.hashCode());
    }

    @Test
    public void testNullFieldsForHashCode() {
        GroupOperation a = new GroupOperation(null, null, "a");
        GroupOperation b = new GroupOperation(null, "foo", "a");
        GroupOperation c = new GroupOperation(null, "bar", "a");
        GroupOperation d = new GroupOperation(null, null, "b");
        GroupOperation e = new GroupOperation(AVG, null, "avg");

        Assert.assertNotEquals(a.hashCode(), b.hashCode());
        Assert.assertNotEquals(a.hashCode(), c.hashCode());
        Assert.assertNotEquals(b.hashCode(), c.hashCode());
        Assert.assertEquals(a.hashCode(), d.hashCode());
        Assert.assertNotEquals(a.hashCode(), e.hashCode());
        Assert.assertNotEquals(b.hashCode(), e.hashCode());
        Assert.assertNotEquals(c.hashCode(), e.hashCode());
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
    public void testNullFieldsForEquals() {
        GroupOperation a = new GroupOperation(null, null, "a");
        GroupOperation b = new GroupOperation(null, "foo", "a");
        GroupOperation c = new GroupOperation(null, "bar", "a");
        GroupOperation d = new GroupOperation(null, null, "a");

        Assert.assertNotEquals(a, b);
        Assert.assertNotEquals(a, c);
        Assert.assertNotEquals(b, c);
        Assert.assertEquals(a, d);
    }

    @Test
    public void testHavingGroupOperations() {
        Assert.assertFalse(GroupOperation.hasOperations(null));
        Assert.assertFalse(GroupOperation.hasOperations(Collections.emptyMap()));

        Map<String, Object> attributes = new HashMap<>();

        attributes.put(GroupOperation.OPERATIONS, null);
        Assert.assertFalse(GroupOperation.hasOperations(attributes));

        attributes.put(GroupOperation.OPERATIONS, new HashMap<>());
        Assert.assertTrue(GroupOperation.hasOperations(attributes));
    }

    @Test
    public void testCheckingGroupOperations() {
        List<GroupOperation> list = new ArrayList<>();

        Assert.assertFalse(GroupOperation.checkOperations(list).isPresent());

        list.add(new GroupOperation(AVG, "foo", "avg1"));
        list.add(new GroupOperation(MIN, "foo", null));
        list.add(new GroupOperation(COUNT, null, null));

        Assert.assertFalse(GroupOperation.checkOperations(list).isPresent());

        list.add(new GroupOperation(SUM, null, null));
        list.add(new GroupOperation(AVG, null, "foo"));

        Optional<List<BulletError>> optionalErrors = GroupOperation.checkOperations(list);
        Assert.assertTrue(optionalErrors.isPresent());
        List<BulletError> errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 2);
        Assert.assertEquals(errors.get(0),
                            BulletError.makeError(GroupOperation.GROUP_OPERATION_REQUIRES_FIELD + SUM,
                                                  GroupOperation.OPERATION_REQUIRES_FIELD_RESOLUTION));
        Assert.assertEquals(errors.get(1),
                            BulletError.makeError(GroupOperation.GROUP_OPERATION_REQUIRES_FIELD + AVG,
                                                  GroupOperation.OPERATION_REQUIRES_FIELD_RESOLUTION));
    }

    @Test
    public void testGettingGroupOperations() {
        Assert.assertEquals(GroupOperation.getOperations(null).size(), 0);
        Assert.assertEquals(GroupOperation.getOperations(Collections.emptyMap()).size(), 0);
        Assert.assertEquals(GroupOperation.getOperations(null).size(), 0);

        Map<String, Object> attributes = Collections.singletonMap(GroupOperation.OPERATIONS, null);

        Assert.assertEquals(GroupOperation.getOperations(attributes).size(), 0);

        attributes = makeAttributes(makeGroupOperation(SUM, "foo", null),
                                    makeGroupOperation(AVG, "bar", "baz"),
                                    makeGroupOperation(COUNT, null, null),
                                    makeGroupOperation(MIN, "qux", null),
                                    makeGroupOperation(MAX, "norf", "quux"));


        Set<GroupOperation> actual = GroupOperation.getOperations(attributes);

        List<GroupOperation> expected = Arrays.asList(new GroupOperation(SUM, "foo", null),
                                                      new GroupOperation(AVG, "bar", "baz"),
                                                      new GroupOperation(COUNT, null, null),
                                                      new GroupOperation(MIN, "qux", null),
                                                      new GroupOperation(MAX, "norf", "quux"));

        Assert.assertEquals(actual.size(), expected.size());
        Assert.assertTrue(actual.containsAll(expected));
    }

    @Test
    public void testFailGettingGroupOperationsBadTypes() {
        Map<String, Object> attributes = new HashMap<>();

        attributes.put(GroupOperation.OPERATIONS, 1L);
        Assert.assertEquals(GroupOperation.getOperations(attributes).size(), 0);

        attributes.put(GroupOperation.OPERATIONS, new HashMap<>());
        Assert.assertEquals(GroupOperation.getOperations(attributes).size(), 0);

        attributes.put(GroupOperation.OPERATIONS, Collections.singletonList(null));
        Assert.assertEquals(GroupOperation.getOperations(attributes).size(), 0);

        List<Object> badOperations = new ArrayList<>();
        badOperations.add(1L);
        badOperations.add(2L);
        attributes.put(GroupOperation.OPERATIONS, badOperations);
        Assert.assertEquals(GroupOperation.getOperations(attributes).size(), 0);
    }
}
