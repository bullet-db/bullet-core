/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.querying.FilterOperations;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class FilterOperationsTest {
    @Test
    public void testComparatorUnsupportedType() {
        TypedObject object = new TypedObject(Type.MAP, singletonMap("foo", "bar"));
        // foo cannot be casted to map, so eq will return false (values will be empty)
        Assert.assertFalse(FilterOperations.EQ.compare(object, singletonList("foo")));
        // foo cannot be casted to map, so neq will return true (values will be empty)
        Assert.assertTrue(FilterOperations.NE.compare(object, singletonList("foo")));
        Assert.assertFalse(FilterOperations.GT.compare(object, singletonList("foo")));
        Assert.assertFalse(FilterOperations.GE.compare(object, singletonList("foo")));
        Assert.assertFalse(FilterOperations.LT.compare(object, singletonList("foo")));
        Assert.assertFalse(FilterOperations.LE.compare(object, singletonList("foo")));
        Assert.assertFalse(FilterOperations.RLIKE.compare(object, singletonList(Pattern.compile("foo"))));
    }

    @Test
    public void testOnDoubles() {
        TypedObject object = new TypedObject(Double.valueOf("1.234"));
        Assert.assertTrue(FilterOperations.EQ.compare(object, Arrays.asList("1.234", "4.343", "foo")));
        Assert.assertFalse(FilterOperations.EQ.compare(object, singletonList("4.343")));
    }

    @Test
    public void testComparatorUncastable() {
        TypedObject object = new TypedObject(Double.valueOf("1.234"));
        Assert.assertFalse(FilterOperations.EQ.compare(object, singletonList("foo")));
    }

    @Test
    public void testNulls() {
        TypedObject object = new TypedObject(Type.NULL, null);
        Assert.assertFalse(FilterOperations.EQ.compare(object, singletonList("foo")));
        Assert.assertTrue(FilterOperations.EQ.compare(object, singletonList("null")));
        Assert.assertTrue(FilterOperations.EQ.compare(object, singletonList("NULL")));
        Assert.assertTrue(FilterOperations.EQ.compare(object, singletonList("Null")));
        Assert.assertTrue(FilterOperations.NE.compare(object, singletonList("foo")));
        Assert.assertFalse(FilterOperations.NE.compare(object, singletonList("null")));
        Assert.assertFalse(FilterOperations.NE.compare(object, singletonList("NULL")));
        Assert.assertFalse(FilterOperations.NE.compare(object, singletonList("Null")));
        Assert.assertFalse(FilterOperations.GT.compare(object, singletonList("foo")));
        Assert.assertFalse(FilterOperations.LT.compare(object, singletonList("foo")));
        Assert.assertFalse(FilterOperations.GE.compare(object, singletonList("foo")));
        Assert.assertFalse(FilterOperations.LE.compare(object, singletonList("foo")));
        Assert.assertFalse(FilterOperations.RLIKE.compare(object, singletonList(Pattern.compile("foo"))));
        Assert.assertFalse(FilterOperations.RLIKE.compare(object, singletonList(Pattern.compile("null"))));
        Assert.assertFalse(FilterOperations.RLIKE.compare(object, singletonList(Pattern.compile("nu.*"))));
    }

    @Test
    public void testMixedTypes() {
        TypedObject object = new TypedObject(2.34);
        Assert.assertTrue(FilterOperations.EQ.compare(object, Arrays.asList("foo", "2.34")));
        Assert.assertFalse(FilterOperations.EQ.compare(object, Arrays.asList("foo", "3.42")));

        Assert.assertTrue(FilterOperations.NE.compare(object, Arrays.asList("baz", "bar")));
        Assert.assertFalse(FilterOperations.NE.compare(object, Arrays.asList("baz", "2.34")));

        Assert.assertTrue(FilterOperations.GT.compare(object, Arrays.asList("baz", "2.1")));
        Assert.assertFalse(FilterOperations.GT.compare(object, Arrays.asList("baz", "2.34")));
        Assert.assertFalse(FilterOperations.GT.compare(object, Arrays.asList("baz", "2.4")));

        Assert.assertFalse(FilterOperations.LT.compare(object, Arrays.asList("baz", "2.1")));
        Assert.assertFalse(FilterOperations.LT.compare(object, Arrays.asList("baz", "2.34")));
        Assert.assertTrue(FilterOperations.LT.compare(object, Arrays.asList("baz", "2.4")));

        Assert.assertTrue(FilterOperations.GE.compare(object, Arrays.asList("baz", "2.1")));
        Assert.assertTrue(FilterOperations.GE.compare(object, Arrays.asList("baz", "2.34")));
        Assert.assertFalse(FilterOperations.GE.compare(object, Arrays.asList("baz", "2.4")));

        Assert.assertFalse(FilterOperations.LE.compare(object, Arrays.asList("baz", "2.1")));
        Assert.assertTrue(FilterOperations.LE.compare(object, Arrays.asList("baz", "2.34")));
        Assert.assertTrue(FilterOperations.LE.compare(object, Arrays.asList("baz", "2.4")));
    }

    @Test
    public void testEquality() {
        TypedObject object = new TypedObject("foo");
        Assert.assertTrue(FilterOperations.EQ.compare(object, Arrays.asList("foo", "bar")));
        Assert.assertFalse(FilterOperations.EQ.compare(object, Arrays.asList("baz", "bar")));
        // Will become a string
        object = new TypedObject(singletonList("foo"));
        Assert.assertFalse(FilterOperations.EQ.compare(object, Arrays.asList("foo", "bar")));
        // Can't be casted to a list, so the equality check will fail
        object = new TypedObject(Type.LIST, singletonList("foo"));
        Assert.assertFalse(FilterOperations.EQ.compare(object, Arrays.asList("foo", "bar")));
    }

    @Test
    public void testInEquality() {
        TypedObject object = new TypedObject(1L);
        Assert.assertFalse(FilterOperations.NE.compare(object, Arrays.asList("1", "2")));
        Assert.assertTrue(FilterOperations.NE.compare(object, Arrays.asList("2", "3")));
        // Will become a string
        object = new TypedObject(singletonList("1"));
        Assert.assertTrue(FilterOperations.NE.compare(object, Arrays.asList("1", "2")));
        // Can't be casted to a list, so the inequality check will pass
        object = new TypedObject(Type.LIST, singletonList("foo"));
        Assert.assertTrue(FilterOperations.NE.compare(object, Arrays.asList("foo", "bar")));
    }

    @Test
    public void testGreaterNumeric() {
        TypedObject object = new TypedObject(1L);
        Assert.assertTrue(FilterOperations.GT.compare(object, Arrays.asList("0", "2")));
        Assert.assertFalse(FilterOperations.GT.compare(object, Arrays.asList("1", "2")));
        Assert.assertTrue(FilterOperations.GE.compare(object, Arrays.asList("0", "3")));
        Assert.assertFalse(FilterOperations.GE.compare(object, Arrays.asList("2", "3")));

        // Will become UNKNOWN
        object = new TypedObject(singletonList("1"));
        Assert.assertFalse(FilterOperations.GT.compare(object, Arrays.asList("1", "2")));
        Assert.assertFalse(FilterOperations.GE.compare(object, Arrays.asList("1", "2")));

        // Will become UNKNOWN
        object = new TypedObject(Type.LIST, singletonList("1"));
        Assert.assertFalse(FilterOperations.GT.compare(object, Arrays.asList("1", "2")));
        Assert.assertFalse(FilterOperations.GE.compare(object, Arrays.asList("1", "2")));
    }

    @Test
    public void testLessNumeric() {
        TypedObject object = new TypedObject(1L);
        Assert.assertTrue(FilterOperations.LT.compare(object, Arrays.asList("-10", "2")));
        Assert.assertFalse(FilterOperations.LT.compare(object, Arrays.asList("-1", "0")));
        Assert.assertTrue(FilterOperations.LE.compare(object, Arrays.asList("0", "1")));
        Assert.assertFalse(FilterOperations.LE.compare(object, Arrays.asList("-2", "-3")));

        // Will become a string
        object = new TypedObject(singletonList("1"));
        Assert.assertFalse(FilterOperations.LT.compare(object, Arrays.asList("1", "2")));
        Assert.assertFalse(FilterOperations.LE.compare(object, Arrays.asList("1", "2")));
        // Can't be casted to a list, so the less check will fail
        object = new TypedObject(Type.LIST, singletonList("1"));
        Assert.assertFalse(FilterOperations.LT.compare(object, Arrays.asList("1", "2")));
        Assert.assertFalse(FilterOperations.LE.compare(object, Arrays.asList("1", "2")));
    }

    @Test
    public void testGreaterString() {
        TypedObject object = new TypedObject("foo");
        Assert.assertTrue(FilterOperations.GT.compare(object, Arrays.asList("bravo", "2")));
        Assert.assertFalse(FilterOperations.GT.compare(object, Arrays.asList("zulu", "xray")));
        Assert.assertTrue(FilterOperations.GE.compare(object, Arrays.asList("alpha", "foo")));
        Assert.assertFalse(FilterOperations.GE.compare(object, Arrays.asList("golf", "november")));
    }

    @Test
    public void testLessString() {
        TypedObject object = new TypedObject("foo");
        Assert.assertFalse(FilterOperations.LT.compare(object, Arrays.asList("bravo", "2")));
        Assert.assertTrue(FilterOperations.LT.compare(object, Arrays.asList("zulu", "xray")));
        Assert.assertTrue(FilterOperations.LE.compare(object, Arrays.asList("oscar", "foo")));
        Assert.assertFalse(FilterOperations.LE.compare(object, Arrays.asList("echo", "fi")));
    }

    @Test
    public void testRegexMatching() {
        List<Pattern> pattern = Stream.of(".g.", ".*foo.*").map(Pattern::compile).collect(Collectors.toList());
        Assert.assertTrue(FilterOperations.RLIKE.compare(new TypedObject("foo"), pattern));
        Assert.assertTrue(FilterOperations.RLIKE.compare(new TypedObject("food"), pattern));
        Assert.assertTrue(FilterOperations.RLIKE.compare(new TypedObject("egg"), pattern));
        Assert.assertFalse(FilterOperations.RLIKE.compare(new TypedObject("g"), pattern));
        Assert.assertFalse(FilterOperations.RLIKE.compare(new TypedObject("fgoo"), pattern));
        Assert.assertFalse(FilterOperations.RLIKE.compare(new TypedObject(Type.NULL, null), pattern));
    }
}
