/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.typesystem;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class TypeTest {
    @Test
    public void testCurrentTypes() {
        Assert.assertEquals(new HashSet<>(Type.PRIMITIVES),
                            new HashSet<>(Arrays.asList(Type.INTEGER, Type.LONG, Type.BOOLEAN, Type.DOUBLE, Type.STRING)));
        Assert.assertEquals(new HashSet<>(Type.NUMERICS),
                            new HashSet<>(Arrays.asList(Type.INTEGER, Type.LONG, Type.DOUBLE)));
    }

    @Test
    public void testTypeGetting() {
        Assert.assertEquals(Type.getType(null), Type.NULL);
        Assert.assertEquals(Type.getType(true), Type.BOOLEAN);
        Assert.assertEquals(Type.getType("foo"), Type.STRING);
        Assert.assertEquals(Type.getType(1L), Type.LONG);
        Assert.assertEquals(Type.getType(1.2), Type.DOUBLE);
        Assert.assertEquals(Type.getType(1), Type.INTEGER);
        Assert.assertEquals(Type.getType(3.14F), Type.UNKNOWN);
        Assert.assertEquals(Type.getType(new HashSet<String>()), Type.UNKNOWN);
    }

    @Test
    public void testBooleanCasting() {
        Assert.assertEquals(Type.BOOLEAN.cast("true"), true);
        Assert.assertEquals(Type.BOOLEAN.cast("false"), false);
        Assert.assertEquals(Type.BOOLEAN.cast("foo"), false);
        Assert.assertEquals(Type.BOOLEAN.cast("1"), false);
    }

    @Test
    public void testNullCasting() {
        Assert.assertEquals(Type.NULL.cast("null"), null);
        Assert.assertEquals(Type.NULL.cast("NULL"), null);
        Assert.assertEquals(Type.NULL.cast("Null"), null);
        Assert.assertEquals(Type.NULL.cast("false"), "false");
        Assert.assertEquals(Type.NULL.cast("42"), "42");
    }

    @Test
    public void testStringCasting() {
        Assert.assertEquals(Type.STRING.cast("1"), "1");
        Assert.assertEquals(Type.STRING.cast("foo"), "foo");
        Assert.assertEquals(Type.STRING.cast("true"), "true");
        Assert.assertEquals(Type.STRING.cast("1.23"), "1.23");
    }

    @Test
    public void testUnknownCasting() {
        Assert.assertEquals(Type.UNKNOWN.cast("1"), "1");
        Assert.assertEquals(Type.UNKNOWN.cast("{}"), "{}");
        Assert.assertEquals(Type.UNKNOWN.cast("[]"), "[]");
        Assert.assertEquals(Type.UNKNOWN.cast("1.23"), "1.23");
    }

    @Test
    public void testLongCasting() {
        Assert.assertEquals(Type.LONG.cast("41"), 41L);
    }

    @Test(expectedExceptions = NumberFormatException.class)
    public void testLongFailCastingDouble() {
        Type.LONG.cast("41.99");
    }

    @Test(expectedExceptions = NumberFormatException.class)
    public void testLongFailCastingString() {
        Type.LONG.cast("foo");
    }

    @Test
    public void testDoubleCasting() {
        Assert.assertEquals(Type.DOUBLE.cast("42.0"), 42.0);
        Assert.assertEquals(Type.DOUBLE.cast("42"), 42.0);
    }

    @Test(expectedExceptions = NumberFormatException.class)
    public void testDoubleFailCastingString() {
        Type.DOUBLE.cast("foo");
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testListUnsupportedCasting() {
        Type.LIST.cast(Collections.emptyList().toString());
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testMapUnsupportedCasting() {
        Type.MAP.cast(Collections.emptyMap().toString());
    }

}
