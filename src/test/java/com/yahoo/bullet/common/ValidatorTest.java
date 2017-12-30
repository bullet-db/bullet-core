/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import com.yahoo.bullet.common.Validator.Entry;
import com.yahoo.bullet.common.Validator.Relationship;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class ValidatorTest {
    private BulletConfig empty;

    @BeforeMethod
    public void setup() {
        empty = new BulletConfig();
        empty.clear();
    }

    @Test
    public void testDefaultEntry() {
        Validator validator = new Validator();
        Entry entry = validator.define("foo");
        empty.set("foo", "bar");
        entry.normalize(empty);

        Assert.assertEquals(empty.get("foo"), "bar");
        Assert.assertNull(entry.getDefaultValue());
    }

    @Test
    public void testEntryDefaulting() {
        Validator validator = new Validator();
        Entry entry = validator.define("foo").checkIf(Validator::isNotNull).defaultTo("baz").castTo(Validator::asString);

        Assert.assertNull(empty.get("foo"));
        entry.normalize(empty);
        Assert.assertEquals(empty.get("foo"), "baz");
        Assert.assertEquals(entry.getDefaultValue(), "baz");

        // Check to see if type conversion is done for default of the non-final type.
        entry.defaultTo(1);
        Assert.assertEquals(entry.getDefaultValue(), "1");
    }

    @Test
    public void testEntryCasting() {
        Validator validator = new Validator();
        Entry entry = validator.define("foo").castTo(Validator::asInt);
        empty.set("foo", 1.035f);
        entry.normalize(empty);

        Assert.assertEquals(empty.get("foo").getClass(), Integer.class);
        Assert.assertEquals(empty.get("foo"), 1);
    }

    @Test
    public void testEntryMultipleChecks() {
        Validator validator = new Validator();
        Entry entry = validator.define("foo")
                               .checkIf(Validator::isNotNull)
                               .checkIf(Validator.isIn("baz", "bar"))
                               .defaultTo("qux");

        Assert.assertNull(empty.get("foo"));

        entry.normalize(empty);
        Assert.assertEquals(empty.get("foo"), "qux");

        empty.set("foo", "baz");
        entry.normalize(empty);
        Assert.assertEquals(empty.get("foo"), "baz");

        empty.set("foo", "bar");
        entry.normalize(empty);
        Assert.assertEquals(empty.get("foo"), "bar");

        empty.set("foo", "foo");
        entry.normalize(empty);
        Assert.assertEquals(empty.get("foo"), "qux");
    }

    @Test
    public void testIsNotNull() {
        Assert.assertTrue(Validator.isNotNull("foo"));
        Assert.assertFalse(Validator.isNotNull(null));
    }

    @Test
    public void testisType() {
        Assert.assertTrue(Validator.isType("foo", String.class));
        Assert.assertTrue(Validator.isType(1, Integer.class));
        Assert.assertTrue(Validator.isType(1L, Long.class));
        Assert.assertTrue(Validator.isType(asList("foo", "bar"), List.class));
        Assert.assertFalse(Validator.isType(0, Float.class));
    }

    @Test
    public void testIsBoolean() {
        Assert.assertTrue(Validator.isBoolean(true));
        Assert.assertTrue(Validator.isBoolean(false));
        Assert.assertFalse(Validator.isBoolean("foo"));
    }

    @Test
    public void testIsString() {
        Assert.assertTrue(Validator.isString("foo"));
        Assert.assertFalse(Validator.isString(null));
        Assert.assertFalse(Validator.isString(1.0));
    }

    @Test
    public void testIsFloat() {
        Assert.assertTrue(Validator.isFloat(1.34));
        Assert.assertTrue(Validator.isFloat(1.34f));
        Assert.assertFalse(Validator.isFloat(2));
    }

    @Test
    public void testIsInt() {
        Assert.assertTrue(Validator.isInt(1));
        Assert.assertTrue(Validator.isInt(3L));
        Assert.assertFalse(Validator.isInt(2.3));
    }

    @Test
    public void testIsNumber() {
        Assert.assertTrue(Validator.isNumber(1.3));
        Assert.assertTrue(Validator.isNumber(1));
        Assert.assertTrue(Validator.isNumber(42L));
        Assert.assertTrue(Validator.isNumber(4.2f));
        Assert.assertFalse(Validator.isNumber("foo"));
    }

    @Test
    public void testIsPositive() {
        Assert.assertTrue(Validator.isPositive(2.4));
        Assert.assertTrue(Validator.isPositive(2));
        Assert.assertTrue(Validator.isPositive(0.02));
        Assert.assertFalse(Validator.isPositive(-0.3));
    }

    @Test
    public void testIsPositiveInt() {
        Assert.assertTrue(Validator.isPositiveInt(1));
        Assert.assertTrue(Validator.isPositiveInt(2L));
        Assert.assertFalse(Validator.isPositiveInt(0.3));
        Assert.assertFalse(Validator.isPositiveInt(0L));
        Assert.assertFalse(Validator.isPositiveInt(0));
        Assert.assertFalse(Validator.isPositiveInt(-10));
        Assert.assertFalse(Validator.isPositiveInt(-10.3));
    }

    @Test
    public void testIsPowerOfTwo() {
        Assert.assertTrue(Validator.isPowerOfTwo(1));
        Assert.assertTrue(Validator.isPowerOfTwo(2));
        Assert.assertTrue(Validator.isPowerOfTwo(4));
        Assert.assertTrue(Validator.isPowerOfTwo(16384));
        Assert.assertFalse(Validator.isPowerOfTwo(3));
        Assert.assertFalse(Validator.isPowerOfTwo(2.0));
        Assert.assertFalse(Validator.isPowerOfTwo(-4));
    }

    @Test
    public void testIsMap() {
        Assert.assertTrue(Validator.isMap(Collections.singletonMap("foo", "bar")));
        Assert.assertFalse(Validator.isMap("foo"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testIsIn() {
        Assert.assertTrue(Validator.isIn("foo", "bar", "baz", "qux").test("foo"));
        Assert.assertTrue(Validator.isIn("foo", "bar", "baz", "qux").test("qux"));
        Assert.assertFalse(Validator.isIn("foo", "bar", "baz", "qux").test("f"));
        Assert.assertFalse(Validator.isIn("foo", "bar", "baz", "qux").test(1.0));

        Assert.assertTrue(Validator.isIn(asList("foo", "bar"), asList("baz", "qux")).test(asList("foo", "bar")));
        Assert.assertTrue(Validator.isIn(asList("foo", "bar"), asList("baz", "qux")).test(asList("baz", "qux")));
        Assert.assertFalse(Validator.isIn(asList("foo", "bar"), asList("baz", "qux")).test(singletonList("baz")));
        Assert.assertFalse(Validator.isIn(asList("foo", "bar"), asList("baz", "qux")).test(singletonList("foo")));
        Assert.assertFalse(Validator.isIn(asList("foo", "bar"), asList("baz", "qux")).test(null));
        Assert.assertFalse(Validator.isIn(asList("foo", "bar"), asList("baz", "qux")).test("foo"));
    }

    @Test
    public void testIsInRange() {
        Assert.assertTrue(Validator.isInRange(-1, 2.0).test(-1));
        Assert.assertTrue(Validator.isInRange(-1, 2.0).test(0));
        Assert.assertTrue(Validator.isInRange(-1, 2.0).test(2.0));
        Assert.assertTrue(Validator.isInRange(-1.0, 2.0).test(0.1));
        Assert.assertTrue(Validator.isInRange(-1.0, 2.0).test(0.0f));
        Assert.assertTrue(Validator.isInRange(-1.0, 2.0).test(1L));
        Assert.assertFalse(Validator.isInRange(-1, 2.0).test("0"));
        Assert.assertFalse(Validator.isInRange(-1, 2.0).test(-1.1));
        Assert.assertFalse(Validator.isInRange(-1, 2.0).test(2.1));
        Assert.assertFalse(Validator.isInRange(-1, 2.0).test(null));
    }

    @Test
    public void testIntegerConversion() {
        Assert.assertEquals(Validator.asInt(3.4), 3);
        Assert.assertEquals(Validator.asInt(3), 3);
        Assert.assertEquals(Validator.asInt(3L), 3);
        Assert.assertEquals(Validator.asInt(3.4f), 3);
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testIntegerConversionFailure() {
        Validator.asInt("foo");
    }

    @Test
    public void testFloatConversion() {
        Assert.assertEquals(Validator.asFloat(3.0), 3.0f);
        Assert.assertEquals(Validator.asFloat(3), 3.0f);
        Assert.assertEquals(Validator.asFloat(3L), 3.0f);
        Assert.assertEquals(Validator.asFloat(3.0f), 3.0f);
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testFloatConversionFailure() {
        Validator.asFloat("foo");
    }

    @Test
    public void testDoubleConversion() {
        Assert.assertEquals(Validator.asDouble(3.0), 3.0);
        Assert.assertEquals(Validator.asDouble(3), 3.0);
        Assert.assertEquals(Validator.asDouble(3L), 3.0);
        Assert.assertEquals(Validator.asDouble(3.0f), 3.0);
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testDoubleConversionFailure() {
        Validator.asDouble("foo");
    }

    @Test
    public void testStringConversion() {
        Assert.assertEquals(Validator.asString("foo"), "foo");
        Assert.assertEquals(Validator.asString(1.0), "1.0");
        Assert.assertEquals(Validator.asString(asList("foo", "bar")), asList("foo", "bar").toString());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testStringConversionFailure() {
        Validator.asString(null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testRelationshipWithoutEntry() {
        Validator validator = new Validator();
        validator.relate("Won't be created", "foo", "bar");
    }

    @Test
    public void testRelationshipDefaulting() {
        Validator validator = new Validator();
        validator.define("foo");
        validator.define("bar");

        Relationship relation = validator.relate("Test", "foo", "bar");

        // Nothing happens
        empty.set("foo", 0.2);
        empty.set("bar", "baz");
        relation.normalize(empty);
        Assert.assertEquals(empty.get("foo"), 0.2);
        Assert.assertEquals(empty.get("bar"), "baz");
    }

    @Test
    public void testRelationshipFailureUsingEntryDefaults() {
        Validator validator = new Validator();
        validator.define("foo").defaultTo(0);
        validator.define("bar").defaultTo(42);

        Relationship relation = validator.relate("Test", "foo", "bar").checkIf(Validator::isGreaterOrEqual);

        empty.set("foo", -1L);
        empty.set("bar", 0.2);
        relation.normalize(empty);
        Assert.assertEquals(empty.get("foo"), 0);
        Assert.assertEquals(empty.get("bar"), 42);

        empty.set("foo", 42);
        empty.set("bar", 4.2);
        relation.normalize(empty);
        Assert.assertEquals(empty.get("foo"), 42);
        Assert.assertEquals(empty.get("bar"), 4.2);
    }

    @Test
    public void testRelationshipWithMultipleChecks() {
        Validator validator = new Validator();
        validator.define("foo").defaultTo(42);
        validator.define("bar").defaultTo(11).castTo(Validator::asDouble);

        Relationship relation = validator.relate("Test", "foo", "bar").checkIf(Validator::isGreaterOrEqual)
                                         .checkIf((a, b) -> ((Number) b).intValue() > 10);

        empty.set("foo", -1L);
        empty.set("bar", 0.2);
        relation.normalize(empty);
        Assert.assertEquals(empty.get("foo"), 42);
        Assert.assertEquals(empty.get("bar"), 11.0);

        empty.set("foo", 42);
        empty.set("bar", 4.2);
        relation.normalize(empty);
        Assert.assertEquals(empty.get("foo"), 42);
        Assert.assertEquals(empty.get("bar"), 11.0);

        empty.set("foo", 42);
        empty.set("bar", 40.3);
        relation.normalize(empty);
        Assert.assertEquals(empty.get("foo"), 42);
        Assert.assertEquals(empty.get("bar"), 40.3);
    }

    @Test
    public void testRelationshipFailureUsesCustomDefaults() {
        Validator validator = new Validator();
        validator.define("foo").defaultTo(0);
        validator.define("bar").defaultTo(42);

        Relationship relation = validator.relate("Test", "foo", "bar").checkIf(Validator::isGreaterOrEqual);
        relation.orElseUse("qux", "norf");

        empty.set("foo", 42);
        empty.set("bar", 4.2);
        relation.normalize(empty);
        Assert.assertEquals(empty.get("foo"), 42);
        Assert.assertEquals(empty.get("bar"), 4.2);

        empty.set("foo", -1L);
        empty.set("bar", 0.2);
        relation.normalize(empty);
        Assert.assertEquals(empty.get("foo"), "qux");
        Assert.assertEquals(empty.get("bar"), "norf");
    }
}
