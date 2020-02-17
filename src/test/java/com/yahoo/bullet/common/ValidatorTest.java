/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.common;

import com.yahoo.bullet.common.Validator.Entry;
import com.yahoo.bullet.common.Validator.Relationship;
import com.yahoo.bullet.common.Validator.State;
import com.yahoo.bullet.pubsub.MockPubSub;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

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
    public void testEntryUnless() {
        Validator validator = new Validator();
        // foo should be 1 or 2, defaults 1L and is casted to String UNLESS it is already "1"
        Entry entry = validator.define("foo")
                               .checkIf(Validator::isNotNull)
                               .checkIf(Validator.isIn(1L, 2L))
                               .defaultTo(1L)
                               .castTo(Objects::toString)
                               .unless("1"::equals);

        Assert.assertNull(empty.get("foo"));
        entry.normalize(empty);
        Assert.assertEquals(empty.get("foo"), "1");

        empty.set("foo", 1L);
        entry.normalize(empty);
        Assert.assertEquals(empty.get("foo"), "1");

        empty.set("foo", 2L);
        entry.normalize(empty);
        Assert.assertEquals(empty.get("foo"), "2");

        String original = "1";
        empty.set("foo", "1");
        entry.normalize(empty);
        Assert.assertEquals(empty.get("foo"), "1");
        // This is the SAME string
        Assert.assertTrue(empty.get("foo") == original);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testEntryFailing() {
        Validator validator = new Validator();
        Entry entry = validator.define("foo")
                               .checkIf(Validator::isNotNull)
                               .checkIf(Validator.isIn("baz", "bar"))
                               .orFail();
        empty.set("foo", "qux");
        entry.normalize(empty);
    }

    @Test
    public void testIsNotNull() {
        Assert.assertTrue(Validator.isNotNull("foo"));
        Assert.assertFalse(Validator.isNotNull(null));
    }

    @Test
    public void testIsTrue() {
        Assert.assertFalse(Validator.isTrue("foo"));
        Assert.assertFalse(Validator.isTrue(null));
        Assert.assertFalse(Validator.isTrue(false));
        Assert.assertTrue(Validator.isTrue(true));
    }

    @Test
    public void testIsFalse() {
        Assert.assertFalse(Validator.isFalse("foo"));
        Assert.assertFalse(Validator.isFalse(null));
        Assert.assertFalse(Validator.isFalse(true));
        Assert.assertTrue(Validator.isFalse(false));
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
    public void testIsNonEmptyList() {
        Assert.assertTrue(Validator.isNonEmptyList(asList(0)));
        Assert.assertTrue(Validator.isNonEmptyList(asList(0, 1)));
        Assert.assertTrue(Validator.isNonEmptyList(asList("string")));
        Assert.assertFalse(Validator.isNonEmptyList("string"));
        Assert.assertFalse(Validator.isNonEmptyList(null));
        Assert.assertFalse(Validator.isNonEmptyList(emptyList()));
    }

    @Test
    public void testIsMap() {
        Assert.assertTrue(Validator.isMap(singletonMap("foo", "bar")));
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
    public void testIsAtleastNTimes() {
        BiPredicate<Object, Object> isAtleastTwice = Validator.isAtleastNTimes(2.0);
        Assert.assertTrue(isAtleastTwice.test(2L, 1L));
        Assert.assertTrue(isAtleastTwice.test(3L, 1L));
        Assert.assertFalse(isAtleastTwice.test(0L, 1L));

        BiPredicate<Object, Object> isAtleastThrice = Validator.isAtleastNTimes(3.0);
        Assert.assertFalse(isAtleastThrice.test(2.0, 1L));
        Assert.assertTrue(isAtleastThrice.test(3.0, 1L));
        Assert.assertTrue(isAtleastThrice.test(4.0, 1L));
        Assert.assertFalse(isAtleastThrice.test(0.1, 1L));
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

    @Test(expectedExceptions = IllegalStateException.class)
    public void testRelationshipFailureFailsOut() {
        Validator validator = new Validator();
        validator.define("foo").defaultTo(0);
        validator.define("bar").defaultTo(42);

        Relationship relation = validator.relate("Test", "foo", "bar").checkIf(Validator::isGreaterOrEqual);
        relation.orFail();

        empty.set("foo", -1L);
        empty.set("bar", 0.2);
        relation.normalize(empty);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testStateWithoutEntry() {
        Validator validator = new Validator();
        validator.evaluate("Test", "foo", "bar", "baz");
    }

    @Test
    public void testStateDefaulting() {
        Validator validator = new Validator();

        validator.define("foo").defaultTo(42);
        validator.define("bar").defaultTo(11).castTo(Validator::asDouble);
        validator.define("baz").defaultTo(true).checkIf(Validator::isBoolean);

        validator.evaluate("Test", "foo", "bar", "baz").checkIf(o -> false);

        empty.set("foo", 22);
        empty.set("bar", -1.4);
        empty.set("baz", false);
        validator.validate(empty);

        Assert.assertEquals(empty.get("foo"), 42);
        Assert.assertEquals(empty.get("bar"), 11.0);
        Assert.assertEquals(empty.get("baz"), true);
    }

    @Test
    public void testStateMultipleChecks() {
        Validator validator = new Validator();

        validator.define("foo").defaultTo(42);
        validator.define("bar").defaultTo(11).castTo(Validator::asDouble);
        validator.define("baz").defaultTo(true).checkIf(Validator::isBoolean);

        validator.evaluate("Test", "foo", "bar", "baz")
                 .checkIf((o) -> o.get(0).equals("1") && o.get(1).equals(2.0) && o.get(2).equals(false))
                 .checkIf((o) -> Double.valueOf(o.get(0).toString()) >= 0.0);

        empty.set("foo", "1");
        empty.set("bar", 2.0);
        empty.set("baz", false);
        validator.validate(empty);

        Assert.assertEquals(empty.get("foo"), "1");
        Assert.assertEquals(empty.get("bar"), 2.0);
        Assert.assertEquals(empty.get("baz"), false);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testStateCheckFailureErrorsOut() {
        Validator validator = new Validator();

        validator.define("foo").defaultTo(42);
        validator.define("bar").defaultTo(11).castTo(Validator::asDouble);
        validator.define("baz").defaultTo(true).checkIf(Validator::isBoolean);

        validator.evaluate("Test", "foo", "bar", "baz")
                 .checkIf((o) -> false)
                 .orFail();

        empty.set("foo", 22);
        empty.set("bar", -1.4);
        empty.set("baz", false);
        validator.validate(empty);
    }

    @Test
    public void testCopyingPreservesOriginal() {
        Validator validator = new Validator();
        Set<Long> whitelist = new HashSet<>();

        validator.define("foo").checkIf(whitelist::contains).defaultTo(0L);

        empty.set("foo", -1L);
        validator.validate(empty);
        // Defaults to 0 since -1 is not in the whitelist
        Assert.assertEquals(empty.get("foo"), 0L);

        // Allow -1 through
        whitelist.add(-1L);

        empty.set("foo", -1L);
        validator.validate(empty);
        Assert.assertEquals(empty.get("foo"), -1L);

        Validator copy = validator.copy();

        empty.set("foo", 10L);
        copy.validate(empty);
        Assert.assertEquals(empty.get("foo"), 0L);

        empty.set("foo", -1L);
        copy.validate(empty);
        Assert.assertEquals(empty.get("foo"), -1L);

        // Whitelist 1 and -1
        whitelist.add(1L);

        // Add a new check to the original Entry
        Entry original = validator.getEntries().get("foo");
        original.checkIf((o) -> ((long) o > 0L));

        // Will default since -1 is not > 0
        empty.set("foo", -1L);
        validator.validate(empty);
        Assert.assertEquals(empty.get("foo"), 0L);

        // Will not default since it does not have the same check anymore
        empty.set("foo", -1L);
        copy.validate(empty);
        Assert.assertEquals(empty.get("foo"), -1L);
    }

    @Test
    public void testCopyingDoesNotDeepCopy() {
        Validator validator = new Validator();
        Set<Long> whitelist = new HashSet<>();

        validator.define("foo").checkIf(whitelist::contains).defaultTo(0);
        validator.define("bar").defaultTo(42);
        validator.define("baz").defaultTo(false);

        Relationship relation = validator.relate("Test", "foo", "bar").checkIf(Validator::isGreaterOrEqual);
        relation.orElseUse("qux", "norf");

        State state = validator.evaluate("Test", "foo", "bar", "baz").checkIf(o -> true);

        empty.set("foo", -1L);
        empty.set("bar", -1.4);
        empty.set("baz", true);
        validator.validate(empty);
        // Defaults to 0 since -1 is not in the whitelist
        Assert.assertEquals(empty.get("foo"), 0);
        Assert.assertEquals(empty.get("bar"), -1.4);
        Assert.assertEquals(empty.get("baz"), true);

        // Allow -1 through
        whitelist.add(-1L);

        empty.set("foo", -1L);
        empty.set("bar", -1.4);
        empty.set("baz", false);
        validator.validate(empty);
        Assert.assertEquals(empty.get("foo"), -1L);
        Assert.assertEquals(empty.get("bar"), -1.4);
        Assert.assertEquals(empty.get("baz"), false);

        Validator copy = validator.copy();

        // The copy also has the shallow copy of contains since -1 goes through
        empty.set("foo", -1L);
        empty.set("bar", -1.4);
        empty.set("baz", false);
        copy.validate(empty);
        Assert.assertEquals(empty.get("foo"), -1L);
        Assert.assertEquals(empty.get("bar"), -1.4);
        Assert.assertEquals(empty.get("baz"), false);

        // Removing the white
        whitelist.clear();
        copy.validate(empty);
        Assert.assertEquals(empty.get("foo"), 0);
        Assert.assertEquals(empty.get("bar"), -1.4);
        Assert.assertEquals(empty.get("baz"), false);

        //  Changing the state to fail, doesn't change the copy
        state.checkIf(o -> false);
        state.orFail();

        copy.validate(empty);
        Assert.assertEquals(empty.get("foo"), 0);
        Assert.assertEquals(empty.get("bar"), -1.4);
        Assert.assertEquals(empty.get("baz"), false);
    }

    @Test
    public void testIsListOfType() {
        Predicate<Object> stringChecker = Validator.isListOfType(String.class);

        Assert.assertFalse(stringChecker.test(null));
        Assert.assertFalse(stringChecker.test(emptyList()));
        Assert.assertTrue(stringChecker.test(singletonList("a")));
        Assert.assertTrue(stringChecker.test(asList("a", "b")));
        Assert.assertFalse(stringChecker.test(singletonList(1)));
        Assert.assertFalse(stringChecker.test(asList(1, 2)));
    }

    @Test
    public void testIsClassName() {
        Assert.assertTrue(Validator.isClassName(Validator.class.getName()));
        Assert.assertTrue(Validator.isClassName(MockPubSub.class.getName()));
        Assert.assertFalse(Validator.isClassName("fake.class.path.foo"));
        Assert.assertFalse(Validator.isClassName(null));
        Assert.assertFalse(Validator.isClassName(asList("foo", "bar")));
    }

    @Test
    public void testHasMinimumListSize() {
        Predicate<Object> hasThreeOrMore = Validator.hasMinimumListSize(3);
        Assert.assertFalse(hasThreeOrMore.test(null));
        Assert.assertFalse(hasThreeOrMore.test(emptyList()));
        Assert.assertFalse(hasThreeOrMore.test(singletonList("a")));
        Assert.assertFalse(hasThreeOrMore.test(asList("a", "b")));
        Assert.assertTrue(hasThreeOrMore.test(asList("a", "b", "c")));
        Assert.assertTrue(hasThreeOrMore.test(asList("a", "b", "c", "d")));

        Assert.assertTrue(hasThreeOrMore.test(asList(1, 2, 3, 4)));
        Assert.assertFalse(hasThreeOrMore.test(asList(1, 2)));

        Assert.assertTrue(Validator.hasMinimumListSize(0).test(emptyList()));
    }

    @Test
    public void testHasMaximumListSize() {
        Predicate<Object> hasThreeOrLess = Validator.hasMaximumListSize(3);
        Assert.assertFalse(hasThreeOrLess.test(null));
        Assert.assertTrue(hasThreeOrLess.test(emptyList()));
        Assert.assertTrue(hasThreeOrLess.test(singletonList("a")));
        Assert.assertTrue(hasThreeOrLess.test(asList("a", "b")));
        Assert.assertTrue(hasThreeOrLess.test(asList("a", "b", "c")));

        Assert.assertFalse(hasThreeOrLess.test(asList("a", "b", "c", "d")));
        Assert.assertTrue(hasThreeOrLess.test(asList(1, 2)));

        Assert.assertTrue(Validator.hasMaximumListSize(0).test(emptyList()));
    }

    @Test
    public void testIsImplied() {
        Assert.assertTrue(Validator.isImplied(true, true));
        Assert.assertTrue(Validator.isImplied(false, true));
        Assert.assertTrue(Validator.isImplied(false, false));
        Assert.assertFalse(Validator.isImplied(true, false));
    }

    @Test
    public void testANDing() {
        Predicate<Object> notNullAndNotEmptyList = Validator.and(Objects::nonNull, a -> !(((List) a).isEmpty()));
        Assert.assertFalse(notNullAndNotEmptyList.test(null));
        Assert.assertFalse(notNullAndNotEmptyList.test(emptyList()));
        Assert.assertTrue(notNullAndNotEmptyList.test(singletonList("a")));
    }

    @Test
    public void testORing() {
        Predicate<Object> nullOrEmptyList = Validator.or(Objects::isNull, a -> ((List) a).isEmpty());
        Assert.assertTrue(nullOrEmptyList.test(null));
        Assert.assertTrue(nullOrEmptyList.test(emptyList()));
        Assert.assertFalse(nullOrEmptyList.test(singletonList("a")));
    }

    @Test
    public void testNOTing() {
        Predicate<Object> isNotNull = Validator.not(Validator::isNull);
        Assert.assertTrue(isNotNull.test("a"));
        Assert.assertFalse(isNotNull.test(null));
    }

    @Test
    public void testIfTrueThenCheck() {
        BiPredicate<Object, Object> isEnabledAndCheck = Validator.ifTrueThenCheck(Validator::isList);
        Assert.assertTrue(isEnabledAndCheck.test(true, emptyList()));
        Assert.assertTrue(isEnabledAndCheck.test(false, null));
        Assert.assertTrue(isEnabledAndCheck.test(false, null));
        Assert.assertFalse(isEnabledAndCheck.test(true, 8));
    }

    @Test
    public void testIsImpliedBy() {
        BiPredicate<Object, Object> isEnabledAndCheck = Validator.isImpliedBy(Validator::isTrue, Validator::isList);
        Assert.assertTrue(isEnabledAndCheck.test(true, emptyList()));
        Assert.assertTrue(isEnabledAndCheck.test(false, null));
        Assert.assertTrue(isEnabledAndCheck.test(false, null));
        Assert.assertFalse(isEnabledAndCheck.test(true, 8));
    }
}
