/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static com.yahoo.bullet.querying.evaluators.EvaluatorUtils.fieldEvaluator;
import static com.yahoo.bullet.querying.evaluators.EvaluatorUtils.listEvaluator;
import static com.yahoo.bullet.querying.evaluators.EvaluatorUtils.valueEvaluator;

public class BinaryOperationsTest {
    @Test
    public void testConstructor() {
        // coverage only
        new BinaryOperations();
    }

    @Test
    public void testAdd() {
        Assert.assertEquals(BinaryOperations.add(valueEvaluator(2.0), valueEvaluator(4.0f), null), new TypedObject(Type.DOUBLE, 6.0));
        Assert.assertEquals(BinaryOperations.add(valueEvaluator(2.0f), valueEvaluator(4L), null), new TypedObject(Type.FLOAT, 6.0f));
        Assert.assertEquals(BinaryOperations.add(valueEvaluator(2L), valueEvaluator(4), null), new TypedObject(Type.LONG, 6L));
        Assert.assertEquals(BinaryOperations.add(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.INTEGER, 6));
    }

    @Test
    public void testSub() {
        Assert.assertEquals(BinaryOperations.sub(valueEvaluator(2.0f), valueEvaluator(4.0), null), new TypedObject(Type.DOUBLE, -2.0));
        Assert.assertEquals(BinaryOperations.sub(valueEvaluator(2L), valueEvaluator(4.0f), null), new TypedObject(Type.FLOAT, -2.0f));
        Assert.assertEquals(BinaryOperations.sub(valueEvaluator(2), valueEvaluator(4L), null), new TypedObject(Type.LONG, -2L));
        Assert.assertEquals(BinaryOperations.sub(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.INTEGER, -2));
    }

    @Test
    public void testMul() {
        Assert.assertEquals(BinaryOperations.mul(valueEvaluator(2.0), valueEvaluator(4.0f), null), new TypedObject(Type.DOUBLE, 8.0));
        Assert.assertEquals(BinaryOperations.mul(valueEvaluator(2.0f), valueEvaluator(4L), null), new TypedObject(Type.FLOAT, 8.0f));
        Assert.assertEquals(BinaryOperations.mul(valueEvaluator(2L), valueEvaluator(4), null), new TypedObject(Type.LONG, 8L));
        Assert.assertEquals(BinaryOperations.mul(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.INTEGER, 8));
    }

    @Test
    public void testDiv() {
        Assert.assertEquals(BinaryOperations.div(valueEvaluator(2.0), valueEvaluator(4.0f), null), new TypedObject(Type.DOUBLE, 0.5));
        Assert.assertEquals(BinaryOperations.div(valueEvaluator(2.0f), valueEvaluator(4L), null), new TypedObject(Type.FLOAT, 0.5f));
        Assert.assertEquals(BinaryOperations.div(valueEvaluator(2L), valueEvaluator(4), null), new TypedObject(Type.LONG, 0L));
        Assert.assertEquals(BinaryOperations.div(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.INTEGER, 0));
    }

    @Test
    public void testEquals() {
        Assert.assertEquals(BinaryOperations.equals(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.equals(valueEvaluator(2), valueEvaluator(2), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testEqualsAny() {
        Assert.assertEquals(BinaryOperations.equalsAny(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.equalsAny(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.equalsAny(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testEqualsAll() {
        Assert.assertEquals(BinaryOperations.equalsAll(valueEvaluator(2), listEvaluator(2, 2, 2), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.equalsAll(valueEvaluator(2), listEvaluator(2, 2, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.equalsAll(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testNotEquals() {
        Assert.assertEquals(BinaryOperations.notEquals(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.notEquals(valueEvaluator(2), valueEvaluator(2), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testNotEqualsAny() {
        Assert.assertEquals(BinaryOperations.notEqualsAny(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.notEqualsAny(valueEvaluator(2), listEvaluator(2, 2, 2), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.notEqualsAny(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testNotEqualsAll() {
        Assert.assertEquals(BinaryOperations.notEqualsAll(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.notEqualsAll(valueEvaluator(2), listEvaluator(2, 2, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.notEqualsAll(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testGreaterThan() {
        Assert.assertEquals(BinaryOperations.greaterThan(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.greaterThan(valueEvaluator(2), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testGreaterThanAny() {
        Assert.assertEquals(BinaryOperations.greaterThanAny(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.greaterThanAny(valueEvaluator(2), listEvaluator(2, 2, 1), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.greaterThanAny(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testGreaterThanAll() {
        Assert.assertEquals(BinaryOperations.greaterThanAll(valueEvaluator(2), listEvaluator(1, 0, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.greaterThanAll(valueEvaluator(2), listEvaluator(1, 0, -1), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.greaterThanAll(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testLessThan() {
        Assert.assertEquals(BinaryOperations.lessThan(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.lessThan(valueEvaluator(2), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testLessThanAny() {
        Assert.assertEquals(BinaryOperations.lessThanAny(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.lessThanAny(valueEvaluator(2), listEvaluator(2, 2, 1), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.lessThanAny(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testLessThanAll() {
        Assert.assertEquals(BinaryOperations.lessThanAll(valueEvaluator(2), listEvaluator(1, 0, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.lessThanAll(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.lessThanAll(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testGreaterThanOrEquals() {
        Assert.assertEquals(BinaryOperations.greaterThanOrEquals(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.greaterThanOrEquals(valueEvaluator(2), valueEvaluator(2), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.greaterThanOrEquals(valueEvaluator(2), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testGreaterThanOrEqualsAny() {
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(2), listEvaluator(3, 3, 1), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testGreaterThanOrEqualsAll() {
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAll(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAll(valueEvaluator(2), listEvaluator(2, 1, 0), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAll(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testLessThanOrEquals() {
        Assert.assertEquals(BinaryOperations.lessThanOrEquals(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.lessThanOrEquals(valueEvaluator(2), valueEvaluator(2), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.lessThanOrEquals(valueEvaluator(2), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testLessThanOrEqualsAny() {
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(2), listEvaluator(2, 1, 0), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(2), listEvaluator(1, 0, 1), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testLessThanOrEqualsAll() {
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAll(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAll(valueEvaluator(2), listEvaluator(2, 1, 5), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAll(valueEvaluator(2), listEvaluator(), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testRegexLike() {
        Assert.assertEquals(BinaryOperations.regexLike(valueEvaluator("aabc"), valueEvaluator(".*abc"), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.regexLike(valueEvaluator("abbc"), valueEvaluator(".*abc"), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testRegexLikeAny() {
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator("aabc"), listEvaluator(".*abc"), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator("abbc"), listEvaluator(".*abc"), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator("abbc"), listEvaluator(".*abc", ".*bbc"), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator("abbc"), listEvaluator(), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testSizeIs() {
        Assert.assertEquals(BinaryOperations.sizeIs(listEvaluator(1, 2, 3), valueEvaluator(3), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.sizeIs(listEvaluator(1, 2, 3), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.sizeIs(valueEvaluator("abc"), valueEvaluator(3), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.sizeIs(valueEvaluator("abc"), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testContainsKey() {
        BulletRecord record = RecordBox.get().addMap("map", Pair.of("abc", 123)).getRecord();

        Assert.assertEquals(BinaryOperations.containsKey(fieldEvaluator("map"), valueEvaluator("abc"), record), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.containsKey(fieldEvaluator("map"), valueEvaluator("def"), record), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testContainsValue() {
        BulletRecord record = RecordBox.get().addMap("map", Pair.of("abc", 123)).getRecord();

        Assert.assertEquals(BinaryOperations.containsValue(fieldEvaluator("map"), valueEvaluator(123), record), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.containsValue(fieldEvaluator("map"), valueEvaluator(456), record), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.containsValue(listEvaluator(456, 789), valueEvaluator(123), record), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.containsValue(listEvaluator(456, 789), valueEvaluator(456), record), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testIn() {
        BulletRecord record = RecordBox.get().addMap("map", Pair.of("abc", 123)).getRecord();

        Assert.assertEquals(BinaryOperations.in(valueEvaluator(123), fieldEvaluator("map"), record), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.in(valueEvaluator(456), fieldEvaluator("map"), record), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.in(valueEvaluator(123), listEvaluator(456, 789), record), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.in(valueEvaluator(456), listEvaluator(456, 789), record), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testAnd() {
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(1), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(1), valueEvaluator(0), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(false), valueEvaluator(true), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(false), valueEvaluator(false), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testOr() {
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(1), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(1), valueEvaluator(0), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(false), valueEvaluator(true), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(false), valueEvaluator(false), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testXor() {
        Assert.assertEquals(BinaryOperations.xor(valueEvaluator(1), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.xor(valueEvaluator(1), valueEvaluator(0), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.xor(valueEvaluator(false), valueEvaluator(true), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.xor(valueEvaluator(false), valueEvaluator(false), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testFilter() {
        Assert.assertEquals(BinaryOperations.filter(listEvaluator(1, 2, 3, 4, 5), listEvaluator(false, true, false, true, true), null),
                            new TypedObject(Type.INTEGER_LIST, new ArrayList<>(Arrays.asList(2, 4, 5))));
        Assert.assertEquals(BinaryOperations.filter(listEvaluator(1, 2, 3), listEvaluator(false, false, false), null),
                            new TypedObject(Type.INTEGER_LIST, new ArrayList<>()));
    }
}
