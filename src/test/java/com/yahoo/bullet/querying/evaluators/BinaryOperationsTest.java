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
        Assert.assertEquals(BinaryOperations.add(valueEvaluator(null), valueEvaluator(4), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.add(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.add(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testSub() {
        Assert.assertEquals(BinaryOperations.sub(valueEvaluator(2.0f), valueEvaluator(4.0), null), new TypedObject(Type.DOUBLE, -2.0));
        Assert.assertEquals(BinaryOperations.sub(valueEvaluator(2L), valueEvaluator(4.0f), null), new TypedObject(Type.FLOAT, -2.0f));
        Assert.assertEquals(BinaryOperations.sub(valueEvaluator(2), valueEvaluator(4L), null), new TypedObject(Type.LONG, -2L));
        Assert.assertEquals(BinaryOperations.sub(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.INTEGER, -2));
        Assert.assertEquals(BinaryOperations.sub(valueEvaluator(null), valueEvaluator(4), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.sub(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.sub(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testMul() {
        Assert.assertEquals(BinaryOperations.mul(valueEvaluator(2.0), valueEvaluator(4.0f), null), new TypedObject(Type.DOUBLE, 8.0));
        Assert.assertEquals(BinaryOperations.mul(valueEvaluator(2.0f), valueEvaluator(4L), null), new TypedObject(Type.FLOAT, 8.0f));
        Assert.assertEquals(BinaryOperations.mul(valueEvaluator(2L), valueEvaluator(4), null), new TypedObject(Type.LONG, 8L));
        Assert.assertEquals(BinaryOperations.mul(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.INTEGER, 8));
        Assert.assertEquals(BinaryOperations.mul(valueEvaluator(null), valueEvaluator(4), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.mul(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.mul(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testDiv() {
        Assert.assertEquals(BinaryOperations.div(valueEvaluator(2.0), valueEvaluator(4.0f), null), new TypedObject(Type.DOUBLE, 0.5));
        Assert.assertEquals(BinaryOperations.div(valueEvaluator(2.0f), valueEvaluator(4L), null), new TypedObject(Type.FLOAT, 0.5f));
        Assert.assertEquals(BinaryOperations.div(valueEvaluator(2L), valueEvaluator(4), null), new TypedObject(Type.LONG, 0L));
        Assert.assertEquals(BinaryOperations.div(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.INTEGER, 0));
        Assert.assertEquals(BinaryOperations.div(valueEvaluator(null), valueEvaluator(4), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.div(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.div(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testEquals() {
        Assert.assertEquals(BinaryOperations.equals(valueEvaluator(2), valueEvaluator(4), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.equals(valueEvaluator(2), valueEvaluator(2), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.equals(valueEvaluator(null), valueEvaluator(4), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.equals(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.equals(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testEqualsAny() {
        Assert.assertEquals(BinaryOperations.equalsAny(valueEvaluator(2), listEvaluator(2, 4, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.equalsAny(valueEvaluator(2), listEvaluator(3, 4, 6), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.equalsAny(valueEvaluator(2), listEvaluator(), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.equalsAny(valueEvaluator(null), listEvaluator(2, 4, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.equalsAny(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.equalsAny(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.equalsAny(valueEvaluator(2), listEvaluator(2, null, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.equalsAny(valueEvaluator(2), listEvaluator(3, null, 6), null), TypedObject.NULL);
    }

    @Test
    public void testEqualsAll() {
        Assert.assertEquals(BinaryOperations.equalsAll(valueEvaluator(2), listEvaluator(2, 2, 2), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.equalsAll(valueEvaluator(2), listEvaluator(2, 2, 6), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.equalsAll(valueEvaluator(2), listEvaluator(), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.equalsAll(valueEvaluator(null), listEvaluator(2, 2, 2), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.equalsAll(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.equalsAll(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.equalsAll(valueEvaluator(2), listEvaluator(2, null, 2), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.equalsAll(valueEvaluator(2), listEvaluator(2, null, 6), null), TypedObject.FALSE);
    }

    @Test
    public void testNotEquals() {
        Assert.assertEquals(BinaryOperations.notEquals(valueEvaluator(2), valueEvaluator(4), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.notEquals(valueEvaluator(2), valueEvaluator(2), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.notEquals(valueEvaluator(null), valueEvaluator(4), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notEquals(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notEquals(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testNotEqualsAny() {
        Assert.assertEquals(BinaryOperations.notEqualsAny(valueEvaluator(2), listEvaluator(2, 4, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.notEqualsAny(valueEvaluator(2), listEvaluator(2, 2, 2), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.notEqualsAny(valueEvaluator(2), listEvaluator(), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.notEqualsAny(valueEvaluator(null), listEvaluator(2, 4, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notEqualsAny(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notEqualsAny(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notEqualsAny(valueEvaluator(2), listEvaluator(2, null, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.notEqualsAny(valueEvaluator(2), listEvaluator(2, null, null), null), TypedObject.NULL);
    }

    @Test
    public void testNotEqualsAll() {
        Assert.assertEquals(BinaryOperations.notEqualsAll(valueEvaluator(2), listEvaluator(3, 4, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.notEqualsAll(valueEvaluator(2), listEvaluator(2, 2, 6), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.notEqualsAll(valueEvaluator(2), listEvaluator(), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.notEqualsAll(valueEvaluator(null), listEvaluator(3, 4, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notEqualsAll(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notEqualsAll(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notEqualsAll(valueEvaluator(2), listEvaluator(3, null, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notEqualsAll(valueEvaluator(2), listEvaluator(2, null, 6), null), TypedObject.FALSE);
    }

    @Test
    public void testGreaterThan() {
        Assert.assertEquals(BinaryOperations.greaterThan(valueEvaluator(2), valueEvaluator(4), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.greaterThan(valueEvaluator(2), valueEvaluator(1), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.greaterThan(valueEvaluator(null), valueEvaluator(4), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThan(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThan(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testGreaterThanAny() {
        Assert.assertEquals(BinaryOperations.greaterThanAny(valueEvaluator(2), listEvaluator(2, 4, 6), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.greaterThanAny(valueEvaluator(2), listEvaluator(2, 2, 1), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.greaterThanAny(valueEvaluator(2), listEvaluator(), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.greaterThanAny(valueEvaluator(null), listEvaluator(2, 4, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanAny(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanAny(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanAny(valueEvaluator(2), listEvaluator(2, null, 1), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.greaterThanAny(valueEvaluator(2), listEvaluator(2, null, 6), null), TypedObject.NULL);
    }

    @Test
    public void testGreaterThanAll() {
        Assert.assertEquals(BinaryOperations.greaterThanAll(valueEvaluator(2), listEvaluator(1, 0, 6), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.greaterThanAll(valueEvaluator(2), listEvaluator(1, 0, -1), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.greaterThanAll(valueEvaluator(2), listEvaluator(), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.greaterThanAll(valueEvaluator(null), listEvaluator(1, 0, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanAll(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanAll(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanAll(valueEvaluator(2), listEvaluator(1, null, 6), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.greaterThanAll(valueEvaluator(2), listEvaluator(1, null, -1), null), TypedObject.NULL);
    }

    @Test
    public void testLessThan() {
        Assert.assertEquals(BinaryOperations.lessThan(valueEvaluator(2), valueEvaluator(4), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThan(valueEvaluator(2), valueEvaluator(1), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.lessThan(valueEvaluator(null), valueEvaluator(4), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThan(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThan(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testLessThanAny() {
        Assert.assertEquals(BinaryOperations.lessThanAny(valueEvaluator(2), listEvaluator(2, 4, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThanAny(valueEvaluator(2), listEvaluator(2, 2, 1), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.lessThanAny(valueEvaluator(2), listEvaluator(), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.lessThanAny(valueEvaluator(null), listEvaluator(2, 4, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanAny(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanAny(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanAny(valueEvaluator(2), listEvaluator(2, null, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThanAny(valueEvaluator(2), listEvaluator(2, null, 1), null), TypedObject.NULL);
    }

    @Test
    public void testLessThanAll() {
        Assert.assertEquals(BinaryOperations.lessThanAll(valueEvaluator(2), listEvaluator(1, 0, 6), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.lessThanAll(valueEvaluator(2), listEvaluator(3, 4, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThanAll(valueEvaluator(2), listEvaluator(), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThanAll(valueEvaluator(null), listEvaluator(1, 0, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanAll(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanAll(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanAll(valueEvaluator(2), listEvaluator(1, null, 6), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.lessThanAll(valueEvaluator(2), listEvaluator(3, null, 6), null), TypedObject.NULL);
    }

    @Test
    public void testGreaterThanOrEquals() {
        Assert.assertEquals(BinaryOperations.greaterThanOrEquals(valueEvaluator(2), valueEvaluator(4), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.greaterThanOrEquals(valueEvaluator(2), valueEvaluator(2), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.greaterThanOrEquals(valueEvaluator(2), valueEvaluator(1), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.greaterThanOrEquals(valueEvaluator(null), valueEvaluator(4), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanOrEquals(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanOrEquals(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testGreaterThanOrEqualsAny() {
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(2), listEvaluator(3, 4, 6), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(2), listEvaluator(2, 4, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(2), listEvaluator(3, 3, 1), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(2), listEvaluator(), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(null), listEvaluator(3, 4, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(2), listEvaluator(3, null, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAny(valueEvaluator(2), listEvaluator(2, null, 6), null), TypedObject.TRUE);
    }

    @Test
    public void testGreaterThanOrEqualsAll() {
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAll(valueEvaluator(2), listEvaluator(3, 4, 6), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAll(valueEvaluator(2), listEvaluator(2, 1, 0), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAll(valueEvaluator(2), listEvaluator(), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAll(valueEvaluator(null), listEvaluator(3, 4, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAll(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAll(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAll(valueEvaluator(2), listEvaluator(3, null, 6), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.greaterThanOrEqualsAll(valueEvaluator(2), listEvaluator(2, null, 0), null), TypedObject.NULL);
    }

    @Test
    public void testLessThanOrEquals() {
        Assert.assertEquals(BinaryOperations.lessThanOrEquals(valueEvaluator(2), valueEvaluator(4), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThanOrEquals(valueEvaluator(2), valueEvaluator(2), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThanOrEquals(valueEvaluator(2), valueEvaluator(1), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.lessThanOrEquals(valueEvaluator(null), valueEvaluator(4), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanOrEquals(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanOrEquals(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testLessThanOrEqualsAny() {
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(2), listEvaluator(3, 4, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(2), listEvaluator(2, 1, 0), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(2), listEvaluator(1, 0, 1), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(2), listEvaluator(), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(null), listEvaluator(3, 4, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(2), listEvaluator(3, null, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAny(valueEvaluator(2), listEvaluator(1, null, 0), null), TypedObject.NULL);
    }

    @Test
    public void testLessThanOrEqualsAll() {
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAll(valueEvaluator(2), listEvaluator(2, 4, 6), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAll(valueEvaluator(2), listEvaluator(2, 1, 5), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAll(valueEvaluator(2), listEvaluator(), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAll(valueEvaluator(null), listEvaluator(2, 4, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAll(valueEvaluator(2), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAll(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAll(valueEvaluator(2), listEvaluator(2, null, 6), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.lessThanOrEqualsAll(valueEvaluator(2), listEvaluator(1, null, 5), null), TypedObject.FALSE);
    }

    @Test
    public void testRegexLike() {
        Assert.assertEquals(BinaryOperations.regexLike(valueEvaluator("aabc"), valueEvaluator(".*abc"), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.regexLike(valueEvaluator("abbc"), valueEvaluator(".*abc"), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.regexLike(valueEvaluator(null), valueEvaluator(".*abc"), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.regexLike(valueEvaluator("aabc"), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.regexLike(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testRegexLikeAny() {
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator("aabc"), listEvaluator(".*abc"), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator("abbc"), listEvaluator(".*abc"), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator("abbc"), listEvaluator(".*abc", ".*bbc"), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator("abbc"), listEvaluator(), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator(null), listEvaluator(".*abc"), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator("abbc"), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator("abbc"), listEvaluator(null, ".*bbc"), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.regexLikeAny(valueEvaluator("abbc"), listEvaluator(".*abc", null), null), TypedObject.NULL);
    }

    @Test
    public void testNotRegexLike() {
        Assert.assertEquals(BinaryOperations.notRegexLike(valueEvaluator("aabc"), valueEvaluator(".*abc"), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.notRegexLike(valueEvaluator("abbc"), valueEvaluator(".*abc"), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.notRegexLike(valueEvaluator(null), valueEvaluator(".*abc"), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notRegexLike(valueEvaluator("aabc"), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notRegexLike(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testNotRegexLikeAny() {
        Assert.assertEquals(BinaryOperations.notRegexLikeAny(valueEvaluator("aabc"), listEvaluator(".*abc"), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.notRegexLikeAny(valueEvaluator("abbc"), listEvaluator(".*abc"), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.notRegexLikeAny(valueEvaluator("abbc"), listEvaluator(".*abc", ".*bbc"), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.notRegexLikeAny(valueEvaluator("abbc"), listEvaluator(), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.notRegexLikeAny(valueEvaluator(null), listEvaluator(".*abc"), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notRegexLikeAny(valueEvaluator("abbc"), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notRegexLikeAny(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notRegexLikeAny(valueEvaluator("abbc"), listEvaluator(null, ".*bbc"), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.notRegexLikeAny(valueEvaluator("abbc"), listEvaluator(".*abc", null), null), TypedObject.NULL);
    }

    @Test
    public void testSizeIs() {
        Assert.assertEquals(BinaryOperations.sizeIs(listEvaluator(1, 2, 3), valueEvaluator(3), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.sizeIs(listEvaluator(1, 2, 3), valueEvaluator(4), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.sizeIs(valueEvaluator("abc"), valueEvaluator(3), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.sizeIs(valueEvaluator("abc"), valueEvaluator(4), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.sizeIs(valueEvaluator(null), valueEvaluator(4), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.sizeIs(listEvaluator(1, 2, 3), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.sizeIs(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testContainsKey() {
        BulletRecord record = RecordBox.get().addMap("map", Pair.of("abc", 123)).getRecord();

        Assert.assertEquals(BinaryOperations.containsKey(fieldEvaluator("map"), valueEvaluator("abc"), record), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.containsKey(fieldEvaluator("map"), valueEvaluator("def"), record), TypedObject.FALSE);

        record = RecordBox.get().addMap("map", Pair.of("abc", 123), Pair.of(null, 456)).getRecord();

        Assert.assertEquals(BinaryOperations.containsKey(valueEvaluator(null), valueEvaluator("abc"), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.containsKey(fieldEvaluator("map"), valueEvaluator(null), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.containsKey(valueEvaluator(null), valueEvaluator(null), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.containsKey(fieldEvaluator("map"), valueEvaluator("abc"), record), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.containsKey(fieldEvaluator("map"), valueEvaluator("def"), record), TypedObject.NULL);
    }

    @Test
    public void testContainsValue() {
        BulletRecord record = RecordBox.get().addMap("map", Pair.of("abc", 123)).getRecord();

        Assert.assertEquals(BinaryOperations.containsValue(fieldEvaluator("map"), valueEvaluator(123), record), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.containsValue(fieldEvaluator("map"), valueEvaluator(456), record), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.containsValue(listEvaluator(456, 789), valueEvaluator(123), record), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.containsValue(listEvaluator(456, 789), valueEvaluator(456), record), TypedObject.TRUE);

        record = RecordBox.get().addMap("map", Pair.of("abc", 123), Pair.of("def", null)).getRecord();

        Assert.assertEquals(BinaryOperations.containsValue(valueEvaluator(null), valueEvaluator(123), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.containsValue(fieldEvaluator("map"), valueEvaluator(null), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.containsValue(valueEvaluator(null), valueEvaluator(null), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.containsValue(fieldEvaluator("map"), valueEvaluator(123), record), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.containsValue(fieldEvaluator("map"), valueEvaluator(456), record), TypedObject.NULL);
    }

    @Test
    public void testIn() {
        BulletRecord record = RecordBox.get().addMap("map", Pair.of("abc", 123)).getRecord();

        Assert.assertEquals(BinaryOperations.in(valueEvaluator(123), fieldEvaluator("map"), record), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.in(valueEvaluator(456), fieldEvaluator("map"), record), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.in(valueEvaluator(123), listEvaluator(456, 789), record), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.in(valueEvaluator(456), listEvaluator(456, 789), record), TypedObject.TRUE);

        record = RecordBox.get().addMap("map", Pair.of("abc", 123), Pair.of("def", null)).getRecord();

        Assert.assertEquals(BinaryOperations.in(valueEvaluator(null), fieldEvaluator("map"), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.in(valueEvaluator(123), valueEvaluator(null), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.in(valueEvaluator(null), valueEvaluator(null), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.in(valueEvaluator(123), fieldEvaluator("map"), record), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.in(valueEvaluator(456), fieldEvaluator("map"), record), TypedObject.NULL);
    }

    @Test
    public void testNotIn() {
        BulletRecord record = RecordBox.get().addMap("map", Pair.of("abc", 123)).getRecord();

        Assert.assertEquals(BinaryOperations.notIn(valueEvaluator(123), fieldEvaluator("map"), record), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.notIn(valueEvaluator(456), fieldEvaluator("map"), record), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.notIn(valueEvaluator(123), listEvaluator(456, 789), record), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.notIn(valueEvaluator(456), listEvaluator(456, 789), record), TypedObject.FALSE);

        record = RecordBox.get().addMap("map", Pair.of("abc", 123), Pair.of("def", null)).getRecord();

        Assert.assertEquals(BinaryOperations.notIn(valueEvaluator(null), fieldEvaluator("map"), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notIn(valueEvaluator(123), valueEvaluator(null), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notIn(valueEvaluator(null), valueEvaluator(null), record), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.notIn(valueEvaluator(123), fieldEvaluator("map"), record), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.notIn(valueEvaluator(456), fieldEvaluator("map"), record), TypedObject.NULL);
    }

    @Test
    public void testAnd() {
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(1), valueEvaluator(1), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(1), valueEvaluator(0), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(false), valueEvaluator(true), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(false), valueEvaluator(false), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(true), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(null), valueEvaluator(true), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(false), valueEvaluator(null), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(null), valueEvaluator(false), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.and(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testOr() {
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(1), valueEvaluator(1), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(1), valueEvaluator(0), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(false), valueEvaluator(true), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(false), valueEvaluator(false), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(true), valueEvaluator(null), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(null), valueEvaluator(true), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(false), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(null), valueEvaluator(false), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.or(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testXor() {
        Assert.assertEquals(BinaryOperations.xor(valueEvaluator(1), valueEvaluator(1), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.xor(valueEvaluator(1), valueEvaluator(0), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.xor(valueEvaluator(false), valueEvaluator(true), null), TypedObject.TRUE);
        Assert.assertEquals(BinaryOperations.xor(valueEvaluator(false), valueEvaluator(false), null), TypedObject.FALSE);
        Assert.assertEquals(BinaryOperations.xor(valueEvaluator(null), valueEvaluator(true), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.xor(valueEvaluator(true), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.xor(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
    }

    @Test
    public void testFilter() {
        Assert.assertEquals(BinaryOperations.filter(listEvaluator(1, 2, 3, 4, 5), listEvaluator(false, true, false, true, true), null),
                            new TypedObject(Type.INTEGER_LIST, new ArrayList<>(Arrays.asList(2, 4, 5))));
        Assert.assertEquals(BinaryOperations.filter(listEvaluator(1, 2, 3), listEvaluator(false, false, false), null),
                            new TypedObject(Type.INTEGER_LIST, new ArrayList<>()));
        Assert.assertEquals(BinaryOperations.filter(valueEvaluator(null), listEvaluator(false, false, false), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.filter(listEvaluator(1, 2, 3), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.filter(valueEvaluator(null), valueEvaluator(null), null), TypedObject.NULL);
        Assert.assertEquals(BinaryOperations.filter(listEvaluator(1, 2, 3, 4, 5), listEvaluator(false, null, false, true, true), null),
                            new TypedObject(Type.INTEGER_LIST, new ArrayList<>(Arrays.asList(4, 5))));
    }
}
