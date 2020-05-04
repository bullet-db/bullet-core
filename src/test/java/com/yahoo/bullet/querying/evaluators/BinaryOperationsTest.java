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
        Assert.assertEquals(BinaryOperations.ADD.apply(valueEvaluator(2.0), valueEvaluator(4.0f), null), new TypedObject(Type.DOUBLE, 6.0));
        Assert.assertEquals(BinaryOperations.ADD.apply(valueEvaluator(2.0f), valueEvaluator(4L), null), new TypedObject(Type.FLOAT, 6.0f));
        Assert.assertEquals(BinaryOperations.ADD.apply(valueEvaluator(2L), valueEvaluator(4), null), new TypedObject(Type.LONG, 6L));
        Assert.assertEquals(BinaryOperations.ADD.apply(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.INTEGER, 6));
    }

    @Test
    public void testSub() {
        Assert.assertEquals(BinaryOperations.SUB.apply(valueEvaluator(2.0f), valueEvaluator(4.0), null), new TypedObject(Type.DOUBLE, -2.0));
        Assert.assertEquals(BinaryOperations.SUB.apply(valueEvaluator(2L), valueEvaluator(4.0f), null), new TypedObject(Type.FLOAT, -2.0f));
        Assert.assertEquals(BinaryOperations.SUB.apply(valueEvaluator(2), valueEvaluator(4L), null), new TypedObject(Type.LONG, -2L));
        Assert.assertEquals(BinaryOperations.SUB.apply(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.INTEGER, -2));
    }

    @Test
    public void testMul() {
        Assert.assertEquals(BinaryOperations.MUL.apply(valueEvaluator(2.0), valueEvaluator(4.0f), null), new TypedObject(Type.DOUBLE, 8.0));
        Assert.assertEquals(BinaryOperations.MUL.apply(valueEvaluator(2.0f), valueEvaluator(4L), null), new TypedObject(Type.FLOAT, 8.0f));
        Assert.assertEquals(BinaryOperations.MUL.apply(valueEvaluator(2L), valueEvaluator(4), null), new TypedObject(Type.LONG, 8L));
        Assert.assertEquals(BinaryOperations.MUL.apply(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.INTEGER, 8));
    }

    @Test
    public void testDiv() {
        Assert.assertEquals(BinaryOperations.DIV.apply(valueEvaluator(2.0), valueEvaluator(4.0f), null), new TypedObject(Type.DOUBLE, 0.5));
        Assert.assertEquals(BinaryOperations.DIV.apply(valueEvaluator(2.0f), valueEvaluator(4L), null), new TypedObject(Type.FLOAT, 0.5f));
        Assert.assertEquals(BinaryOperations.DIV.apply(valueEvaluator(2L), valueEvaluator(4), null), new TypedObject(Type.LONG, 0L));
        Assert.assertEquals(BinaryOperations.DIV.apply(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.INTEGER, 0));
    }

    @Test
    public void testEquals() {
        Assert.assertEquals(BinaryOperations.EQUALS.apply(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.EQUALS.apply(valueEvaluator(2), valueEvaluator(2), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testEqualsAny() {
        Assert.assertEquals(BinaryOperations.EQUALS_ANY.apply(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.EQUALS_ANY.apply(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testEqualsAll() {
        Assert.assertEquals(BinaryOperations.EQUALS_ALL.apply(valueEvaluator(2), listEvaluator(2, 2, 2), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.EQUALS_ALL.apply(valueEvaluator(2), listEvaluator(2, 2, 6), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testNotEquals() {
        Assert.assertEquals(BinaryOperations.NOT_EQUALS.apply(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.NOT_EQUALS.apply(valueEvaluator(2), valueEvaluator(2), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testNotEqualsAny() {
        Assert.assertEquals(BinaryOperations.NOT_EQUALS_ANY.apply(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.NOT_EQUALS_ANY.apply(valueEvaluator(2), listEvaluator(2, 2, 2), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testNotEqualsAll() {
        Assert.assertEquals(BinaryOperations.NOT_EQUALS_ALL.apply(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.NOT_EQUALS_ALL.apply(valueEvaluator(2), listEvaluator(2, 2, 6), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testGreaterThan() {
        Assert.assertEquals(BinaryOperations.GREATER_THAN.apply(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.GREATER_THAN.apply(valueEvaluator(2), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testGreaterThanAny() {
        Assert.assertEquals(BinaryOperations.GREATER_THAN_ANY.apply(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.GREATER_THAN_ANY.apply(valueEvaluator(2), listEvaluator(2, 2, 1), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testGreaterThanAll() {
        Assert.assertEquals(BinaryOperations.GREATER_THAN_ALL.apply(valueEvaluator(2), listEvaluator(1, 0, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.GREATER_THAN_ALL.apply(valueEvaluator(2), listEvaluator(1, 0, -1), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testLessThan() {
        Assert.assertEquals(BinaryOperations.LESS_THAN.apply(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.LESS_THAN.apply(valueEvaluator(2), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testLessThanAny() {
        Assert.assertEquals(BinaryOperations.LESS_THAN_ANY.apply(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.LESS_THAN_ANY.apply(valueEvaluator(2), listEvaluator(2, 2, 1), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testLessThanAll() {
        Assert.assertEquals(BinaryOperations.LESS_THAN_ALL.apply(valueEvaluator(2), listEvaluator(1, 0, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.LESS_THAN_ALL.apply(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testGreaterThanOrEquals() {
        Assert.assertEquals(BinaryOperations.GREATER_THAN_OR_EQUALS.apply(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.GREATER_THAN_OR_EQUALS.apply(valueEvaluator(2), valueEvaluator(2), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.GREATER_THAN_OR_EQUALS.apply(valueEvaluator(2), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testGreaterThanOrEqualsAny() {
        Assert.assertEquals(BinaryOperations.GREATER_THAN_OR_EQUALS_ANY.apply(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.GREATER_THAN_OR_EQUALS_ANY.apply(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.GREATER_THAN_OR_EQUALS_ANY.apply(valueEvaluator(2), listEvaluator(3, 3, 1), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testGreaterThanOrEqualsAll() {
        Assert.assertEquals(BinaryOperations.GREATER_THAN_OR_EQUALS_ALL.apply(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.GREATER_THAN_OR_EQUALS_ALL.apply(valueEvaluator(2), listEvaluator(2, 1, 0), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testLessThanOrEquals() {
        Assert.assertEquals(BinaryOperations.LESS_THAN_OR_EQUALS.apply(valueEvaluator(2), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.LESS_THAN_OR_EQUALS.apply(valueEvaluator(2), valueEvaluator(2), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.LESS_THAN_OR_EQUALS.apply(valueEvaluator(2), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testLessThanOrEqualsAny() {
        Assert.assertEquals(BinaryOperations.LESS_THAN_OR_EQUALS_ANY.apply(valueEvaluator(2), listEvaluator(3, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.LESS_THAN_OR_EQUALS_ANY.apply(valueEvaluator(2), listEvaluator(2, 1, 0), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.LESS_THAN_OR_EQUALS_ANY.apply(valueEvaluator(2), listEvaluator(1, 0, 1), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testLessThanOrEqualsAll() {
        Assert.assertEquals(BinaryOperations.LESS_THAN_OR_EQUALS_ALL.apply(valueEvaluator(2), listEvaluator(2, 4, 6), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.LESS_THAN_OR_EQUALS_ALL.apply(valueEvaluator(2), listEvaluator(2, 1, 5), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testRegexLike() {
        Assert.assertEquals(BinaryOperations.REGEX_LIKE.apply(valueEvaluator("aabc"), valueEvaluator(".*abc"), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.REGEX_LIKE.apply(valueEvaluator("abbc"), valueEvaluator(".*abc"), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testSizeIs() {
        Assert.assertEquals(BinaryOperations.SIZE_IS.apply(listEvaluator(1, 2, 3), valueEvaluator(3), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.SIZE_IS.apply(listEvaluator(1, 2, 3), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.SIZE_IS.apply(valueEvaluator("abc"), valueEvaluator(3), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.SIZE_IS.apply(valueEvaluator("abc"), valueEvaluator(4), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testContainsKey() {
        BulletRecord record = RecordBox.get().addMap("map", Pair.of("abc", 123)).getRecord();

        Assert.assertEquals(BinaryOperations.CONTAINS_KEY.apply(fieldEvaluator("map"), valueEvaluator("abc"), record), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.CONTAINS_KEY.apply(fieldEvaluator("map"), valueEvaluator("def"), record), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testContainsValue() {
        BulletRecord record = RecordBox.get().addMap("map", Pair.of("abc", 123)).getRecord();

        Assert.assertEquals(BinaryOperations.CONTAINS_VALUE.apply(fieldEvaluator("map"), valueEvaluator(123), record), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.CONTAINS_VALUE.apply(fieldEvaluator("map"), valueEvaluator(456), record), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.CONTAINS_VALUE.apply(listEvaluator(456, 789), valueEvaluator(123), record), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.CONTAINS_VALUE.apply(listEvaluator(456, 789), valueEvaluator(456), record), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testIn() {
        BulletRecord record = RecordBox.get().addMap("map", Pair.of("abc", 123)).getRecord();

        Assert.assertEquals(BinaryOperations.IN.apply(valueEvaluator(123), fieldEvaluator("map"), record), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.IN.apply(valueEvaluator(456), fieldEvaluator("map"), record), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.IN.apply(valueEvaluator(123), listEvaluator(456, 789), record), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.IN.apply(valueEvaluator(456), listEvaluator(456, 789), record), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testAnd() {
        Assert.assertEquals(BinaryOperations.AND.apply(valueEvaluator(1), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.AND.apply(valueEvaluator(1), valueEvaluator(0), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.AND.apply(valueEvaluator(false), valueEvaluator(true), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.AND.apply(valueEvaluator(false), valueEvaluator(false), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testOr() {
        Assert.assertEquals(BinaryOperations.OR.apply(valueEvaluator(1), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.OR.apply(valueEvaluator(1), valueEvaluator(0), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.OR.apply(valueEvaluator(false), valueEvaluator(true), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.OR.apply(valueEvaluator(false), valueEvaluator(false), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testXor() {
        Assert.assertEquals(BinaryOperations.XOR.apply(valueEvaluator(1), valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(BinaryOperations.XOR.apply(valueEvaluator(1), valueEvaluator(0), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.XOR.apply(valueEvaluator(false), valueEvaluator(true), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(BinaryOperations.XOR.apply(valueEvaluator(false), valueEvaluator(false), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testFilter() {
        Assert.assertEquals(BinaryOperations.FILTER.apply(listEvaluator(1, 2, 3, 4, 5), listEvaluator(false, true, false, true, true), null),
                            new TypedObject(Type.INTEGER_LIST, new ArrayList<>(Arrays.asList(2, 4, 5))));
        Assert.assertEquals(BinaryOperations.FILTER.apply(listEvaluator(1, 2, 3), listEvaluator(false, false, false), null),
                            new TypedObject(Type.INTEGER_LIST, new ArrayList<>()));
    }
}
