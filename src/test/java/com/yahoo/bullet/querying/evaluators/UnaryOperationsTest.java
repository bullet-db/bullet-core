/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.yahoo.bullet.querying.evaluators.EvaluatorUtils.listEvaluator;
import static com.yahoo.bullet.querying.evaluators.EvaluatorUtils.valueEvaluator;

public class UnaryOperationsTest {
    @Test
    public void testConstructor() {
        // coverage only
        new UnaryOperations();
    }

    @Test
    public void testNot() {
        Assert.assertEquals(UnaryOperations.NOT.apply(valueEvaluator(false), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(UnaryOperations.NOT.apply(valueEvaluator(true), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(UnaryOperations.NOT.apply(valueEvaluator(0), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(UnaryOperations.NOT.apply(valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(UnaryOperations.NOT.apply(valueEvaluator("abc"), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(UnaryOperations.NOT.apply(valueEvaluator("true"), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testSizeOf() {
        Assert.assertEquals(UnaryOperations.SIZE_OF.apply(listEvaluator(1, 2, 3), null), new TypedObject(Type.INTEGER, 3));
        Assert.assertEquals(UnaryOperations.SIZE_OF.apply(valueEvaluator("hello"), null), new TypedObject(Type.INTEGER, 5));
    }

    @Test
    public void testIsNull() {
        Assert.assertEquals(UnaryOperations.IS_NULL.apply(valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(UnaryOperations.IS_NULL.apply(valueEvaluator(null), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testIsNotNull() {
        Assert.assertEquals(UnaryOperations.IS_NOT_NULL.apply(valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(UnaryOperations.IS_NOT_NULL.apply(valueEvaluator(null), null), new TypedObject(Type.BOOLEAN, false));
    }
}
