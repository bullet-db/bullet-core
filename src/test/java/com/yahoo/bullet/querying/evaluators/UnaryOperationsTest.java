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
        Assert.assertEquals(UnaryOperations.not(valueEvaluator(false), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(UnaryOperations.not(valueEvaluator(true), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(UnaryOperations.not(valueEvaluator(0), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(UnaryOperations.not(valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(UnaryOperations.not(valueEvaluator("abc"), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(UnaryOperations.not(valueEvaluator("true"), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testSizeOf() {
        Assert.assertEquals(UnaryOperations.sizeOf(listEvaluator(1, 2, 3), null), new TypedObject(Type.INTEGER, 3));
        Assert.assertEquals(UnaryOperations.sizeOf(valueEvaluator("hello"), null), new TypedObject(Type.INTEGER, 5));
    }

    @Test
    public void testIsNull() {
        Assert.assertEquals(UnaryOperations.isNull(valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(UnaryOperations.isNull(valueEvaluator(null), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testIsNotNull() {
        Assert.assertEquals(UnaryOperations.isNotNull(valueEvaluator(1), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(UnaryOperations.isNotNull(valueEvaluator(null), null), new TypedObject(Type.BOOLEAN, false));
    }
}
