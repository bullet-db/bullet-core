/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.evaluators.BinaryEvaluator;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BinaryExpressionTest {
    @Test
    public void testConstructor() {
        BinaryExpression expression = new BinaryExpression(new ValueExpression(1), new ValueExpression(2), Operation.ADD);
        Assert.assertEquals(expression.toString(), "{left: {value: 1, type: INTEGER}, right: {value: 2, type: INTEGER}, op: +, modifier: NONE, type: null}");
        Assert.assertTrue(expression.getEvaluator() instanceof BinaryEvaluator);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullLeft() {
        new BinaryExpression(null, new ValueExpression(2), Operation.ADD);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullRight() {
        new BinaryExpression(new ValueExpression(1), null, Operation.ADD);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullOp() {
        new BinaryExpression(new ValueExpression(1), new ValueExpression(2), null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullModifier() {
        new BinaryExpression(new ValueExpression(1), new ValueExpression(2), Operation.ADD, null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "Binary expression requires a binary operation\\.")
    public void testConstructorNotBinaryOp() {
        new BinaryExpression(new ValueExpression(1), new ValueExpression(2), Operation.SIZE_OF);
    }

    @Test
    public void testEqualsAndHashCode() {
        BinaryExpression expression = new BinaryExpression(new ValueExpression(1), new ValueExpression(2), Operation.ADD);
        expression.setType(Type.INTEGER);

        ExpressionUtils.testEqualsAndHashCode(() -> new BinaryExpression(new ValueExpression(1), new ValueExpression(2), Operation.ADD),
                new BinaryExpression(new ValueExpression(2), new ValueExpression(2), Operation.ADD),
                new BinaryExpression(new ValueExpression(1), new ValueExpression(1), Operation.ADD),
                new BinaryExpression(new ValueExpression(1), new ValueExpression(2), Operation.SUB),
                new BinaryExpression(new ValueExpression(1), new ValueExpression(2), Operation.ADD, BinaryExpression.Modifier.ALL),
                expression);
    }
}
