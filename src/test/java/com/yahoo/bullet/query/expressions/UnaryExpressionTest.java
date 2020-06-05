/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.evaluators.UnaryEvaluator;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UnaryExpressionTest {
    @Test
    public void testConstructor() {
        UnaryExpression expression = new UnaryExpression(new ValueExpression(1), Operation.NOT);
        Assert.assertEquals(expression.toString(), "{operand: {value: 1, type: INTEGER}, op: NOT, type: null}");
        Assert.assertTrue(expression.getEvaluator() instanceof UnaryEvaluator);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullOperand() {
        new UnaryExpression(null, Operation.NOT);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullOp() {
        new UnaryExpression(new ValueExpression(1), null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "Unary expression requires a unary operation\\.")
    public void testConstructorNotUnaryOp() {
        new UnaryExpression(new ValueExpression(1), Operation.AND);
    }

    @Test
    public void testEqualsAndHashCode() {
        UnaryExpression expression = new UnaryExpression(new ValueExpression(1), Operation.NOT);
        expression.setType(Type.BOOLEAN);

        ExpressionUtils.testEqualsAndHashCode(() -> new UnaryExpression(new ValueExpression(1), Operation.NOT),
                                              new UnaryExpression(new ValueExpression(2), Operation.NOT),
                                              new UnaryExpression(new ValueExpression(1), Operation.IS_NOT_NULL),
                                              expression);
    }
}
