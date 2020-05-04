/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.evaluators.CastEvaluator;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CastExpressionTest {
    @Test
    public void testConstructor() {
        CastExpression expression = new CastExpression(new ValueExpression(1), Type.LONG);
        Assert.assertEquals(expression.toString(), "{value: {value: 1, type: INTEGER}, castType: LONG, type: null}");
        Assert.assertTrue(expression.getEvaluator() instanceof CastEvaluator);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullValue() {
        new CastExpression(null, Type.LONG);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullCastType() {
        new CastExpression(new ValueExpression(1), null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "Cast type cannot be null or unknown\\.")
    public void testConstructorNullType() {
        new CastExpression(new ValueExpression(1), Type.NULL);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "Cast type cannot be null or unknown\\.")
    public void testConstructorUnknownType() {
        new CastExpression(new ValueExpression(1), Type.UNKNOWN);
    }

    @Test
    public void testEqualsAndHashCode() {
        CastExpression expression = new CastExpression(new ValueExpression(1), Type.LONG);
        expression.setType(Type.LONG);

        ExpressionUtils.testEqualsAndHashCode(() -> new CastExpression(new ValueExpression(1), Type.LONG),
                                              new CastExpression(new ValueExpression(2), Type.LONG),
                                              new CastExpression(new ValueExpression(1), Type.DOUBLE),
                                              expression);
    }
}
