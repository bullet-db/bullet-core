/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.evaluators.ValueEvaluator;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class ValueExpressionTest {
    @Test
    public void testConstructor() {
        ValueExpression expression = new ValueExpression(1);
        Assert.assertEquals(expression.getValue(), 1);
        Assert.assertEquals(expression.getType(), Type.INTEGER);
        Assert.assertEquals(expression.toString(), "{value: 1, type: INTEGER}");

        expression = new ValueExpression("1");
        Assert.assertEquals(expression.getValue(), "1");
        Assert.assertEquals(expression.getType(), Type.STRING);
        Assert.assertEquals(expression.toString(), "{value: '1', type: STRING}");

        expression = new ValueExpression(null);
        Assert.assertEquals(expression.getValue(), null);
        Assert.assertEquals(expression.getType(), Type.NULL);
        Assert.assertEquals(expression.toString(), "{value: null, type: NULL}");

    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "Value must be primitive or null\\.")
    public void testConstructorThrow() {
        new ValueExpression(new ArrayList<>());
    }

    @Test
    public void testGetEvaluator() {
        Assert.assertTrue(new ValueExpression(1).getEvaluator() instanceof ValueEvaluator);
    }

    @Test
    public void testEqualsAndHashCode() {
        ValueExpression expression = new ValueExpression(1);
        expression.setType(Type.LONG);

        ExpressionUtils.testEqualsAndHashCode(() -> new ValueExpression(1),
                                              new ValueExpression(2),
                                              expression);
    }
}
