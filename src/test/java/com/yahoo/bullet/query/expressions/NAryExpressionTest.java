/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.evaluators.NAryEvaluator;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class NAryExpressionTest {
    @Test
    public void testConstructor() {
        NAryExpression expression = new NAryExpression(Arrays.asList(new ValueExpression(1)), Operation.IF);
        Assert.assertEquals(expression.toString(), "{operands: [{value: 1, type: INTEGER}], op: IF, type: null}");
        Assert.assertTrue(expression.getEvaluator() instanceof NAryEvaluator);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullValues() {
        new NAryExpression(null, Operation.IF);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullOp() {
        new NAryExpression(new ArrayList<>(), null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "N-ary expression requires an n-ary operation\\.")
    public void testConstructorNotNAryOp() {
        new NAryExpression(new ArrayList<>(), Operation.ADD);
    }

    @Test
    public void testEqualsAndHashCode() {
        NAryExpression expression = new NAryExpression(Arrays.asList(new ValueExpression(1)), Operation.IF);
        expression.setType(Type.INTEGER);

        ExpressionUtils.testEqualsAndHashCode(() -> new NAryExpression(Arrays.asList(new ValueExpression(1)), Operation.IF),
                                              new NAryExpression(Arrays.asList(new ValueExpression(2)), Operation.IF),
                                              new NAryExpression(Arrays.asList(new ValueExpression(1)), Operation.AND),
                                              expression);
    }
}
