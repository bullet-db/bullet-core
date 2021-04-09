/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.querying.evaluators.FieldEvaluator;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldExpressionTest {
    @Test
    public void testConstructors() {
        FieldExpression expression = new FieldExpression("abc");
        Assert.assertEquals(expression.getName(), "abc");
        Assert.assertEquals(expression.toString(), "{field: abc, type: null}");
        Assert.assertTrue(expression.getEvaluator() instanceof FieldEvaluator);

        expression = new FieldExpression("abc", 0);
        Assert.assertEquals(expression.getName(), "abc.0");
        Assert.assertEquals(expression.toString(), "{field: abc, key: 0, type: null}");
        Assert.assertTrue(expression.getEvaluator() instanceof FieldEvaluator);

        expression = new FieldExpression("abc", 0, "def");
        Assert.assertEquals(expression.getName(), "abc.0.def");
        Assert.assertEquals(expression.toString(), "{field: abc, key: 0, subKey: def, type: null}");
        Assert.assertTrue(expression.getEvaluator() instanceof FieldEvaluator);

        expression = new FieldExpression("abc", "def");
        Assert.assertEquals(expression.getName(), "abc.def");
        Assert.assertEquals(expression.toString(), "{field: abc, key: def, type: null}");
        Assert.assertTrue(expression.getEvaluator() instanceof FieldEvaluator);

        expression = new FieldExpression("abc", "def", "ghi");
        Assert.assertEquals(expression.getName(), "abc.def.ghi");
        Assert.assertEquals(expression.toString(), "{field: abc, key: def, subKey: ghi, type: null}");
        Assert.assertTrue(expression.getEvaluator() instanceof FieldEvaluator);
    }

    @Test
    public void testAdditiveConstructors() {

    }

    @Test
    public void testConstructorThrows() {

    }

    @Test
    public void testEqualsAndHashCode() {
        FieldExpression expression = new FieldExpression("abc", 0, "def");
        expression.setType(Type.INTEGER);

        ExpressionUtils.testEqualsAndHashCode(() -> new FieldExpression("abc", 0, "def"),
                                              new FieldExpression("def", 0, "def"),
                                              new FieldExpression("abc", 1, "def"),
                                              new FieldExpression("abc", 0, "abc"),
                                              expression);

        expression = new FieldExpression("abc", "def", "ghi");
        expression.setType(Type.INTEGER);

        ExpressionUtils.testEqualsAndHashCode(() -> new FieldExpression("abc", "def", "ghi"),
                                              new FieldExpression("def", "def", "ghi"),
                                              new FieldExpression("abc", "abc", "ghi"),
                                              new FieldExpression("abc", "def", "abc"),
                                              expression);
    }
}
