/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.BulletException;
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
    public void testConstructorWithIndex() {
        Assert.assertEquals(new FieldExpression(new FieldExpression("abc"), 0), new FieldExpression("abc", 0));
    }

    @Test
    public void testConstructorWithKey() {
        Assert.assertEquals(new FieldExpression(new FieldExpression("abc"), "def"), new FieldExpression("abc", "def"));
        Assert.assertEquals(new FieldExpression(new FieldExpression("abc", "def"), "ghi"), new FieldExpression("abc", "def", "ghi"));
    }

    @Test
    public void testConstructorWithVariableKey() {
        Assert.assertEquals(new FieldExpression(new FieldExpression("abc"), new ValueExpression("def")), new FieldExpression("abc", new ValueExpression("def")));
        Assert.assertEquals(new FieldExpression(new FieldExpression("abc", "def"), new ValueExpression("ghi")), new FieldExpression("abc", "def", new ValueExpression("ghi")));
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The field expression already has a key and cannot accept an index\\.")
    public void testConstructorNotAnotherIndex() {
        new FieldExpression(new FieldExpression("abc", "def"), 0);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The field expression already has a key and subkey and cannot accept another key\\.")
    public void testConstructorNotAnotherKey() {
        new FieldExpression(new FieldExpression("abc", "def", "ghi"), "jkl");
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The field expression already has a key and subkey and cannot accept another key\\.")
    public void testConstructorNotAnotherVariableKey() {
        new FieldExpression(new FieldExpression("abc", "def", "ghi"), new ValueExpression("jkl"));
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
