/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.querying.evaluators.FieldEvaluator;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ComplexFieldExpressionTest {
    @Test
    public void testConstructor() {
        ComplexFieldExpression expression = new ComplexFieldExpression("abc", new ValueExpression(0));
        Assert.assertEquals(expression.toString(), "{field: abc, key: {value: 0, type: INTEGER}, subKey: null, type: null}");
        Assert.assertTrue(expression.getEvaluator() instanceof FieldEvaluator);

        expression = new ComplexFieldExpression("abc", new ValueExpression(0), new ValueExpression("def"));
        Assert.assertEquals(expression.toString(), "{field: abc, key: {value: 0, type: INTEGER}, subKey: {value: 'def', type: STRING}, type: null}");
        Assert.assertTrue(expression.getEvaluator() instanceof FieldEvaluator);
    }

    @Test
    public void testEqualsAndHashCode() {
        ComplexFieldExpression expression = new ComplexFieldExpression("abc", new ValueExpression(0), new ValueExpression("def"));
        expression.setType(Type.INTEGER);

        ExpressionUtils.testEqualsAndHashCode(() -> new ComplexFieldExpression("abc", new ValueExpression(0), new ValueExpression("def")),
                                              new ComplexFieldExpression("def", new ValueExpression(0), new ValueExpression("def")),
                                              new ComplexFieldExpression("abc", new ValueExpression(1), new ValueExpression("def")),
                                              new ComplexFieldExpression("abc", new ValueExpression(0), new ValueExpression("abc")),
                                              expression);
    }
}
