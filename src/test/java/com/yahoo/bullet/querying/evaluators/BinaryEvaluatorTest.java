/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BinaryEvaluatorTest {
    @Test
    public void testConstructor() {
        BinaryExpression expression = new BinaryExpression(new ValueExpression(1), new ValueExpression(2), Operation.ADD);
        expression.setType(Type.INTEGER);

        BinaryEvaluator evaluator = new BinaryEvaluator(expression);
        Assert.assertTrue(evaluator.getLeft() instanceof ValueEvaluator);
        Assert.assertTrue(evaluator.getRight() instanceof ValueEvaluator);
        Assert.assertEquals(evaluator.getOp(), BinaryOperations.ADD);
        Assert.assertEquals(evaluator.getType(), Type.INTEGER);
        Assert.assertEquals(evaluator.evaluate(RecordBox.get().getRecord()), new TypedObject(Type.INTEGER, 3));
    }

    @Test
    public void testModifierNone() {
        BinaryEvaluator evaluator = new BinaryEvaluator(new BinaryExpression(new ValueExpression(1),
                                                                             new ValueExpression(2),
                                                                             Operation.EQUALS,
                                                                             BinaryExpression.Modifier.NONE));
        Assert.assertEquals(evaluator.getOp(), BinaryOperations.EQUALS);
    }

    @Test
    public void testModifierAll() {
        BinaryEvaluator evaluator = new BinaryEvaluator(new BinaryExpression(new ValueExpression(1),
                                                                             new ValueExpression(2),
                                                                             Operation.EQUALS,
                                                                             BinaryExpression.Modifier.ALL));
        Assert.assertEquals(evaluator.getOp(), BinaryOperations.EQUALS_ALL);
    }

    @Test
    public void testModifierAny() {
        BinaryEvaluator evaluator = new BinaryEvaluator(new BinaryExpression(new ValueExpression(1),
                                                                             new ValueExpression(2),
                                                                             Operation.EQUALS,
                                                                             BinaryExpression.Modifier.ANY));
        Assert.assertEquals(evaluator.getOp(), BinaryOperations.EQUALS_ANY);
    }
}
