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

import static com.yahoo.bullet.querying.evaluators.BinaryOperations.BINARY_OPERATORS;

public class BinaryEvaluatorTest {
    @Test
    public void testConstructor() {
        BinaryExpression expression = new BinaryExpression(new ValueExpression(1), new ValueExpression(2), Operation.ADD);
        expression.setType(Type.INTEGER);

        BinaryEvaluator evaluator = new BinaryEvaluator(expression);
        Assert.assertTrue(evaluator.left instanceof ValueEvaluator);
        Assert.assertTrue(evaluator.right instanceof ValueEvaluator);
        Assert.assertEquals(evaluator.op, BINARY_OPERATORS.get(Operation.ADD));
        Assert.assertEquals(evaluator.evaluate(RecordBox.get().getRecord()), new TypedObject(Type.INTEGER, 3));
    }
}
