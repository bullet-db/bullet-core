/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UnaryEvaluatorTest {
    @Test
    public void testConstructor() {
        UnaryExpression expression = new UnaryExpression(new ValueExpression(1), Operation.IS_NOT_NULL);
        expression.setType(Type.BOOLEAN);

        UnaryEvaluator evaluator = new UnaryEvaluator(expression);
        Assert.assertTrue(evaluator.getOperand() instanceof ValueEvaluator);
        Assert.assertEquals(evaluator.getOp(), UnaryOperations.IS_NOT_NULL);
        Assert.assertEquals(evaluator.getType(), Type.BOOLEAN);
        Assert.assertEquals(evaluator.evaluate(RecordBox.get().getRecord()), new TypedObject(Type.BOOLEAN, true));
    }
}
