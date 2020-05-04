/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.NAryExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

public class NAryEvaluatorTest {
    @Test
    public void testConstructor() {
        NAryExpression expression = new NAryExpression(Arrays.asList(new ValueExpression(false),
                                                                     new ValueExpression(1),
                                                                     new ValueExpression(2)),
                                                       Operation.IF);
        expression.setType(Type.INTEGER);

        NAryEvaluator evaluator = new NAryEvaluator(expression);
        Assert.assertTrue(evaluator.getOperands().get(0) instanceof ValueEvaluator);
        Assert.assertTrue(evaluator.getOperands().get(1) instanceof ValueEvaluator);
        Assert.assertTrue(evaluator.getOperands().get(2) instanceof ValueEvaluator);
        Assert.assertEquals(evaluator.getOp(), NAryOperations.IF);
        Assert.assertEquals(evaluator.getType(), Type.INTEGER);
        Assert.assertEquals(evaluator.evaluate(RecordBox.get().getRecord()), new TypedObject(Type.INTEGER, 2));
    }
}
