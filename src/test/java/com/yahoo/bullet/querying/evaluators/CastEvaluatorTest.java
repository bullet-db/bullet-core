/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.CastExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CastEvaluatorTest {
    @Test
    public void testConstructor() {
        CastExpression expression = new CastExpression(new ValueExpression(5), Type.STRING);
        expression.setType(Type.STRING);

        CastEvaluator evaluator = new CastEvaluator(expression);
        Assert.assertTrue(evaluator.value instanceof ValueEvaluator);
        Assert.assertEquals(evaluator.castType, Type.STRING);
        Assert.assertEquals(evaluator.evaluate(RecordBox.get().getRecord()), new TypedObject(Type.STRING, "5"));
    }
}
