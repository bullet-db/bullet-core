/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.ListExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class ListEvaluatorTest {
    @Test
    public void testConstructor() {
        ListExpression expression = new ListExpression(Arrays.asList(new ValueExpression(1), new ValueExpression(2)));
        expression.setType(Type.INTEGER_LIST);

        ListEvaluator evaluator = new ListEvaluator(expression);
        Assert.assertTrue(evaluator.evaluators.get(0) instanceof ValueEvaluator);
        Assert.assertTrue(evaluator.evaluators.get(1) instanceof ValueEvaluator);
        Assert.assertEquals(evaluator.type, Type.INTEGER_LIST);
        Assert.assertEquals(evaluator.evaluate(RecordBox.get().getRecord()), new TypedObject(Type.INTEGER_LIST, new ArrayList<>(Arrays.asList(1, 2))));
    }
}
