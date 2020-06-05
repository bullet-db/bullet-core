/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.querying.evaluators.ListEvaluator;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class ListExpressionTest {
    @Test
    public void testConstructor() {
        ListExpression expression = new ListExpression(Arrays.asList(new ValueExpression(1), new ValueExpression(2)));
        Assert.assertEquals(expression.toString(), "{values: [{value: 1, type: INTEGER}, {value: 2, type: INTEGER}], type: null}");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorThrows() {
        new ListExpression(null);
    }

    @Test
    public void testGetEvaluator() {
        Assert.assertTrue(new ListExpression(new ArrayList<>()).getEvaluator() instanceof ListEvaluator);
    }

    @Test
    public void testEqualsAndHashCode() {
        ListExpression expression = new ListExpression(new ArrayList<>());
        expression.setType(Type.INTEGER_LIST);

        ExpressionUtils.testEqualsAndHashCode(() -> new ListExpression(new ArrayList<>()),
                                              new ListExpression(Arrays.asList(new ValueExpression(1))),
                                              expression);
    }
}
