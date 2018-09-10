/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

public class ComputationTest {
    @Test
    public void testToString() {
        Computation aggregation = new Computation();
        aggregation.setType(PostAggregation.Type.COMPUTATION);
        Assert.assertEquals(aggregation.toString(), "{type: COMPUTATION, expression: null, newFieldName: null}");

        aggregation.setNewFieldName("newName");
        Assert.assertEquals(aggregation.toString(), "{type: COMPUTATION, expression: null, newFieldName: newName}");

        aggregation.setExpression(ExpressionUtils.makeBinaryExpression(Expression.Operation.ADD,
                                                                       ExpressionUtils.makeCastExpression(ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "1")), CastExpression.CastType.INTEGER),
                                                                       ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "2"))));
        Assert.assertEquals(aggregation.toString(),
                            "{type: COMPUTATION, " +
                                     "expression: {operation: ADD, " +
                                                  "leftExpression: {operation: CAST, expression: {operation: null, value: {kind: VALUE, value: 1}}, type: INTEGER}, " +
                                                  "rightExpression: {operation: null, value: {kind: VALUE, value: 2}}}, newFieldName: newName}");
    }

    @Test
    public void testInitializeWithoutType() {
        Computation aggregation = new Computation();
        Optional<List<BulletError>> errors = aggregation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), PostAggregation.TYPE_MISSING);
    }

    @Test
    public void testInitializeWithoutExpression() {
        Computation aggregation = new Computation();
        aggregation.setType(PostAggregation.Type.COMPUTATION);
        Optional<List<BulletError>> errors = aggregation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), Computation.COMPUTATION_REQUIRES_EXPRESSION_ERROR);
    }

    @Test
    public void testInitializeWithoutNewFieldName() {
        Computation aggregation = new Computation();
        aggregation.setType(PostAggregation.Type.COMPUTATION);
        aggregation.setExpression(ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "1")));
        Optional<List<BulletError>> errors = aggregation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), Computation.COMPUTATION_REQUIRES_NEW_FIELD_ERROR);
    }

    @Test
    public void testInitialize() {
        Computation aggregation = new Computation();
        aggregation.setType(PostAggregation.Type.COMPUTATION);
        aggregation.setExpression(ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "1")));
        aggregation.setNewFieldName("newName");
        Optional<List<BulletError>> errors = aggregation.initialize();
        Assert.assertFalse(errors.isPresent());
        Assert.assertEquals(aggregation.getNewFieldName(), "newName");
        Assert.assertEquals(aggregation.getExpression().toString(), "{operation: null, value: {kind: VALUE, value: 1}}");
    }
}
