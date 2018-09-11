/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

public class ComputationTest {
    @Test
    public void testToString() {
        Computation aggregation = new Computation();
        aggregation.setType(PostAggregation.Type.COMPUTATION);
        Assert.assertEquals(aggregation.toString(), "{type: COMPUTATION, expression: null, newName: null}");

        aggregation.setNewName("newName");
        Assert.assertEquals(aggregation.toString(), "{type: COMPUTATION, expression: null, newName: newName}");

        aggregation.setExpression(ExpressionUtils.makeBinaryExpression(Expression.Operation.ADD,
                                                                       ExpressionUtils.makeCastExpression(ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "1", Type.INTEGER)), Type.INTEGER),
                                                                       ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "2", Type.INTEGER))));
        Assert.assertEquals(aggregation.toString(),
                            "{type: COMPUTATION, " +
                                     "expression: {operation: ADD, " +
                                                  "left: {operation: CAST, expression: {operation: null, value: {kind: VALUE, value: 1, type: INTEGER}}, type: INTEGER}, " +
                                                  "right: {operation: null, value: {kind: VALUE, value: 2, type: INTEGER}}}, newName: newName}");
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
        Assert.assertEquals(errors.get().get(0), Computation.COMPUTATION_REQUIRES_VALID_EXPRESSION_ERROR);
    }

    @Test
    public void testInitializeWithoutNONValidBinaryExpression() {
        Computation aggregation = new Computation();
        aggregation.setType(PostAggregation.Type.COMPUTATION);
        BinaryExpression binaryExpression = new BinaryExpression();
        aggregation.setExpression(binaryExpression);
        Optional<List<BulletError>> errors = aggregation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), BinaryExpression.BINARY_EXPRESSION_REQUIRES_VALID_EXPRESSIONS_ERROR);
    }

    @Test
    public void testInitializeWithoutNONValidCastExpression() {
        Computation aggregation = new Computation();
        aggregation.setType(PostAggregation.Type.COMPUTATION);
        aggregation.setExpression(ExpressionUtils.makeBinaryExpression(Expression.Operation.ADD,
                                                                       ExpressionUtils.makeCastExpression(null, Type.INTEGER),
                                                                       ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "2", Type.INTEGER))));
        Optional<List<BulletError>> errors = aggregation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), CastExpression.CAST_EXPRESSION_REQUIRES_VALID_EXPRESSION_ERROR);

        aggregation.setExpression(ExpressionUtils.makeBinaryExpression(Expression.Operation.ADD,
                                                                       ExpressionUtils.makeCastExpression(ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "2", Type.INTEGER)), Type.MAP),
                                                                       ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "2", Type.INTEGER))));
        errors = aggregation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), CastExpression.CAST_EXPRESSION_REQUIRES_PRIMITIVE_TYPE_ERROR);
    }

    @Test
    public void testInitializeWithoutNONValidLeafExpression() {
        Computation aggregation = new Computation();
        aggregation.setType(PostAggregation.Type.COMPUTATION);
        aggregation.setExpression(ExpressionUtils.makeBinaryExpression(Expression.Operation.ADD,
                                                                       ExpressionUtils.makeCastExpression(ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "2", Type.MAP)), Type.INTEGER),
                                                                       ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "2"))));
        Optional<List<BulletError>> errors = aggregation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), LeafExpression.LEAF_EXPRESSION_REQUIRES_PRIMITIVE_TYPE_ERROR);
    }

    @Test
    public void testInitializeWithoutNewFieldName() {
        Computation aggregation = new Computation();
        aggregation.setType(PostAggregation.Type.COMPUTATION);
        aggregation.setExpression(ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "1", Type.INTEGER)));
        Optional<List<BulletError>> errors = aggregation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), Computation.COMPUTATION_REQUIRES_NEW_FIELD_ERROR);
    }

    @Test
    public void testInitialize() {
        Computation aggregation = new Computation();
        aggregation.setType(PostAggregation.Type.COMPUTATION);
        aggregation.setExpression(ExpressionUtils.makeBinaryExpression(Expression.Operation.ADD,
                                                                       ExpressionUtils.makeCastExpression(ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "2", Type.INTEGER)), Type.INTEGER),
                                                                       ExpressionUtils.makeLeafExpression(new Value(Value.Kind.VALUE, "2", Type.INTEGER))));
        aggregation.setNewName("newName");
        Optional<List<BulletError>> errors = aggregation.initialize();
        Assert.assertFalse(errors.isPresent());
        Assert.assertEquals(aggregation.getNewName(), "newName");
    }
}
