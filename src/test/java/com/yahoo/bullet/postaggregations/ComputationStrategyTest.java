/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Expression;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.parsing.Value;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.yahoo.bullet.parsing.ExpressionUtils.makeBinaryExpression;
import static com.yahoo.bullet.parsing.ExpressionUtils.makeCastExpression;
import static com.yahoo.bullet.parsing.ExpressionUtils.makeLeafExpression;

public class ComputationStrategyTest {
    private ComputationStrategy makeComputation(Expression expression, String newName) {
        Computation postAggregation = new Computation();
        postAggregation.setType(PostAggregation.Type.COMPUTATION);
        postAggregation.setExpression(expression);
        postAggregation.setNewName(newName);
        postAggregation.initialize();
        ComputationStrategy computationStrategy = new ComputationStrategy(postAggregation);
        computationStrategy.initialize();
        return computationStrategy;
    }

    @Test
    public void testComputation() {
        // (1 / 1.0) * 1.0 + cast(2.0, DOUBLE) * FIELD("a") - 3
        ComputationStrategy computation = makeComputation(
                makeBinaryExpression(Expression.Operation.SUB,
                                     makeBinaryExpression(Expression.Operation.ADD,
                                                          makeBinaryExpression(Expression.Operation.MUL,
                                                                               makeBinaryExpression(Expression.Operation.DIV,
                                                                                                    makeLeafExpression(new Value(Value.Kind.VALUE, "1", Type.LONG)),
                                                                                                    makeLeafExpression(new Value(Value.Kind.VALUE, "1.0", Type.DOUBLE))),
                                                                               makeLeafExpression(new Value(Value.Kind.VALUE, "1.0", Type.DOUBLE))),
                                                          makeBinaryExpression(Expression.Operation.MUL,
                                                                               makeCastExpression(makeLeafExpression(new Value(Value.Kind.VALUE, "2.0", Type.DOUBLE)),
                                                                                                  Type.DOUBLE),
                                                                               makeLeafExpression(new Value(Value.Kind.FIELD, "a")))),
                                     makeLeafExpression(new Value(Value.Kind.VALUE, "3", Type.LONG))),
                "newName");
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).getRecord());
        records.add(RecordBox.get().add("a", 2).getRecord());
        records.add(RecordBox.get().add("a", 1).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), 8.0);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Double);
        Assert.assertEquals(result.getRecords().get(1).get("newName"), 2.0);
        Assert.assertEquals(result.getRecords().get(2).get("newName"), 0.0);
    }

    @Test
    public void testIntegerComputation() {
        // cast(2, INTEGER) - cast(5, INTEGER) * cast(3, INTEGER) + (cast(1, INTEGER) / cast(1, INTEGER))
        ComputationStrategy computation = makeComputation(
                makeBinaryExpression(Expression.Operation.ADD,
                                     makeBinaryExpression(Expression.Operation.SUB,
                                                          makeCastExpression(makeLeafExpression(new Value(Value.Kind.VALUE, "2", Type.LONG)),
                                                                             Type.INTEGER),
                                                          makeBinaryExpression(Expression.Operation.MUL,
                                                                               makeCastExpression(makeLeafExpression(new Value(Value.Kind.VALUE, "5", Type.LONG)),
                                                                                                  Type.INTEGER),
                                                                               makeCastExpression(makeLeafExpression(new Value(Value.Kind.VALUE, "3", Type.LONG)),
                                                                                                  Type.INTEGER))),
                                     makeBinaryExpression(Expression.Operation.DIV,
                                                          makeCastExpression(makeLeafExpression(new Value(Value.Kind.VALUE, "1", Type.LONG)),
                                                                             Type.INTEGER),
                                                          makeCastExpression(makeLeafExpression(new Value(Value.Kind.VALUE, "1", Type.LONG)),
                                                                             Type.INTEGER))),
                "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), -12);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Integer);
    }

    @Test
    public void testLongComputation() {
        // 2 - 5 * 3 + 1 / 1
        ComputationStrategy computation = makeComputation(
                makeBinaryExpression(Expression.Operation.ADD,
                                     makeBinaryExpression(Expression.Operation.SUB,
                                                          makeLeafExpression(new Value(Value.Kind.VALUE, "2", Type.LONG)),
                                                          makeBinaryExpression(Expression.Operation.MUL,
                                                                               makeLeafExpression(new Value(Value.Kind.VALUE, "5", Type.LONG)),
                                                                               makeLeafExpression(new Value(Value.Kind.VALUE, "3", Type.LONG)))),
                                     makeBinaryExpression(Expression.Operation.DIV,
                                                          makeLeafExpression(new Value(Value.Kind.VALUE, "1", Type.LONG)),
                                                          makeLeafExpression(new Value(Value.Kind.VALUE, "1", Type.LONG)))),
                "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), -12L);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Long);
    }

    @Test
    public void testFloatComputation() {
        // cast(2.0, FLOAT) - cast(5.0, FLOAT) * cast(3.0, FLOAT) + (cast(1.0, FLOAT) / cast(1.0, FLOAT))
        ComputationStrategy computation = makeComputation(
                makeBinaryExpression(Expression.Operation.ADD,
                                     makeBinaryExpression(Expression.Operation.SUB,
                                                          makeCastExpression(makeLeafExpression(new Value(Value.Kind.VALUE, "2.0", Type.DOUBLE)),
                                                                             Type.FLOAT),
                                                          makeBinaryExpression(Expression.Operation.MUL,
                                                                               makeCastExpression(makeLeafExpression(new Value(Value.Kind.VALUE, "5.0", Type.DOUBLE)),
                                                                                                  Type.FLOAT),
                                                                               makeCastExpression(makeLeafExpression(new Value(Value.Kind.VALUE, "3.0", Type.DOUBLE)),
                                                                                                  Type.FLOAT))),
                                     makeBinaryExpression(Expression.Operation.DIV,
                                                          makeCastExpression(makeLeafExpression(new Value(Value.Kind.VALUE, "1.0", Type.DOUBLE)),
                                                                             Type.FLOAT),
                                                          makeCastExpression(makeLeafExpression(new Value(Value.Kind.VALUE, "1.0", Type.DOUBLE)),
                                                                             Type.FLOAT))),
                "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), -12.0f);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Float);
    }

    @Test
    public void testDoubleComputation() {
        // 2.0 - 5.0 * 3.0 + 1.0 / 1.0
        ComputationStrategy computation = makeComputation(
                makeBinaryExpression(Expression.Operation.ADD,
                                     makeBinaryExpression(Expression.Operation.SUB,
                                                          makeLeafExpression(new Value(Value.Kind.VALUE, "2.0", Type.DOUBLE)),
                                                          makeBinaryExpression(Expression.Operation.MUL,
                                                                               makeLeafExpression(new Value(Value.Kind.VALUE, "5.0", Type.DOUBLE)),
                                                                               makeLeafExpression(new Value(Value.Kind.VALUE, "3.0", Type.DOUBLE)))),
                                     makeBinaryExpression(Expression.Operation.DIV,
                                                          makeLeafExpression(new Value(Value.Kind.VALUE, "1.0", Type.DOUBLE)),
                                                          makeLeafExpression(new Value(Value.Kind.VALUE, "1.0", Type.DOUBLE)))),
                "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), -12.0);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Double);
    }

    @Test
    public void testBooleanComputation() {
        ComputationStrategy computation = makeComputation(makeLeafExpression(new Value(Value.Kind.VALUE, "true", Type.BOOLEAN)), "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);
        Assert.assertEquals(result.getRecords().get(0).get("newName"), true);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Boolean);
    }

    @Test
    public void testStringComputation() {
        ComputationStrategy computation = makeComputation(makeLeafExpression(new Value(Value.Kind.VALUE, "abc", Type.STRING)), "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);
        Assert.assertEquals(result.getRecords().get(0).get("newName"), "abc");
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof String);
    }

    @Test
    public void testUnsupportedComputation() {
        ComputationStrategy computation = makeComputation(
                makeBinaryExpression(Expression.Operation.ADD,
                                     makeLeafExpression(new Value(Value.Kind.VALUE, "1", Type.LONG)),
                                     makeLeafExpression(new Value(Value.Kind.VALUE, "true", Type.BOOLEAN))),
                "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertFalse(result.getRecords().get(0).hasField("newName"));
    }

    @Test
    public void testNonExistingFieldComputation() {
        ComputationStrategy computation = makeComputation(makeLeafExpression(new Value(Value.Kind.FIELD, "a")), "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertFalse(result.getRecords().get(0).hasField("newName"));
    }
}
