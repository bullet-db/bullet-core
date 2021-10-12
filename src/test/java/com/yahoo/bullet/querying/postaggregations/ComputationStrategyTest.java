/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.postaggregations;

import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class ComputationStrategyTest {
    @Test
    public void testComputation() {
        // Computations are done using the fields in the original record and then the new fields are written at the end.
        // a = 2 * a + 5 * b
        // b = a * b
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("a", new BinaryExpression(new BinaryExpression(new ValueExpression(2),
                                                                            new FieldExpression("a"),
                                                                            Operation.MUL),
                                                       new BinaryExpression(new ValueExpression(5),
                                                                            new FieldExpression("b"),
                                                                            Operation.MUL),
                                                       Operation.ADD)));
        fields.add(new Field("b", new BinaryExpression(new FieldExpression("a"), new FieldExpression("b"), Operation.MUL)));

        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2).add("c", 0).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).add("c", 1).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).add("c", 2).getRecord());

        Clip clip = new Clip();
        clip.add(records);

        ComputationStrategy strategy = (ComputationStrategy) new Computation(fields).getPostStrategy();
        Clip result = strategy.execute(clip);

        Assert.assertEquals(result.getRecords().size(), 3);
        Assert.assertEquals(result.getRecords().get(0).typedGet("a").getValue(), 20);
        Assert.assertEquals(result.getRecords().get(0).typedGet("b").getValue(), 10);
        Assert.assertEquals(result.getRecords().get(0).typedGet("c").getValue(), 0);
        Assert.assertEquals(result.getRecords().get(1).typedGet("a").getValue(), 24);
        Assert.assertEquals(result.getRecords().get(1).typedGet("b").getValue(), 8);
        Assert.assertEquals(result.getRecords().get(1).typedGet("c").getValue(), 1);
        Assert.assertEquals(result.getRecords().get(2).typedGet("a").getValue(), 37);
        Assert.assertEquals(result.getRecords().get(2).typedGet("b").getValue(), 7);
        Assert.assertEquals(result.getRecords().get(2).typedGet("c").getValue(), 2);
    }
}
