/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.query.postaggregations.Having;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class HavingStrategyTest {
    @Test
    public void testHaving() {
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 6).getRecord());
        records.add(RecordBox.get().add("a", 1).getRecord());
        records.add(RecordBox.get().add("a", 8).getRecord());
        records.add(RecordBox.get().add("a", 2).getRecord());
        records.add(RecordBox.get().add("b", 10).getRecord());
        records.add(RecordBox.get().add("a", 7).getRecord());

        Clip clip = new Clip();
        clip.add(records);

        // a > 5
        BinaryExpression expression = new BinaryExpression(new FieldExpression("a"), new ValueExpression(5), Operation.GREATER_THAN);
        HavingStrategy strategy = (HavingStrategy) new Having(expression).getPostStrategy();
        Clip result = strategy.execute(clip);

        Assert.assertEquals(result.getRecords().size(), 3);
        Assert.assertEquals(result.getRecords().get(0).typedGet("a").getValue(), 6);
        Assert.assertEquals(result.getRecords().get(1).typedGet("a").getValue(), 8);
        Assert.assertEquals(result.getRecords().get(2).typedGet("a").getValue(), 7);
    }
}
