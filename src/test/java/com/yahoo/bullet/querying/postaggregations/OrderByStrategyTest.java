/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.postaggregations;

import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OrderByStrategyTest {
    private OrderByStrategy makeOrderBy(List<OrderBy.SortItem> sortItems) {
        return (OrderByStrategy) new OrderBy(sortItems).getPostStrategy();
    }

    @Test
    public void testOrderByAscending() {
        OrderByStrategy orderByStrategy = makeOrderBy(Arrays.asList(new OrderBy.SortItem(new FieldExpression("a"), OrderBy.Direction.ASC),
                                                                    new OrderBy.SortItem(new FieldExpression("b"), OrderBy.Direction.ASC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).typedGet("a").getValue(), 1);
        Assert.assertEquals(result.getRecords().get(0).typedGet("b").getValue(), 7);
        Assert.assertEquals(result.getRecords().get(1).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(1).typedGet("b").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(2).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(2).typedGet("b").getValue(), 4);
        Assert.assertEquals(result.getRecords().get(3).typedGet("a").getValue(), 5);
        Assert.assertEquals(result.getRecords().get(3).typedGet("b").getValue(), 2);
    }

    @Test
    public void testOrderByDescending() {
        OrderByStrategy orderByStrategy = makeOrderBy(Arrays.asList(new OrderBy.SortItem(new FieldExpression("a"), OrderBy.Direction.DESC),
                                                                    new OrderBy.SortItem(new FieldExpression("b"), OrderBy.Direction.DESC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).typedGet("a").getValue(), 5);
        Assert.assertEquals(result.getRecords().get(0).typedGet("b").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(1).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(1).typedGet("b").getValue(), 4);
        Assert.assertEquals(result.getRecords().get(2).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(2).typedGet("b").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(3).typedGet("a").getValue(), 1);
        Assert.assertEquals(result.getRecords().get(3).typedGet("b").getValue(), 7);
    }

    @Test
    public void testOrderByAscendingWithDifferentType() {
        OrderByStrategy orderByStrategy = makeOrderBy(Arrays.asList(new OrderBy.SortItem(new FieldExpression("a"), OrderBy.Direction.ASC),
                                                                    new OrderBy.SortItem(new FieldExpression("b"), OrderBy.Direction.ASC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).typedGet("a").getValue(), 1);
        Assert.assertEquals(result.getRecords().get(0).typedGet("b").getValue(), 7);
        Assert.assertEquals(result.getRecords().get(1).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(1).typedGet("b").getValue(), 2.0);
        Assert.assertEquals(result.getRecords().get(2).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(2).typedGet("b").getValue(), 4);
        Assert.assertEquals(result.getRecords().get(3).typedGet("a").getValue(), 5);
        Assert.assertEquals(result.getRecords().get(3).typedGet("b").getValue(), 2.0);
    }

    @Test
    public void testOrderByAscendingWithNonExistingField() {
        OrderByStrategy orderByStrategy = makeOrderBy(Arrays.asList(new OrderBy.SortItem(new FieldExpression("a"), OrderBy.Direction.ASC),
                                                                    new OrderBy.SortItem(new FieldExpression("c"), OrderBy.Direction.ASC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).typedGet("a").getValue(), 1);
        Assert.assertEquals(result.getRecords().get(0).typedGet("b").getValue(), 7);
        Assert.assertEquals(result.getRecords().get(1).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(1).typedGet("b").getValue(), 4);
        Assert.assertEquals(result.getRecords().get(2).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(2).typedGet("b").getValue(), 2.0);
        Assert.assertEquals(result.getRecords().get(3).typedGet("a").getValue(), 5);
        Assert.assertEquals(result.getRecords().get(3).typedGet("b").getValue(), 2.0);
    }

    @Test
    public void testOrderByAscendingWithSomeMissingField() {
        OrderByStrategy orderByStrategy = makeOrderBy(Arrays.asList(new OrderBy.SortItem(new FieldExpression("a"), OrderBy.Direction.ASC),
                                                                    new OrderBy.SortItem(new FieldExpression("c"), OrderBy.Direction.ASC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).typedGet("a").getValue(), null);
        Assert.assertEquals(result.getRecords().get(0).typedGet("b").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(1).typedGet("a").getValue(), 1);
        Assert.assertEquals(result.getRecords().get(1).typedGet("b").getValue(), 7);
        Assert.assertEquals(result.getRecords().get(2).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(2).typedGet("b").getValue(), 4);
        Assert.assertEquals(result.getRecords().get(3).typedGet("a").getValue(), 5);
        Assert.assertEquals(result.getRecords().get(3).typedGet("b").getValue(), 2);
    }

    @Test
    public void testOrderByComputationWithSingleField() {
        OrderByStrategy orderByStrategy = makeOrderBy(Arrays.asList(new OrderBy.SortItem(new BinaryExpression(new FieldExpression("a"), new ValueExpression(-1), Operation.MUL), OrderBy.Direction.ASC),
                                                                    new OrderBy.SortItem(new BinaryExpression(new FieldExpression("b"), new ValueExpression(-1), Operation.MUL), OrderBy.Direction.ASC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).typedGet("a").getValue(), 5);
        Assert.assertEquals(result.getRecords().get(0).typedGet("b").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(1).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(1).typedGet("b").getValue(), 4);
        Assert.assertEquals(result.getRecords().get(2).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(2).typedGet("b").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(3).typedGet("a").getValue(), 1);
        Assert.assertEquals(result.getRecords().get(3).typedGet("b").getValue(), 7);
    }

    @Test
    public void testOrderByComputationWithMultipleFields() {
        OrderByStrategy orderByStrategy = makeOrderBy(Collections.singletonList(new OrderBy.SortItem(new BinaryExpression(new FieldExpression("a"), new FieldExpression("b"), Operation.ADD), OrderBy.Direction.DESC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).typedGet("a").getValue(), 1);
        Assert.assertEquals(result.getRecords().get(0).typedGet("b").getValue(), 7);
        Assert.assertEquals(result.getRecords().get(1).typedGet("a").getValue(), 5);
        Assert.assertEquals(result.getRecords().get(1).typedGet("b").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(2).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(2).typedGet("b").getValue(), 4);
        Assert.assertEquals(result.getRecords().get(3).typedGet("a").getValue(), 2);
        Assert.assertEquals(result.getRecords().get(3).typedGet("b").getValue(), 2);
    }

    @Test
    public void testOrderByComputationWithMissingField() {
        OrderByStrategy orderByStrategy = makeOrderBy(Collections.singletonList(new OrderBy.SortItem(new UnaryExpression(new FieldExpression("a"), Operation.SIZE_OF), OrderBy.Direction.ASC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", "hello").getRecord());
        records.add(RecordBox.get().add("a", "").getRecord());
        records.add(RecordBox.get().add("a", "foobar").getRecord());
        records.add(RecordBox.get().getRecord());
        records.add(RecordBox.get().add("a", "world").getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).typedGet("a").getValue(), null);
        Assert.assertEquals(result.getRecords().get(1).typedGet("a").getValue(), "");
        Assert.assertEquals(result.getRecords().get(2).typedGet("a").getValue(), "hello");
        Assert.assertEquals(result.getRecords().get(3).typedGet("a").getValue(), "world");
        Assert.assertEquals(result.getRecords().get(4).typedGet("a").getValue(), "foobar");
    }

    @Test
    public void testOrderByComputationWithBadComputation() {
        OrderByStrategy orderByStrategy = makeOrderBy(Collections.singletonList(new OrderBy.SortItem(new UnaryExpression(new FieldExpression("a"), Operation.SIZE_OF), OrderBy.Direction.ASC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 1).getRecord());
        records.add(RecordBox.get().add("a", 3).getRecord());
        records.add(RecordBox.get().add("a", 2).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        // Order should not change
        Assert.assertEquals(result.getRecords().get(0).typedGet("a").getValue(), 1);
        Assert.assertEquals(result.getRecords().get(1).typedGet("a").getValue(), 3);
        Assert.assertEquals(result.getRecords().get(2).typedGet("a").getValue(), 2);
    }
}
