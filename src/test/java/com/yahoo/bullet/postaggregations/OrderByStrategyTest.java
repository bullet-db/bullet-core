/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.parsing.OrderBy;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OrderByStrategyTest {
    private OrderByStrategy makeOrderBy(List<OrderBy.SortItem> sortItems) {
        OrderBy orderBy = new OrderBy();
        orderBy.setType(PostAggregation.Type.ORDER_BY);
        orderBy.setFields(sortItems);
        return new OrderByStrategy(orderBy);
    }

    @Test
    public void testOrderByAscending() {
        OrderByStrategy orderByStrategy = makeOrderBy(Arrays.asList(new OrderBy.SortItem("a", OrderBy.Direction.ASC), new OrderBy.SortItem("b", OrderBy.Direction.ASC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("a"), 1);
        Assert.assertEquals(result.getRecords().get(0).get("b"), 7);
        Assert.assertEquals(result.getRecords().get(1).get("a"), 2);
        Assert.assertEquals(result.getRecords().get(1).get("b"), 2);
        Assert.assertEquals(result.getRecords().get(2).get("a"), 2);
        Assert.assertEquals(result.getRecords().get(2).get("b"), 4);
        Assert.assertEquals(result.getRecords().get(3).get("a"), 5);
        Assert.assertEquals(result.getRecords().get(3).get("b"), 2);
    }

    @Test
    public void testOrderByDescending() {
        OrderByStrategy orderByStrategy = makeOrderBy(Arrays.asList(new OrderBy.SortItem("a", OrderBy.Direction.DESC), new OrderBy.SortItem("b", OrderBy.Direction.DESC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("a"), 5);
        Assert.assertEquals(result.getRecords().get(0).get("b"), 2);
        Assert.assertEquals(result.getRecords().get(1).get("a"), 2);
        Assert.assertEquals(result.getRecords().get(1).get("b"), 4);
        Assert.assertEquals(result.getRecords().get(2).get("a"), 2);
        Assert.assertEquals(result.getRecords().get(2).get("b"), 2);
        Assert.assertEquals(result.getRecords().get(3).get("a"), 1);
        Assert.assertEquals(result.getRecords().get(3).get("b"), 7);
    }

    @Test
    public void testOrderByAscendingWithDifferentType() {
        OrderByStrategy orderByStrategy = makeOrderBy(Arrays.asList(new OrderBy.SortItem("a", OrderBy.Direction.ASC), new OrderBy.SortItem("b", OrderBy.Direction.ASC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("a"), 1);
        Assert.assertEquals(result.getRecords().get(0).get("b"), 7);
        Assert.assertEquals(result.getRecords().get(1).get("a"), 2);
        Assert.assertEquals(result.getRecords().get(1).get("b"), 4);
        Assert.assertEquals(result.getRecords().get(2).get("a"), 2);
        Assert.assertEquals(result.getRecords().get(2).get("b"), 2.0);
        Assert.assertEquals(result.getRecords().get(3).get("a"), 5);
        Assert.assertEquals(result.getRecords().get(3).get("b"), 2.0);
    }

    @Test
    public void testOrderByAscendingWithNonExistingField() {
        OrderByStrategy orderByStrategy = makeOrderBy(Arrays.asList(new OrderBy.SortItem("a", OrderBy.Direction.ASC), new OrderBy.SortItem("c", OrderBy.Direction.ASC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("a"), 1);
        Assert.assertEquals(result.getRecords().get(0).get("b"), 7);
        Assert.assertEquals(result.getRecords().get(1).get("a"), 2);
        Assert.assertEquals(result.getRecords().get(1).get("b"), 4);
        Assert.assertEquals(result.getRecords().get(2).get("a"), 2);
        Assert.assertEquals(result.getRecords().get(2).get("b"), 2.0);
        Assert.assertEquals(result.getRecords().get(3).get("a"), 5);
        Assert.assertEquals(result.getRecords().get(3).get("b"), 2.0);
    }

    @Test
    public void testOrderByAscendingWithSomeMissingField() {
        OrderByStrategy orderByStrategy = makeOrderBy(Arrays.asList(new OrderBy.SortItem("a", OrderBy.Direction.ASC), new OrderBy.SortItem("c", OrderBy.Direction.ASC)));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderByStrategy.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("a"), null);
        Assert.assertEquals(result.getRecords().get(0).get("b"), 2);
        Assert.assertEquals(result.getRecords().get(1).get("a"), 1);
        Assert.assertEquals(result.getRecords().get(1).get("b"), 7);
        Assert.assertEquals(result.getRecords().get(2).get("a"), 2);
        Assert.assertEquals(result.getRecords().get(2).get("b"), 4);
        Assert.assertEquals(result.getRecords().get(3).get("a"), 5);
        Assert.assertEquals(result.getRecords().get(3).get("b"), 2);
    }
}
