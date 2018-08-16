/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OrderByTest {
    private OrderBy makeOrderBy(List<String> fields) {
        return makeOrderBy(fields, "ASC");
    }

    private OrderBy makeOrderBy(Object fields, Object direction) {
        PostAggregation postAggregation = new PostAggregation();
        postAggregation.setType(PostAggregation.Type.ORDER_BY);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("fields", fields);
        attributes.put("direction", direction);
        postAggregation.setAttributes(attributes);
        return new OrderBy(postAggregation);
    }

    @Test
    public void testInitializeWithoutAttributes() {
        PostAggregation postAggregation = new PostAggregation();
        postAggregation.setType(PostAggregation.Type.ORDER_BY);
        PostStrategy orderBy = new OrderBy(postAggregation);
        Optional<List<BulletError>> errors = orderBy.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), OrderBy.ORDERBY_REQUIRES_FIELDS_ERROR);
    }

    @Test
    public void testInitializeWithoutFields() {
        PostAggregation postAggregation = new PostAggregation();
        postAggregation.setType(PostAggregation.Type.ORDER_BY);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("foo", "foo");
        postAggregation.setAttributes(attributes);
        PostStrategy orderBy = new OrderBy(postAggregation);
        Optional<List<BulletError>> errors = orderBy.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), OrderBy.ORDERBY_REQUIRES_FIELDS_ERROR);
    }

    @Test
    public void testInitializeWithNonListFields() {
        OrderBy orderBy = makeOrderBy("foo", "aa");
        Optional<List<BulletError>> errors = orderBy.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), OrderBy.ORDERBY_REQUIRES_FIELDS_ERROR);
    }

    @Test
    public void testInitializeWithUnrecognizedDirection() {
        OrderBy orderBy = makeOrderBy(Arrays.asList("a", "b"), "aa");
        Optional<List<BulletError>> errors = orderBy.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), OrderBy.ORDERBY_UNKNOWN_DIRECTION_ERROR);
    }

    @Test
    public void testInitialize() {
        OrderBy orderBy = makeOrderBy(Arrays.asList("a", "b"), "DESC");
        Optional<List<BulletError>> errors = orderBy.initialize();
        Assert.assertFalse(errors.isPresent());
        Assert.assertEquals(orderBy.getFields(), Arrays.asList("a", "b"));
        Assert.assertEquals(orderBy.getDirection(), OrderBy.Direction.DESC);
    }

    @Test
    public void testOrderByAscending() {
        OrderBy orderBy = makeOrderBy(Arrays.asList("a", "b"));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderBy.execute(clip);

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
    public void testOrderByAscendingWithDifferentType() {
        OrderBy orderBy = makeOrderBy(Arrays.asList("a", "b"));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderBy.execute(clip);

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
        OrderBy orderBy = makeOrderBy(Arrays.asList("a", "c"));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 2.0).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderBy.execute(clip);

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
        OrderBy orderBy = makeOrderBy(Arrays.asList("a", "c"));
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", 4).getRecord());
        records.add(RecordBox.get().add("b", 2).getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", 7).getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = orderBy.execute(clip);

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
