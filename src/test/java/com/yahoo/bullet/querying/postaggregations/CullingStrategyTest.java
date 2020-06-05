/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.postaggregations;

import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CullingStrategyTest {
    @Test
    public void testCulling() {
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 1).add("b", 2).add("c", 3).getRecord());
        records.add(RecordBox.get().add("a", 4).add("b", 5).add("c", 6).getRecord());
        records.add(RecordBox.get().add("a", 7).add("b", 8).add("c", 9).getRecord());

        Clip clip = new Clip();
        clip.add(records);

        Set<String> transientFields = new HashSet<>(Arrays.asList("a", "b"));
        CullingStrategy strategy = (CullingStrategy) new Culling(transientFields).getPostStrategy();
        Clip result = strategy.execute(clip);

        Assert.assertEquals(result.getRecords().size(), 3);
        Assert.assertTrue(result.getRecords().get(0).typedGet("a").isNull());
        Assert.assertTrue(result.getRecords().get(0).typedGet("b").isNull());
        Assert.assertEquals(result.getRecords().get(0).typedGet("c").getValue(), 3);
        Assert.assertTrue(result.getRecords().get(1).typedGet("a").isNull());
        Assert.assertTrue(result.getRecords().get(1).typedGet("b").isNull());
        Assert.assertEquals(result.getRecords().get(1).typedGet("c").getValue(), 6);
        Assert.assertTrue(result.getRecords().get(2).typedGet("a").isNull());
        Assert.assertTrue(result.getRecords().get(2).typedGet("b").isNull());
        Assert.assertEquals(result.getRecords().get(2).typedGet("c").getValue(), 9);
    }
}
