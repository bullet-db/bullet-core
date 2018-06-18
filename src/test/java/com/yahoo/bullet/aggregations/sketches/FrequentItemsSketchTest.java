/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.sketches;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.frequencies.ErrorType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class FrequentItemsSketchTest {
    private static BulletRecordProvider provider = new BulletConfig().getBulletRecordProvider();

    private static final Map<String, String> ALL_METADATA = new HashMap<>();
    static {
        ALL_METADATA.put(Meta.Concept.SKETCH_ESTIMATED_RESULT.getName(), "isEst");
        ALL_METADATA.put(Meta.Concept.SKETCH_FAMILY.getName(), "family");
        ALL_METADATA.put(Meta.Concept.SKETCH_SIZE.getName(), "size");
        ALL_METADATA.put(Meta.Concept.SKETCH_MAXIMUM_COUNT_ERROR.getName(), "error");
        ALL_METADATA.put(Meta.Concept.SKETCH_ITEMS_SEEN.getName(), "n");
        ALL_METADATA.put(Meta.Concept.SKETCH_ACTIVE_ITEMS.getName(), "actives");
    }

    @Test(expectedExceptions = SketchesArgumentException.class, expectedExceptionsMessageRegExp = ".*power of 2.*")
    public void testBadCreation() {
        new FrequentItemsSketch(ErrorType.NO_FALSE_NEGATIVES, 12, 1, provider);
    }

    @Test
    public void testExactCounting() {
        FrequentItemsSketch sketch = new FrequentItemsSketch(ErrorType.NO_FALSE_NEGATIVES, 32, 15, provider);
        IntStream.range(0, 10).forEach(i -> IntStream.range(0, 10).forEach(j -> sketch.update(String.valueOf(i))));
        sketch.update("foo");
        IntStream.range(10, 100).forEach(i -> sketch.update("bar"));
        sketch.update("baz");

        Clip result = sketch.getResult("meta", ALL_METADATA);
        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");

        Assert.assertEquals(metadata.size(), 5);

        Assert.assertFalse((Boolean) metadata.get("isEst"));
        Assert.assertEquals((String) metadata.get("family"), Family.FREQUENCY.getFamilyName());
        Assert.assertNull(metadata.get("size"));
        Assert.assertEquals(metadata.get("error"), 0L);
        Assert.assertEquals(metadata.get("n"), 192L);
        Assert.assertEquals(metadata.get("actives"), 13);

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 13);
        for (BulletRecord actual : records) {
            String item = actual.get(FrequentItemsSketch.ITEM_FIELD).toString();
            Assert.assertEquals(actual.fieldCount(), 2);
            if ("bar".equals(item)) {
                Assert.assertEquals(actual.get(FrequentItemsSketch.COUNT_FIELD), 90L);
            } else if ("foo".equals(item) || "baz".equals(item)) {
                Assert.assertEquals(actual.get(FrequentItemsSketch.COUNT_FIELD), 1L);
            } else if (Integer.valueOf(item) < 10) {
                Assert.assertEquals(actual.get(FrequentItemsSketch.COUNT_FIELD), 10L);
            } else {
                Assert.fail("This should not be a case");
            }
        }

        Assert.assertEquals(sketch.getRecords(), result.getRecords());
        Assert.assertEquals(sketch.getMetadata("meta", ALL_METADATA).asMap(), result.getMeta().asMap());
    }

    @Test
    public void testSizeLimiting() {
        FrequentItemsSketch sketch = new FrequentItemsSketch(ErrorType.NO_FALSE_NEGATIVES, 32, 10, provider);
        // For i from 1 to 13, update the sketch i times
        IntStream.range(1, 13).forEach(i -> IntStream.range(0, i).forEach(j -> sketch.update(String.valueOf(i))));

        Clip result = sketch.getResult("meta", ALL_METADATA);
        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 5);
        Assert.assertFalse((Boolean) metadata.get("isEst"));
        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 10);
        for (BulletRecord actual : records) {
            Assert.assertEquals(actual.fieldCount(), 2);
            Integer item = Integer.valueOf(actual.get(FrequentItemsSketch.ITEM_FIELD).toString());
            // 1, 2 had the lowest and since our size is 10, we should have not seen them
            Assert.assertTrue(item > 2 && item < 13);
            Assert.assertEquals(actual.get(FrequentItemsSketch.COUNT_FIELD), Long.valueOf(item));
        }

        Assert.assertEquals(sketch.getRecords(), result.getRecords());
        Assert.assertEquals(sketch.getMetadata("meta", ALL_METADATA).asMap(), result.getMeta().asMap());
    }

    @Test
    public void testApproximateCounting() {
        FrequentItemsSketch sketch = new FrequentItemsSketch(ErrorType.NO_FALSE_POSITIVES, 32, 40, provider);
        IntStream.range(0, 40).forEach(i -> sketch.update(String.valueOf(i)));
        IntStream.of(10, 20, 25, 30).forEach(i -> IntStream.range(0, i).forEach(j -> sketch.update(String.valueOf(i))));

        Clip result = sketch.getResult("meta", ALL_METADATA);
        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 5);
        Assert.assertTrue((Boolean) metadata.get("isEst"));
        Long error = (Long) metadata.get("error");
        List<BulletRecord> records = result.getRecords();
        //  We should have our four frequent items
        Assert.assertTrue(records.size() >= 4);
        for (BulletRecord actual : records) {
            Assert.assertEquals(actual.fieldCount(), 2);
            Integer item = Integer.valueOf(actual.get(FrequentItemsSketch.ITEM_FIELD).toString());
            Long count = (Long) actual.get(FrequentItemsSketch.COUNT_FIELD);
            if (item == 10 || item == 20 || item == 25 || item == 30) {
                Assert.assertTrue(count >= item + 1);
                Assert.assertTrue(count <= item + 1 + error);
            } else {
                Assert.assertTrue(count >= 1);
                Assert.assertTrue(count <= 1 + error);
            }
        }

        Assert.assertEquals(sketch.getRecords(), result.getRecords());
        Assert.assertEquals(sketch.getMetadata("meta", ALL_METADATA).asMap(), result.getMeta().asMap());
    }

    @Test
    public void testUnioning() {
        FrequentItemsSketch sketch = new FrequentItemsSketch(ErrorType.NO_FALSE_NEGATIVES, 32, 32, provider);
        IntStream.range(0, 10).forEach(i -> IntStream.range(0, 10).forEach(j -> sketch.update(String.valueOf(i))));
        IntStream.range(10, 100).forEach(i -> sketch.update("bar"));

        FrequentItemsSketch another = new FrequentItemsSketch(ErrorType.NO_FALSE_NEGATIVES, 32, 32, provider);
        another.update("foo");
        another.update("bar");
        another.update("baz");

        FrequentItemsSketch union = new FrequentItemsSketch(ErrorType.NO_FALSE_NEGATIVES, 32, 32, provider);
        union.union(sketch.serialize());
        union.union(another.serialize());

        Clip result = union.getResult("meta", ALL_METADATA);
        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 5);
        Assert.assertFalse((Boolean) metadata.get("isEst"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 13);
        for (BulletRecord actual : records) {
            String item = actual.get(FrequentItemsSketch.ITEM_FIELD).toString();
            Assert.assertEquals(actual.fieldCount(), 2);
            if ("bar".equals(item)) {
                Assert.assertEquals(actual.get(FrequentItemsSketch.COUNT_FIELD), 91L);
            } else if ("foo".equals(item) || "baz".equals(item)) {
                Assert.assertEquals(actual.get(FrequentItemsSketch.COUNT_FIELD), 1L);
            } else if (Integer.valueOf(item) < 10) {
                Assert.assertEquals(actual.get(FrequentItemsSketch.COUNT_FIELD), 10L);
            } else {
                Assert.fail("This should not be a case");
            }
        }

        Assert.assertEquals(union.getRecords(), records);
        Assert.assertEquals(union.getMetadata("meta", ALL_METADATA).asMap(), result.getMeta().asMap());
    }

    @Test
    public void testResetting() {
        FrequentItemsSketch sketch = new FrequentItemsSketch(ErrorType.NO_FALSE_NEGATIVES, 32, 32, provider);
        IntStream.range(0, 10).forEach(i -> IntStream.range(0, 10).forEach(j -> sketch.update(String.valueOf(i))));
        IntStream.range(10, 100).forEach(i -> sketch.update("bar"));

        FrequentItemsSketch another = new FrequentItemsSketch(ErrorType.NO_FALSE_NEGATIVES, 32, 32, provider);
        another.update("foo");
        another.update("bar");
        another.update("baz");

        sketch.union(another.serialize());

        Clip result = sketch.getResult("meta", ALL_METADATA);
        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertFalse((Boolean) metadata.get("isEst"));
        Assert.assertEquals(result.getRecords().size(), 13);

        Assert.assertEquals(sketch.getRecords(), result.getRecords());
        Assert.assertEquals(sketch.getMetadata("meta", ALL_METADATA).asMap(), result.getMeta().asMap());

        sketch.reset();

        result = sketch.getResult("meta", ALL_METADATA);
        metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals((String) metadata.get("family"), Family.FREQUENCY.getFamilyName());
        Assert.assertNull(metadata.get("size"));
        Assert.assertEquals(metadata.get("error"), 0L);
        Assert.assertEquals(metadata.get("n"), 0L);
        Assert.assertEquals(metadata.get("actives"), 0);
        Assert.assertFalse((Boolean) metadata.get("isEst"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 0);

        Assert.assertEquals(sketch.getRecords(), result.getRecords());
        Assert.assertEquals(sketch.getMetadata("meta", ALL_METADATA).asMap(), result.getMeta().asMap());
    }
}
