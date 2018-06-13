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
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class ThetaSketchTest {
    private static final Map<String, String> ALL_METADATA = new HashMap<>();
    static {
        ALL_METADATA.put(Concept.SKETCH_ESTIMATED_RESULT.getName(), "isEst");
        ALL_METADATA.put(Concept.SKETCH_STANDARD_DEVIATIONS.getName(), "stddev");
        ALL_METADATA.put(Concept.SKETCH_FAMILY.getName(), "family");
        ALL_METADATA.put(Concept.SKETCH_SIZE.getName(), "size");
        ALL_METADATA.put(Concept.SKETCH_THETA.getName(), "theta");
    }

    private BulletRecordProvider bulletRecordProvider;

    @BeforeMethod
    private void setup() {
        bulletRecordProvider = new BulletConfig().getBulletRecordProvider();
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void testBadCreation() {
        new ThetaSketch(null, null, 1.0f, -2, bulletRecordProvider);
    }

    @Test
    public void testUpdatingForExactResult() {
        ThetaSketch sketch = new ThetaSketch(ResizeFactor.X4, Family.ALPHA, 1.0f, 512, bulletRecordProvider);
        sketch.update("foo");
        sketch.update("bar");
        sketch.update("baz");

        List<BulletRecord> actuals = sketch.getResult(null, null).getRecords();
        Assert.assertEquals(actuals.size(), 1);

        BulletRecord expected = RecordBox.get().add(ThetaSketch.COUNT_FIELD, 3.0).getRecord();
        BulletRecord actual = actuals.get(0);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testUpdatingForApproximateResult() {
        ThetaSketch sketch = new ThetaSketch(ResizeFactor.X4, Family.ALPHA, 1.0f, 512, bulletRecordProvider);
        IntStream.range(0, 1024).forEach(i -> sketch.update(String.valueOf(i)));

        Map<String, String> metaKeys = new HashMap<>();
        metaKeys.put(Concept.SKETCH_ESTIMATED_RESULT.getName(), "isEst");

        Clip result = sketch.getResult("meta", metaKeys);

        Map<String, Object> actualMeta = result.getMeta().asMap();
        Assert.assertTrue(actualMeta.containsKey("meta"));
        Map<String, Object> stats = (Map<String, Object>) actualMeta.get("meta");
        Assert.assertEquals(stats.size(), 1);
        Assert.assertTrue((Boolean) stats.get("isEst"));

        Assert.assertEquals(result.getRecords().size(), 1);
        double actual = (Double) result.getRecords().get(0).get(ThetaSketch.COUNT_FIELD);
        // We better be at least 50% accurate with 512 entries and 1024 uniques
        Assert.assertTrue(actual > 512);
        Assert.assertTrue(actual < 1536);

        Assert.assertEquals(sketch.getRecords(), result.getRecords());
        Assert.assertEquals(sketch.getMetadata("meta", metaKeys).asMap(), actualMeta);
    }

    @Test
    public void testUnioning() {
        ThetaSketch sketch = new ThetaSketch(ResizeFactor.X4, Family.ALPHA, 1.0f, 512, bulletRecordProvider);
        IntStream.range(0, 1024).forEach(i -> sketch.update(String.valueOf(i)));

        ThetaSketch anotherSketch = new ThetaSketch(ResizeFactor.X4, Family.ALPHA, 1.0f, 512, bulletRecordProvider);
        IntStream.range(-1024, 0).forEach(i -> anotherSketch.update(String.valueOf(i)));

        ThetaSketch unionSketch = new ThetaSketch(ResizeFactor.X4, Family.QUICKSELECT, 1.0f, 512, bulletRecordProvider);
        unionSketch.union(sketch.serialize());
        unionSketch.union(anotherSketch.serialize());

        Clip result = unionSketch.getResult("meta", ALL_METADATA);
        Map<String, Object> actualMeta = result.getMeta().asMap();
        Assert.assertTrue(actualMeta.containsKey("meta"));
        Map<String, Object> stats = (Map<String, Object>) actualMeta.get("meta");

        Assert.assertEquals(stats.size(), 5);

        Assert.assertTrue((Boolean) stats.get("isEst"));
        Assert.assertTrue((Double) stats.get("theta") < 1.0);
        // We inserted 2048 unique integers. Size is at least 512 bytes.
        Assert.assertTrue((Integer) stats.get("size") > 512);
        // The family is the family of the Union
        Assert.assertEquals((String) stats.get("family"), Family.QUICKSELECT.getFamilyName());

        Map<String, Map<String, Double>> standardDeviations = (Map<String, Map<String, Double>>) stats.get("stddev");
        double upperOneSigma = standardDeviations.get(KMVSketch.META_STD_DEV_1).get(KMVSketch.META_STD_DEV_UB);
        double lowerOneSigma = standardDeviations.get(KMVSketch.META_STD_DEV_1).get(KMVSketch.META_STD_DEV_LB);
        double upperTwoSigma = standardDeviations.get(KMVSketch.META_STD_DEV_2).get(KMVSketch.META_STD_DEV_UB);
        double lowerTwoSigma = standardDeviations.get(KMVSketch.META_STD_DEV_2).get(KMVSketch.META_STD_DEV_LB);
        double upperThreeSigma = standardDeviations.get(KMVSketch.META_STD_DEV_3).get(KMVSketch.META_STD_DEV_UB);
        double lowerThreeSigma = standardDeviations.get(KMVSketch.META_STD_DEV_3).get(KMVSketch.META_STD_DEV_LB);

        Assert.assertEquals(result.getRecords().size(), 1);
        double actual = (Double) result.getRecords().get(0).get(ThetaSketch.COUNT_FIELD);

        Assert.assertTrue(actual >= lowerOneSigma);
        Assert.assertTrue(actual <= upperOneSigma);
        Assert.assertTrue(actual >= lowerTwoSigma);
        Assert.assertTrue(actual <= upperTwoSigma);
        Assert.assertTrue(actual >= lowerThreeSigma);
        Assert.assertTrue(actual <= upperThreeSigma);

        Assert.assertEquals(unionSketch.getRecords(), result.getRecords());
        Assert.assertEquals(unionSketch.getMetadata("meta", ALL_METADATA).asMap(), actualMeta);
    }

    @Test
    public void testResetting() {
        ThetaSketch sketch = new ThetaSketch(ResizeFactor.X4, Family.ALPHA, 1.0f, 512, bulletRecordProvider);
        sketch.update("foo");
        sketch.update("bar");
        sketch.update("baz");

        List<BulletRecord> actuals = sketch.getResult(null, null).getRecords();
        Assert.assertEquals(actuals.size(), 1);
        BulletRecord expected = RecordBox.get().add(ThetaSketch.COUNT_FIELD, 3.0).getRecord();
        BulletRecord actual = actuals.get(0);
        Assert.assertEquals(actual, expected);

        sketch.reset();

        actuals = sketch.getResult(null, null).getRecords();
        Assert.assertEquals(actuals.size(), 1);
        expected = RecordBox.get().add(ThetaSketch.COUNT_FIELD, 0.0).getRecord();
        actual = actuals.get(0);
        Assert.assertEquals(actual, expected);

        ThetaSketch anotherSketch = new ThetaSketch(ResizeFactor.X4, Family.ALPHA, 1.0f, 512, bulletRecordProvider);
        IntStream.range(0, 41).forEach(i -> anotherSketch.update(String.valueOf(i)));

        sketch.union(anotherSketch.serialize());
        sketch.update("foo");

        actuals = sketch.getResult(null, null).getRecords();
        Assert.assertEquals(actuals.size(), 1);
        expected = RecordBox.get().add(ThetaSketch.COUNT_FIELD, 42.0).getRecord();
        actual = actuals.get(0);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testFetchingDataWithoutResettingAndInsertingMoreData() {
        ThetaSketch sketch = new ThetaSketch(ResizeFactor.X4, Family.ALPHA, 1.0f, 512, bulletRecordProvider);
        sketch.update("foo");
        sketch.update("bar");
        sketch.update("baz");

        List<BulletRecord> actuals = sketch.getResult(null, null).getRecords();
        Assert.assertEquals(actuals.size(), 1);
        BulletRecord expected = RecordBox.get().add(ThetaSketch.COUNT_FIELD, 3.0).getRecord();
        BulletRecord actual = actuals.get(0);
        Assert.assertEquals(actual, expected);

        sketch.update("baz");
        sketch.update("qux");
        sketch.update("norf");

        actuals = sketch.getResult(null, null).getRecords();
        Assert.assertEquals(actuals.size(), 1);
        expected = RecordBox.get().add(ThetaSketch.COUNT_FIELD, 5.0).getRecord();
        actual = actuals.get(0);
        Assert.assertEquals(actual, expected);

        ThetaSketch anotherSketch = new ThetaSketch(ResizeFactor.X4, Family.ALPHA, 1.0f, 512, bulletRecordProvider);
        IntStream.range(0, 41).forEach(i -> anotherSketch.update(String.valueOf(i)));
        sketch.union(anotherSketch.serialize());

        actuals = sketch.getResult(null, null).getRecords();
        Assert.assertEquals(actuals.size(), 1);
        expected = RecordBox.get().add(ThetaSketch.COUNT_FIELD, 46.0).getRecord();
        actual = actuals.get(0);
        Assert.assertEquals(actual, expected);

        sketch.union(anotherSketch.serialize());
        actuals = sketch.getResult(null, null).getRecords();
        Assert.assertEquals(actuals.size(), 1);
        expected = RecordBox.get().add(ThetaSketch.COUNT_FIELD, 46.0).getRecord();
        actual = actuals.get(0);
        Assert.assertEquals(actual, expected);

        // Do it again and make sure getResult is idempotent if new data was not added
        actuals = sketch.getResult(null, null).getRecords();
        Assert.assertEquals(actuals.size(), 1);
        expected = RecordBox.get().add(ThetaSketch.COUNT_FIELD, 46.0).getRecord();
        actual = actuals.get(0);
        Assert.assertEquals(actual, expected);
    }
}
