/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.sketches;

import com.yahoo.bullet.TestHelpers;
import com.yahoo.bullet.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.aggregations.grouping.GroupData;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;

import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.AVG;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.COUNT;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.SUM;

public class TupleSketchTest {
    private static final Set<GroupOperation> OPERATIONS =
            new HashSet<>(Arrays.asList(new GroupOperation(COUNT, null, "cnt"),
                                        new GroupOperation(SUM, "B", "sumB"),
                                        new GroupOperation(AVG, "A", "avgA")));

    private static final Map<String, String> ALL_METADATA = new HashMap<>();
    static {
        ALL_METADATA.put(Concept.ESTIMATED_RESULT.getName(), "isEst");
        ALL_METADATA.put(Concept.STANDARD_DEVIATIONS.getName(), "stddev");
        ALL_METADATA.put(Concept.FAMILY.getName(), "family");
        ALL_METADATA.put(Concept.SIZE.getName(), "size");
        ALL_METADATA.put(Concept.UNIQUES_ESTIMATE.getName(), "est");
        ALL_METADATA.put(Concept.THETA.getName(), "theta");
    }

    private CachingGroupData data;

    private BulletRecord get(String fieldA, double fieldB) {
        return RecordBox.get().add("A", fieldA).add("B", fieldB).getRecord();
    }

    private String addToData(String fieldA, double fieldB, CachingGroupData data) {
        BulletRecord record = get(fieldA, fieldB);

        Map<String, String> values = new HashMap<>();
        values.put("A", fieldA);
        values.put("B", Objects.toString(fieldB));

        data.setCachedRecord(record);
        data.setGroupFields(values);
        return fieldA + ";" + Objects.toString(fieldB);
    }

    @BeforeMethod
    public void setup() {
        data = new CachingGroupData(null, GroupData.makeInitialMetrics(OPERATIONS));
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void testBadCreation() {
        new TupleSketch(ResizeFactor.X1, -1.0f, -2, 0);

    }

    @Test
    public void testDistincts() {
        data = new CachingGroupData(null, new HashMap<>());

        TupleSketch sketch = new TupleSketch(ResizeFactor.X4, 1.0f, 64, 64);

        sketch.update(addToData("foo", 0.0, data), data);
        sketch.update(addToData("bar", 0.2, data), data);
        sketch.update(addToData("foo", 0.0, data), data);

        List<BulletRecord> actuals = sketch.getResult(null, null).getRecords();

        Assert.assertEquals(actuals.size(), 2);

        // Groups become strings
        BulletRecord expectedA = RecordBox.get().add("A", "foo").add("B", "0.0").getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "bar").add("B", "0.2").getRecord();

        TestHelpers.assertContains(actuals, expectedA);
        TestHelpers.assertContains(actuals, expectedB);
    }

    @Test
    public void testExactMetrics() {
        TupleSketch sketch = new TupleSketch(ResizeFactor.X4, 1.0f, 64, 64);

        sketch.update(addToData("9", 4.0, data), data);
        sketch.update(addToData("3", 0.2, data), data);
        sketch.update(addToData("9", 4.0, data), data);

        List<BulletRecord> actuals = sketch.getResult(null, null).getRecords();

        Assert.assertEquals(actuals.size(), 2);

        // Groups become strings
        BulletRecord expectedA = RecordBox.get().add("A", "9").add("B", "4.0")
                                                .add("cnt", 2).add("sumB", 8.0).add("avgA", 9.0).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "3").add("B", "0.2")
                                                .add("cnt", 1).add("sumB", 0.2).add("avgA", 3.0).getRecord();

        TestHelpers.assertContains(actuals, expectedA);
        TestHelpers.assertContains(actuals, expectedB);
    }

    @Test
    public void testApproximateMetrics() {
        TupleSketch sketch = new TupleSketch(ResizeFactor.X4, 1.0f, 32, 16);

        // Insert 2 duplicates of 0 - 63
        IntStream.range(0, 128).forEach(i -> sketch.update(addToData(String.valueOf(i % 64), 1, data), data));

        Clip result = sketch.getResult("meta", ALL_METADATA);
        Map<String, Object> actualMeta = result.getMeta().asMap();
        Assert.assertTrue(actualMeta.containsKey("meta"));
        Map<String, Object> stats = (Map<String, Object>) actualMeta.get("meta");

        Assert.assertEquals(stats.size(), 5);

        Assert.assertTrue((Boolean) stats.get("isEst"));
        Assert.assertTrue((Double) stats.get("theta") < 1.0);
        Assert.assertEquals((String) stats.get("family"), Family.TUPLE.getFamilyName());

        Map<String, Map<String, Double>> standardDeviations = (Map<String, Map<String, Double>>) stats.get("stddev");
        double upperOneSigma = standardDeviations.get(KMVSketch.META_STD_DEV_1).get(KMVSketch.META_STD_DEV_UB);
        double lowerOneSigma = standardDeviations.get(KMVSketch.META_STD_DEV_1).get(KMVSketch.META_STD_DEV_LB);
        double upperTwoSigma = standardDeviations.get(KMVSketch.META_STD_DEV_2).get(KMVSketch.META_STD_DEV_UB);
        double lowerTwoSigma = standardDeviations.get(KMVSketch.META_STD_DEV_2).get(KMVSketch.META_STD_DEV_LB);
        double upperThreeSigma = standardDeviations.get(KMVSketch.META_STD_DEV_3).get(KMVSketch.META_STD_DEV_UB);
        double lowerThreeSigma = standardDeviations.get(KMVSketch.META_STD_DEV_3).get(KMVSketch.META_STD_DEV_LB);

        double estimate = (Double) stats.get("est");

        Assert.assertTrue(estimate >= lowerOneSigma);
        Assert.assertTrue(estimate <= upperOneSigma);
        Assert.assertTrue(estimate >= lowerTwoSigma);
        Assert.assertTrue(estimate <= upperTwoSigma);
        Assert.assertTrue(estimate >= lowerThreeSigma);
        Assert.assertTrue(estimate <= upperThreeSigma);

        Assert.assertEquals(result.getRecords().size(), 16);
        for (BulletRecord actual : result.getRecords()) {
            String fieldA = actual.get("A").toString();
            String fieldB = actual.get("B").toString();
            Long count = (Long) actual.get("cnt");
            Double sumB = (Double) actual.get("sumB");
            Double averageA = (Double) actual.get("avgA");
            Assert.assertTrue(Integer.valueOf(fieldA) < 64);
            Assert.assertEquals(Double.valueOf(fieldB), 1.0);
            Assert.assertEquals(count, Long.valueOf(2));
            Assert.assertEquals(sumB, 2.0);
            // A <= 64, so even if count was 1 or 2, this should be < 64
            Assert.assertTrue(averageA < 64.0);
        }
    }

    @Test
    public void testUnioning() {
        TupleSketch sketch = new TupleSketch(ResizeFactor.X4, 1.0f, 32, 16);
        // 16 0 - 7 fieldA
        IntStream.range(0, 128).forEach(i -> sketch.update(addToData(String.valueOf(i % 8), 1, data), data));

        TupleSketch anotherSketch = new TupleSketch(ResizeFactor.X4, 1.0f, 32, 16);
        // 8 copies of 0 - 15 fieldA
        IntStream.range(0, 128).forEach(i -> sketch.update(addToData(String.valueOf(i % 16), 3, data), data));

        TupleSketch unionSketch = new TupleSketch(ResizeFactor.X4, 1.0f, 32, 16);
        unionSketch.union(sketch.serialize());
        unionSketch.union(anotherSketch.serialize());

        List<BulletRecord> results = unionSketch.getResult(null, null).getRecords();

        Assert.assertEquals(results.size(), 16);
        for (BulletRecord actual : results) {
            Integer fieldA = Integer.valueOf(actual.get("A").toString());
            Double fieldB = Double.valueOf(actual.get("B").toString());
            Long count = (Long) actual.get("cnt");
            Double sumB = (Double) actual.get("sumB");
            Double averageA = (Double) actual.get("avgA");

            if (fieldA < 8) {
                // 1 or 3
                Assert.assertTrue(fieldB == 1.0 || fieldB == 3.0);
                Assert.assertEquals(count, fieldB == 1.0 ? Long.valueOf(16) : Long.valueOf(8));
                Assert.assertTrue(averageA < 8.0);
                // 16 * 1 or 8 * 3
                Assert.assertEquals(sumB, fieldB == 1.0 ? 16.0 : 24.0);
            } else {
                Assert.assertEquals(fieldB, 3.0);
                Assert.assertEquals(count, Long.valueOf(8));
                // 8  * 3
                Assert.assertEquals(sumB, 24.0);
                Assert.assertTrue(averageA < 16.0);
            }
        }
    }

    @Test
    public void testResetting() {
        data = new CachingGroupData(null, new HashMap<>());

        TupleSketch sketch = new TupleSketch(ResizeFactor.X4, 1.0f, 32, 16);
        sketch.update(addToData("foo", 0.0, data), data);
        sketch.update(addToData("bar", 0.2, data), data);

        TupleSketch anotherSketch = new TupleSketch(ResizeFactor.X4, 1.0f, 32, 16);
        // 8 copies of 0 - 15 fieldA
        sketch.update(addToData("bar", 0.2, data), data);
        sketch.update(addToData("baz", 0.2, data), data);
        sketch.update(addToData("qux", 0.2, data), data);

        sketch.union(anotherSketch.serialize());

        List<BulletRecord> actuals = sketch.getResult(null, null).getRecords();

        Assert.assertEquals(actuals.size(), 4);
        BulletRecord expectedA = RecordBox.get().add("A", "foo").add("B", "0.0").getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "bar").add("B", "0.2").getRecord();
        BulletRecord expectedC = RecordBox.get().add("A", "baz").add("B", "0.2").getRecord();
        BulletRecord expectedD = RecordBox.get().add("A", "qux").add("B", "0.2").getRecord();

        TestHelpers.assertContains(actuals, expectedA);
        TestHelpers.assertContains(actuals, expectedB);
        TestHelpers.assertContains(actuals, expectedC);
        TestHelpers.assertContains(actuals, expectedD);

        sketch.reset();

        sketch.update(addToData("foo", 0.0, data), data);
        sketch.update(addToData("bar", 0.2, data), data);

        actuals = sketch.getResult(null, null).getRecords();

        Assert.assertEquals(actuals.size(), 2);
        expectedA = RecordBox.get().add("A", "foo").add("B", "0.0").getRecord();
        expectedB = RecordBox.get().add("A", "bar").add("B", "0.2").getRecord();

        TestHelpers.assertContains(actuals, expectedA);
        TestHelpers.assertContains(actuals, expectedB);
    }
}
