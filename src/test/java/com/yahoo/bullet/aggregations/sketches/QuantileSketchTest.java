/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.sketches;

import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.quantiles.DoublesSketch;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.assertApproxEquals;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.COUNT_FIELD;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.END_EXCLUSIVE;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.NEGATIVE_INFINITY;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.NEGATIVE_INFINITY_START;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.POSITIVE_INFINITY;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.POSITIVE_INFINITY_END;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.PROBABILITY_FIELD;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.QUANTILE_FIELD;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.RANGE_FIELD;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.SEPARATOR;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.START_INCLUSIVE;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.VALUE_FIELD;

public class QuantileSketchTest {

    private static final Map<String, String> ALL_METADATA = new HashMap<>();
    static {
        ALL_METADATA.put(Concept.SKETCH_ESTIMATED_RESULT.getName(), "isEst");
        ALL_METADATA.put(Concept.SKETCH_FAMILY.getName(), "family");
        ALL_METADATA.put(Concept.SKETCH_SIZE.getName(), "size");
        ALL_METADATA.put(Concept.SKETCH_NORMALIZED_RANK_ERROR.getName(), "nre");
        ALL_METADATA.put(Concept.SKETCH_ITEMS_SEEN.getName(), "n");
        ALL_METADATA.put(Concept.SKETCH_MINIMUM_VALUE.getName(), "min");
        ALL_METADATA.put(Concept.SKETCH_MAXIMUM_VALUE.getName(), "max");
    }

    public static double[] makePoints(double... items) {
        return items;
    }

    public static double[] makePoints(double from, double to, double increment) {
        double start = from;
        List<Double> points = new ArrayList<>();
        while (start <= to) {
            points.add(start);
            start += increment;
        }
        return points.stream().mapToDouble(d -> d).toArray();
    }

    public static String getStart(String range) {
        int separatorStart = range.lastIndexOf(SEPARATOR);
        // Remove the range start character
        return range.substring(1, separatorStart);
    }

    public static String getEnd(String range) {
        int endStart = range.lastIndexOf(SEPARATOR) + SEPARATOR.length();
        // Remove the range end character
        return range.substring(endStart, range.length() - 1);
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void testBadCreation() {
        new QuantileSketch(-2, null, null);
    }

    @Test
    public void testExactQuantilesWithNumberOfPoints() {
        QuantileSketch sketch = new QuantileSketch(64, 2, Distribution.DistributionType.QUANTILE, 11);

        // Insert 0, 10, 20 ... 100
        IntStream.range(0, 11).forEach(i -> sketch.update(i * 10.0));

        List<BulletRecord> records = sketch.getResult(null, null).getRecords();
        for (BulletRecord record : records) {
            Double quantile = (Double) record.get(QUANTILE_FIELD);
            Double value = (Double) record.get(VALUE_FIELD);
            // The value is the quantile as a percentage : Value 20.0 is the 0.2 or 20th percentile
            double percent = quantile * 100;

            assertApproxEquals(percent, value);
        }
    }

    @Test
    public void testExactPMFWithNumberOfPoints() {
        QuantileSketch sketch = new QuantileSketch(64, 2, Distribution.DistributionType.PMF, 10);

        // Insert 0, 1, 2 ... 9 three times
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 10));
        // Insert 0, 1, 2 ten times
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 3));

        Clip result = sketch.getResult("meta", ALL_METADATA);
        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");

        Assert.assertEquals(metadata.size(), 7);

        Assert.assertFalse((Boolean) metadata.get("isEst"));
        Assert.assertEquals((String) metadata.get("family"), Family.QUANTILES.getFamilyName());
        // Size should be at least 10 bytes since we inserted 60 items: 10 uniques
        Assert.assertTrue((Integer) metadata.get("size") >= 10);
        Assert.assertEquals(metadata.get("nre"), DoublesSketch.getNormalizedRankError(64));
        // 60 items
        Assert.assertEquals(metadata.get("n"), 60L);
        Assert.assertEquals(metadata.get("min"), 0.0);
        Assert.assertEquals(metadata.get("max"), 9.0);

        List<BulletRecord> records = result.getRecords();
        for (BulletRecord record : records) {
            String range = (String) record.get(RANGE_FIELD);
            double count = (Double) record.get(COUNT_FIELD);
            double probablity = (Double) record.get(PROBABILITY_FIELD);
            String rangeStart = getStart(range);
            String rangeEnd = getEnd(range);

            if (rangeStart.equals(NEGATIVE_INFINITY)) {
                Assert.assertEquals(count, 0.0);
                Assert.assertEquals(probablity, 0.0);
            } else if (rangeEnd.equals(POSITIVE_INFINITY)) {
                Assert.assertEquals(count, 3.0);
                Assert.assertEquals(probablity, 1.0 / 20);
            } else {
                double start = Double.valueOf(rangeStart);
                if (start <= 2.0) {
                    // 0.0 - 1.0, 1.0 - 2.0, 2.0 - 3.0 have 3 from the first, 10 from the second
                    Assert.assertEquals(count, 13.0);
                    Assert.assertEquals(probablity, 13.0 / 60);
                } else {
                    // The rest are all 3
                    Assert.assertEquals(count, 3.0);
                    Assert.assertEquals(probablity, 1.0 / 20);
                }
            }
        }

        Assert.assertEquals(sketch.getRecords(), records);
        Assert.assertEquals(sketch.getMetadata("meta", ALL_METADATA).asMap(), result.getMeta().asMap());
    }

    @Test
    public void testExactCDFWithNumberOfPoints() {
        QuantileSketch sketch = new QuantileSketch(64, 2, Distribution.DistributionType.CDF, 10);

        IntStream.range(0, 30).forEach(i -> sketch.update(i % 10));
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 3));

        Clip result = sketch.getResult(null, null);

        List<BulletRecord> records = result.getRecords();
        for (BulletRecord record : records) {
            String range = (String) record.get(RANGE_FIELD);
            double count = (Double) record.get(COUNT_FIELD);
            double probablity = (Double) record.get(PROBABILITY_FIELD);
            String rangeEnd = getEnd(range);

            if (rangeEnd.equals(POSITIVE_INFINITY)) {
                Assert.assertEquals(count, 60.0);
                Assert.assertEquals(probablity, 1.0);
            } else {
                double end = Double.valueOf(rangeEnd);
                if (end <= 3.0) {
                    // -inf - 0.0, -inf - 1.0, -inf - 2.0, -inf - 3.0 are 13 * 0, 13 * 1, ...
                    Assert.assertEquals(count, end * 13.0);
                    Assert.assertEquals(probablity, end * 13.0 / 60);
                } else {
                    // -inf - 4.0, -inf - 5.0 ... -inf - 9.0 are 13*3 + (1*3),  13*3 + (2*3) ...
                    Assert.assertEquals(count, 39.0 + (end - 3) * 3.0);
                    Assert.assertEquals(probablity, (39.0 + (end - 3) * 3.0) / 60);
                }
            }
        }
    }

    @Test
    public void testExactQuantilesWithProvidedPoints() {
        // Same results as the testExactQuantilesWithNumberOfPoints
        QuantileSketch sketch = new QuantileSketch(64, Distribution.DistributionType.QUANTILE, makePoints(0.0, 1.0, 0.1));

        IntStream.range(0, 11).forEach(i -> sketch.update(i * 10.0));

        List<BulletRecord> records = sketch.getResult(null, null).getRecords();
        for (BulletRecord record : records) {
            Double quantile = (Double) record.get(QUANTILE_FIELD);
            Double value = (Double) record.get(VALUE_FIELD);
            double percent = quantile * 100;

            assertApproxEquals(percent, value);
        }
    }

    @Test
    public void testExactPMFWithProvidedPoints() {
        // Same results as the testExactPMFWithNumberOfPoints
        QuantileSketch sketch = new QuantileSketch(64, Distribution.DistributionType.PMF, makePoints(0.0, 9.0, 1.0));

        IntStream.range(0, 30).forEach(i -> sketch.update(i % 10));
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 3));

        Clip result = sketch.getResult(null, null);

        List<BulletRecord> records = result.getRecords();
        for (BulletRecord record : records) {
            String range = (String) record.get(RANGE_FIELD);
            double count = (Double) record.get(COUNT_FIELD);
            double probablity = (Double) record.get(PROBABILITY_FIELD);
            String rangeStart = getStart(range);
            String rangeEnd = getEnd(range);

            if (rangeStart.equals(NEGATIVE_INFINITY)) {
                Assert.assertEquals(count, 0.0);
                Assert.assertEquals(probablity, 0.0);
            } else if (rangeEnd.equals(POSITIVE_INFINITY)) {
                Assert.assertEquals(count, 3.0);
                Assert.assertEquals(probablity, 1.0 / 20);
            } else {
                double start = Double.valueOf(rangeStart);
                if (start <= 2.0) {
                    Assert.assertEquals(count, 13.0);
                    Assert.assertEquals(probablity, 13.0 / 60);
                } else {
                    Assert.assertEquals(count, 3.0);
                    Assert.assertEquals(probablity, 1.0 / 20);
                }
            }
        }
    }

    @Test
    public void testExactCDFWithProvidedPoints() {
        // Same results as the testExactPMFWithNumberOfPoints
        QuantileSketch sketch = new QuantileSketch(64, Distribution.DistributionType.CDF, makePoints(0.0, 9.0, 1.0));

        IntStream.range(0, 30).forEach(i -> sketch.update(i % 10));
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 3));

        Clip result = sketch.getResult(null, null);

        List<BulletRecord> records = result.getRecords();
        for (BulletRecord record : records) {
            String range = (String) record.get(RANGE_FIELD);
            double count = (Double) record.get(COUNT_FIELD);
            double probablity = (Double) record.get(PROBABILITY_FIELD);
            String rangeEnd = getEnd(range);

            if (rangeEnd.equals(POSITIVE_INFINITY)) {
                Assert.assertEquals(count, 60.0);
                Assert.assertEquals(probablity, 1.0);
            } else {
                double end = Double.valueOf(rangeEnd);
                if (end <= 3.0) {
                    Assert.assertEquals(count, end * 13.0);
                    Assert.assertEquals(probablity, end * 13.0 / 60);
                } else {
                    Assert.assertEquals(count, 39.0 + (end - 3) * 3.0);
                    Assert.assertEquals(probablity, (39.0 + (end - 3) * 3.0) / 60);
                }
            }
        }
    }

    @Test
    public void testNoDataQuantileDistribution() {
        QuantileSketch sketch = new QuantileSketch(64, Distribution.DistributionType.QUANTILE, new double[]{ 0, 0.3, 1 });

        Clip result = sketch.getResult("meta", ALL_METADATA);
        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 7);

        Assert.assertFalse((Boolean) metadata.get("isEst"));
        Assert.assertEquals(metadata.get("n"), 0L);
        Assert.assertEquals(metadata.get("min"), Double.POSITIVE_INFINITY);
        Assert.assertEquals(metadata.get("max"), Double.NEGATIVE_INFINITY);

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 3);

        BulletRecord expectedA = RecordBox.get().add(QUANTILE_FIELD, 0.0).add(VALUE_FIELD, Double.POSITIVE_INFINITY)
                                                .getRecord();
        BulletRecord expectedB = RecordBox.get().add(QUANTILE_FIELD, 0.3).add(VALUE_FIELD, Double.NaN).getRecord();
        BulletRecord expectedC = RecordBox.get().add(QUANTILE_FIELD, 1.0).add(VALUE_FIELD, Double.NEGATIVE_INFINITY)
                                                .getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
        Assert.assertEquals(records.get(2), expectedC);

        Assert.assertEquals(sketch.getRecords(), records);
        Assert.assertEquals(sketch.getMetadata("meta", ALL_METADATA).asMap(), result.getMeta().asMap());
    }

    @Test
    public void testNoDataPMFDistribution() {
        QuantileSketch sketch = new QuantileSketch(64, 2, Distribution.DistributionType.PMF, 10);

        Clip result = sketch.getResult("meta", ALL_METADATA);
        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 7);

        Assert.assertFalse((Boolean) metadata.get("isEst"));
        Assert.assertEquals(metadata.get("n"), 0L);
        Assert.assertEquals(metadata.get("min"), Double.POSITIVE_INFINITY);
        Assert.assertEquals(metadata.get("max"), Double.NEGATIVE_INFINITY);

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 2);

        BulletRecord expectedA = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + Double.POSITIVE_INFINITY + END_EXCLUSIVE)
                                                .add(PROBABILITY_FIELD, Double.NaN)
                                                .add(COUNT_FIELD, Double.NaN)
                                                .getRecord();
        BulletRecord expectedB = RecordBox.get().add(RANGE_FIELD, START_INCLUSIVE + Double.POSITIVE_INFINITY + SEPARATOR + POSITIVE_INFINITY_END)
                                                .add(PROBABILITY_FIELD, Double.NaN)
                                                .add(COUNT_FIELD, Double.NaN)
                                                .getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);

        Assert.assertEquals(sketch.getRecords(), records);
        Assert.assertEquals(sketch.getMetadata("meta", ALL_METADATA).asMap(), result.getMeta().asMap());
    }

    @Test
    public void testNoDataCDFDistribution() {
        QuantileSketch sketch = new QuantileSketch(64, 2, Distribution.DistributionType.CDF, 10);

        Clip result = sketch.getResult("meta", ALL_METADATA);
        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 7);

        Assert.assertFalse((Boolean) metadata.get("isEst"));
        Assert.assertEquals(metadata.get("n"), 0L);
        Assert.assertEquals(metadata.get("min"), Double.POSITIVE_INFINITY);
        Assert.assertEquals(metadata.get("max"), Double.NEGATIVE_INFINITY);

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 2);

        BulletRecord expectedA = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + Double.POSITIVE_INFINITY + END_EXCLUSIVE)
                                                .add(PROBABILITY_FIELD, Double.NaN)
                                                .add(COUNT_FIELD, Double.NaN)
                                                .getRecord();
        BulletRecord expectedB = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + POSITIVE_INFINITY_END)
                                                .add(PROBABILITY_FIELD, Double.NaN)
                                                .add(COUNT_FIELD, Double.NaN)
                                                .getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);

        Assert.assertEquals(sketch.getRecords(), records);
        Assert.assertEquals(sketch.getMetadata("meta", ALL_METADATA).asMap(), result.getMeta().asMap());
    }

    @Test
    public void testOnePointDistribution() {
        QuantileSketch sketch = new QuantileSketch(64, 2, Distribution.DistributionType.QUANTILE, 1);

        // Insert 10, 20 ... 100
        IntStream.range(1, 11).forEach(i -> sketch.update(i * 10.0));

        List<BulletRecord> records = sketch.getResult(null, null).getRecords();
        Assert.assertEquals(records.size(), 1);
        BulletRecord actual = records.get(0);
        BulletRecord expected = RecordBox.get().add(QUANTILE_FIELD, 0.0)
                                               .add(VALUE_FIELD, 10.0).getRecord();
        Assert.assertEquals(actual, expected);

        QuantileSketch anotherSketch = new QuantileSketch(64, Distribution.DistributionType.QUANTILE, makePoints(1.0));
        IntStream.range(1, 11).forEach(i -> anotherSketch.update(i * 10.0));
        records = anotherSketch.getResult(null, null).getRecords();
        Assert.assertEquals(records.size(), 1);
        actual = records.get(0);
        expected = RecordBox.get().add(QUANTILE_FIELD, 1.0)
                                  .add(VALUE_FIELD, 100.0).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testApproximateQuantilesWithNumberOfPoints() {
        QuantileSketch sketch = new QuantileSketch(32, 2, Distribution.DistributionType.QUANTILE, 11);

        IntStream.range(1, 101).forEach(i -> sketch.update(i * 0.1));

        Clip result = sketch.getResult("meta", ALL_METADATA);
        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");

        Assert.assertEquals(metadata.size(), 7);

        Assert.assertTrue((Boolean) metadata.get("isEst"));
        Assert.assertEquals((String) metadata.get("family"), Family.QUANTILES.getFamilyName());
        Assert.assertTrue((Integer) metadata.get("size") >= 100);
        double error = DoublesSketch.getNormalizedRankError(32);
        Assert.assertEquals(metadata.get("nre"), error);
        Assert.assertEquals(metadata.get("n"), 100L);
        assertApproxEquals((Double) metadata.get("min"), 0.1);
        assertApproxEquals((Double) metadata.get("max"), 10.0);

        List<BulletRecord> records = result.getRecords();

        for (BulletRecord record : records) {
            Double quantile = (Double) record.get(QUANTILE_FIELD);
            Double value = (Double) record.get(VALUE_FIELD);

            // We input 100 values: 0.0, 0.1, ... 9.9, and our NRE is ~6.3%. This means, for e.g., that the 50th
            // percentile value is approximate and is between the true 43th and 57th percentile, or between 4.3 and 5.7
            // in our case. The NRE * 10 is the epsilon we should use our comparison with high probability.
            assertApproxEquals(value, quantile * 10, error * 10);
        }

        Assert.assertEquals(sketch.getRecords(), records);
        Assert.assertEquals(sketch.getMetadata("meta", ALL_METADATA).asMap(), result.getMeta().asMap());
    }

    @Test
    public void testUnioning() {
        QuantileSketch sketch = new QuantileSketch(64, Distribution.DistributionType.CDF, makePoints(0.0, 9.0, 1.0));
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 10));

        QuantileSketch anotherSketch = new QuantileSketch(64, Distribution.DistributionType.CDF, makePoints(0.0, 9.0, 1.0));
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 3));

        QuantileSketch mergedSketch = new QuantileSketch(64, Distribution.DistributionType.CDF, makePoints(0.0, 9.0, 1.0));
        mergedSketch.union(sketch.serialize());
        mergedSketch.union(anotherSketch.serialize());

        Clip result = mergedSketch.getResult(null, null);

        List<BulletRecord> records = result.getRecords();
        for (BulletRecord record : records) {
            String range = (String) record.get(RANGE_FIELD);
            double count = (Double) record.get(COUNT_FIELD);
            double probablity = (Double) record.get(PROBABILITY_FIELD);
            String rangeEnd = getEnd(range);

            if (rangeEnd.equals(POSITIVE_INFINITY)) {
                Assert.assertEquals(count, 60.0);
                Assert.assertEquals(probablity, 1.0);
            } else {
                double end = Double.valueOf(rangeEnd);
                if (end <= 3.0) {
                    Assert.assertEquals(count, end * 13.0);
                    Assert.assertEquals(probablity, end * 13.0 / 60);
                } else {
                    Assert.assertEquals(count, 39.0 + (end - 3) * 3.0);
                    Assert.assertEquals(probablity, (39.0 + (end - 3) * 3.0) / 60);
                }
            }
        }
    }

    @Test
    public void testResetting() {
        QuantileSketch sketch = new QuantileSketch(64, Distribution.DistributionType.CDF, makePoints(0.0, 9.0, 1.0));
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 10));

        QuantileSketch anotherSketch = new QuantileSketch(64, Distribution.DistributionType.CDF, makePoints(0.0, 9.0, 1.0));
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 3));

        sketch.union(anotherSketch.serialize());

        Clip result = sketch.getResult(null, null);

        List<BulletRecord> records = result.getRecords();
        for (BulletRecord record : records) {
            String range = (String) record.get(RANGE_FIELD);
            double count = (Double) record.get(COUNT_FIELD);
            String rangeEnd = getEnd(range);

            if (rangeEnd.equals(POSITIVE_INFINITY)) {
                Assert.assertEquals(count, 60.0);
            } else {
                double end = Double.valueOf(rangeEnd);
                if (end <= 3.0) {
                    Assert.assertEquals(count, end * 13.0);
                } else {
                    Assert.assertEquals(count, 39.0 + (end - 3) * 3.0);
                }
            }
        }

        sketch.reset();
        sketch.update(1.0);

        result = sketch.getResult(null, null);

        records = result.getRecords();
        for (BulletRecord record : records) {
            String range = (String) record.get(RANGE_FIELD);
            double count = (Double) record.get(COUNT_FIELD);
            String rangeEnd = getEnd(range);

            if (rangeEnd.equals(POSITIVE_INFINITY)) {
                Assert.assertEquals(count, 1.0);
            } else {
                double end = Double.valueOf(rangeEnd);
                if (end <= 1.0) {
                    Assert.assertEquals(count, 0.0);
                } else {
                    Assert.assertEquals(count, 1.0);
                }
            }
        }
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testZippingBadLength() {
        double[] domain = { -1.0, 0, 4.0 };
        double[] range =  { 0.2, 0.3, 0.5 };
        QuantileSketch.zip(domain, range, Distribution.DistributionType.CDF, 30);
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testZippingBadDomainPMF() {
        double[] domain = { };
        double[] range = { 0.2, 0.3, 0.5 };
        // This will try to use the first entry in domain
        QuantileSketch.zip(domain, range, Distribution.DistributionType.PMF, 30);
    }

    @Test
    public void testZippingBadDomainCDF() {
        double[] domain = { };
        double[] range = { 0.2, 0.3, 0.5 };
        // This will use the -inf to +inf range and the first entry in range
        List<BulletRecord> records = QuantileSketch.zip(domain, range, Distribution.DistributionType.CDF, 30);
        Assert.assertEquals(records.size(), 1);
        BulletRecord actual = records.get(0);
        BulletRecord expected = RecordBox.get()
                                         .add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + POSITIVE_INFINITY_END)
                                         .add(PROBABILITY_FIELD, 0.2)
                                         .add(COUNT_FIELD, 6.0)
                                         .getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testZippingBadDomainQuantiles() {
        double[] domain = { };
        double[] range = { 0.2, 0.3, 0.5 };
        // This will not add anything
        List<BulletRecord> records = QuantileSketch.zip(domain, range, Distribution.DistributionType.QUANTILE, 30);
        Assert.assertEquals(records.size(), 0);
    }

    @Test
    public void testZippingPMF() {
        double[] domain = { -1.0, 0, 4.0 };
        double[] range =  { 0, 0.3, 0.5, 0.2 };

        List<BulletRecord> results = QuantileSketch.zip(domain, range, Distribution.DistributionType.PMF, 30);

        double[] actualRange = results.stream()
                                       .mapToDouble(r -> (Double) r.get(PROBABILITY_FIELD)).toArray();

        Assert.assertEquals(actualRange, range);
    }

    @Test
    public void testZippingQuantiles() {
        double[] domain = { 0.0, 0.5, 1.0 };
        double[] values =  { 0, 3.3, 1.5 };

        List<BulletRecord> results = QuantileSketch.zip(domain, values, Distribution.DistributionType.QUANTILE, 30);

        double[] actualValues = results.stream()
                                       .mapToDouble(r -> (Double) r.get(VALUE_FIELD)).toArray();

        Assert.assertEquals(actualValues, values);
    }

    @Test
    public void testRounding() {
        QuantileSketch sketch = new QuantileSketch(64, 6, Distribution.DistributionType.CDF, 10);

        IntStream.range(0, 30).forEach(i -> sketch.update(i % 10));
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 3));

        Clip result = sketch.getResult(null, null);

        Set<String> actualRangeEnds = result.getRecords().stream().map(r -> (String) r.get(RANGE_FIELD))
                                                                  .map(QuantileSketchTest::getEnd)
                                                                  .collect(Collectors.toSet());
        Set<String> expectedRangeEnds = new HashSet<>(Arrays.asList("0.0", "1.0", "2.0", "3.0", "4.0", "5.0", "6.0",
                                                                    "7.0", "8.0", "9.0", POSITIVE_INFINITY));
        Assert.assertEquals(actualRangeEnds, expectedRangeEnds);

    }
}
