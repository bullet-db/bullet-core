/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.aggregations.sketches.KMVSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.yahoo.bullet.parsing.AggregationUtils.addMetadata;
import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.parsing.AggregationUtils.makeGroupFields;
import static java.util.Arrays.asList;

public class CountDistinctTest {
    @SafeVarargs
    public static CountDistinct makeCountDistinct(BulletConfig configuration, Map<String, Object> attributes,
                                                  List<String> fields, Map.Entry<Concept, String>... metadata) {
        Aggregation aggregation = new Aggregation();
        Map<String, String> asMap = makeGroupFields(fields);
        aggregation.setFields(asMap);
        aggregation.setAttributes(attributes);

        CountDistinct countDistinct = new CountDistinct(aggregation, addMetadata(configuration, metadata).validate());
        countDistinct.initialize();
        return countDistinct;
    }

    @SafeVarargs
    public static CountDistinct makeCountDistinct(List<String> fields, String newName, Map.Entry<Concept, String>... metadata) {
        return makeCountDistinct(makeConfiguration(8, 1024), makeAttributes(newName), fields, metadata);
    }

    public static CountDistinct makeCountDistinct(List<String> fields, String newName) {
        return makeCountDistinct(fields, newName, (Map.Entry<Concept, String>[]) null);
    }

    public static CountDistinct makeCountDistinct(List<String> fields) {
        return makeCountDistinct(fields, null);
    }

    @SafeVarargs
    public static CountDistinct makeCountDistinct(BulletConfig configuration, List<String> fields,
                                                  Map.Entry<Concept, String>... metadata) {
        return makeCountDistinct(configuration, null, fields, metadata);
    }

    public static BulletConfig makeConfiguration(int resizeFactor, float sampling, String family, String separator, int k) {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES, k);
        config.set(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR, resizeFactor);
        config.set(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY, family);
        config.set(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING, sampling);
        config.set(BulletConfig.AGGREGATION_COMPOSITE_FIELD_SEPARATOR, separator);
        return config;
    }

    public static BulletConfig makeConfiguration(int resizeFactor, int k) {
        return makeConfiguration(resizeFactor, BulletConfig.DEFAULT_COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING,
                                 BulletConfig.DEFAULT_COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY,
                                 BulletConfig.DEFAULT_AGGREGATION_COMPOSITE_FIELD_SEPARATOR, k);
    }

    @Test
    public void testFamilyConversion() {
        Assert.assertEquals(CountDistinct.getFamily(Family.ALPHA.getFamilyName()), Family.ALPHA);
        Assert.assertEquals(CountDistinct.getFamily(Family.QUICKSELECT.getFamilyName()), Family.QUICKSELECT);
        Assert.assertEquals(CountDistinct.getFamily(Family.COMPACT.getFamilyName()), Family.ALPHA);
        Assert.assertEquals(CountDistinct.getFamily("foo"), Family.ALPHA);
        Assert.assertEquals(CountDistinct.getFamily(null), Family.ALPHA);
        Assert.assertEquals(CountDistinct.getFamily(""), Family.ALPHA);
    }

    @Test
    public void testResizeFactorConversion() {
        Assert.assertEquals(CountDistinct.getResizeFactor(1), ResizeFactor.X1);
        Assert.assertEquals(CountDistinct.getResizeFactor(2), ResizeFactor.X2);
        Assert.assertEquals(CountDistinct.getResizeFactor(4), ResizeFactor.X4);
        Assert.assertEquals(CountDistinct.getResizeFactor(8), ResizeFactor.X8);

        Assert.assertEquals(CountDistinct.getResizeFactor(0), ResizeFactor.X8);
        Assert.assertEquals(CountDistinct.getResizeFactor(3), ResizeFactor.X8);
        Assert.assertEquals(CountDistinct.getResizeFactor(17), ResizeFactor.X8);
        Assert.assertEquals(CountDistinct.getResizeFactor(-10), ResizeFactor.X8);
    }

    @Test
    public void testNoRecordCount() {
        CountDistinct countDistinct = makeCountDistinct(asList("field"));

        Assert.assertNotNull(countDistinct.getData());
        List<BulletRecord> aggregate = countDistinct.getResult().getRecords();

        Assert.assertEquals(aggregate.size(), 1);

        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add(CountDistinct.DEFAULT_NEW_NAME, 0.0).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testSingleFieldExactCountDistinctWithoutDuplicates() {
        CountDistinct countDistinct = makeCountDistinct(asList("field"));

        IntStream.range(0, 1000).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                                .forEach(countDistinct::consume);

        Assert.assertNotNull(countDistinct.getData());
        List<BulletRecord> aggregate = countDistinct.getResult().getRecords();

        Assert.assertEquals(aggregate.size(), 1);

        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add(CountDistinct.DEFAULT_NEW_NAME, 1000.0).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testSingleFieldExactCountDistinctWithDuplicates() {
        CountDistinct countDistinct = makeCountDistinct(asList("field"));

        IntStream.range(0, 1000).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                                .forEach(countDistinct::consume);
        IntStream.range(0, 1000).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                                .forEach(countDistinct::consume);

        Assert.assertNotNull(countDistinct.getData());
        List<BulletRecord> aggregate = countDistinct.getResult().getRecords();

        Assert.assertEquals(aggregate.size(), 1);

        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add(CountDistinct.DEFAULT_NEW_NAME, 1000.0).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testSingleFieldApproximateCountDistinctWithMetadata() {
        BulletConfig config = makeConfiguration(4, 512);
        CountDistinct countDistinct = makeCountDistinct(config, asList("field"),
                                                        Pair.of(Concept.SKETCH_METADATA, "aggregate_stats"),
                                                        Pair.of(Concept.FAMILY, "family"),
                                                        Pair.of(Concept.SIZE, "size"),
                                                        Pair.of(Concept.THETA, "theta"),
                                                        Pair.of(Concept.ESTIMATED_RESULT, "isEstimate"),
                                                        Pair.of(Concept.STANDARD_DEVIATIONS, "stddev"));
        IntStream.range(0, 1000).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                                .forEach(countDistinct::consume);

        Assert.assertNotNull(countDistinct.getData());
        Clip clip = countDistinct.getResult();

        Map<String, Object> meta = clip.getMeta().asMap();
        Assert.assertEquals(meta.size(), 1);
        Assert.assertTrue(meta.containsKey("aggregate_stats"));

        Map<String, Object> stats = (Map<String, Object>) meta.get("aggregate_stats");
        Assert.assertEquals(stats.size(), 5);

        Assert.assertTrue((Boolean) stats.get("isEstimate"));
        Assert.assertEquals(stats.get("family").toString(), Family.ALPHA.getFamilyName());

        int size = (Integer) stats.get("size");
        // We inserted more than 512 unique entries
        Assert.assertTrue(size > 512);

        double theta = (Double) stats.get("theta");
        Assert.assertTrue(theta <= 1.0);

        Assert.assertTrue(stats.containsKey("stddev"));
        Map<String, Map<String, Double>> standardDeviations = (Map<String, Map<String, Double>>) stats.get("stddev");
        Assert.assertEquals(standardDeviations.size(), 3);

        Assert.assertEquals(clip.getRecords().size(), 1);
        BulletRecord actual = clip.getRecords().get(0);
        double actualEstimate = (Double) actual.get(CountDistinct.DEFAULT_NEW_NAME);

        double upperOneSigma = standardDeviations.get(KMVSketch.META_STD_DEV_1).get(KMVSketch.META_STD_DEV_UB);
        double lowerOneSigma = standardDeviations.get(KMVSketch.META_STD_DEV_1).get(KMVSketch.META_STD_DEV_LB);
        double upperTwoSigma = standardDeviations.get(KMVSketch.META_STD_DEV_2).get(KMVSketch.META_STD_DEV_UB);
        double lowerTwoSigma = standardDeviations.get(KMVSketch.META_STD_DEV_2).get(KMVSketch.META_STD_DEV_LB);
        double upperThreeSigma = standardDeviations.get(KMVSketch.META_STD_DEV_3).get(KMVSketch.META_STD_DEV_UB);
        double lowerThreeSigma = standardDeviations.get(KMVSketch.META_STD_DEV_3).get(KMVSketch.META_STD_DEV_LB);

        Assert.assertTrue(actualEstimate >= lowerOneSigma);
        Assert.assertTrue(actualEstimate <= upperOneSigma);
        Assert.assertTrue(actualEstimate >= lowerTwoSigma);
        Assert.assertTrue(actualEstimate <= upperTwoSigma);
        Assert.assertTrue(actualEstimate >= lowerThreeSigma);
        Assert.assertTrue(actualEstimate <= upperThreeSigma);
    }

    @Test
    public void testNewNamingOfResult() {
        BulletConfig config = makeConfiguration(4, 1024);
        CountDistinct countDistinct = makeCountDistinct(config, makeAttributes("myCount"), asList("field"),
                                                        Pair.of(Concept.SKETCH_METADATA, "stats"),
                                                        Pair.of(Concept.ESTIMATED_RESULT, "est"));

        IntStream.range(0, 1000).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                                .forEach(countDistinct::consume);

        Clip clip = countDistinct.getResult();

        Map<String, Object> meta = clip.getMeta().asMap();
        Assert.assertEquals(meta.size(), 1);
        Assert.assertTrue(meta.containsKey("stats"));
        Map<String, Object> stats = (Map<String, Object>) meta.get("stats");
        Assert.assertEquals(stats.size(), 1);
        Assert.assertFalse((Boolean) stats.get("est"));

        Assert.assertEquals(clip.getRecords().size(), 1);
        BulletRecord actual = clip.getRecords().get(0);
        BulletRecord expected = RecordBox.get().add("myCount", 1000.0).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCombiningExact() {
        BulletConfig config = makeConfiguration(4, 1024);
        CountDistinct countDistinct = makeCountDistinct(config, makeAttributes("myCount"), asList("field"));

        IntStream.range(0, 512).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                               .forEach(countDistinct::consume);

        byte[] firstAggregate = countDistinct.getData();

        // Another one
        countDistinct = makeCountDistinct(config, makeAttributes("myCount"), asList("field"));

        IntStream.range(256, 768).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                               .forEach(countDistinct::consume);

        byte[] secondAggregate = countDistinct.getData();

        // Final one
        countDistinct = makeCountDistinct(config, makeAttributes("myCount"), asList("field"),
                                          Pair.of(Concept.SKETCH_METADATA, "stats"),
                                          Pair.of(Concept.ESTIMATED_RESULT, "est"));

        countDistinct.combine(firstAggregate);
        countDistinct.combine(secondAggregate);

        Clip clip = countDistinct.getResult();

        Map<String, Object> meta = clip.getMeta().asMap();
        Assert.assertEquals(meta.size(), 1);
        Assert.assertTrue(meta.containsKey("stats"));
        Map<String, Object> stats = (Map<String, Object>) meta.get("stats");
        Assert.assertEquals(stats.size(), 1);
        Assert.assertFalse((Boolean) stats.get("est"));

        Assert.assertEquals(clip.getRecords().size(), 1);
        BulletRecord actual = clip.getRecords().get(0);
        BulletRecord expected = RecordBox.get().add("myCount", 768.0).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCombiningAndConsuming() {
        BulletConfig config = makeConfiguration(4, 1024);
        CountDistinct countDistinct = makeCountDistinct(config, makeAttributes("myCount"), asList("field"));

        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                                .forEach(countDistinct::consume);

        byte[] aggregate = countDistinct.getData();

        // New one
        countDistinct = makeCountDistinct(config, makeAttributes("myCount"), asList("field"));

        IntStream.range(0, 768).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                .forEach(countDistinct::consume);

        countDistinct.combine(aggregate);


        Clip clip = countDistinct.getResult();

        Map<String, Object> meta = clip.getMeta().asMap();
        Assert.assertEquals(meta.size(), 0);

        Assert.assertEquals(clip.getRecords().size(), 1);
        BulletRecord actual = clip.getRecords().get(0);
        BulletRecord expected = RecordBox.get().add("myCount", 768.0).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testMultipleFieldsCountDistinct() {
        BulletConfig config = makeConfiguration(4, 512);
        CountDistinct countDistinct = makeCountDistinct(config, makeAttributes("myCount"), asList("fieldA", "fieldB"));
        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("fieldA", i).add("fieldB", 255 - i).getRecord())
                               .forEach(countDistinct::consume);
        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("fieldA", i).add("fieldB", 255 - i).getRecord())
                               .forEach(countDistinct::consume);

        Clip clip = countDistinct.getResult();
        Assert.assertEquals(clip.getRecords().size(), 1);
        BulletRecord actual = clip.getRecords().get(0);
        BulletRecord expected = RecordBox.get().add("myCount", 256.0).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testMultipleFieldsCountDistinctAmbiguity() {
        BulletConfig config = makeConfiguration(4, 512);

        String s = BulletConfig.DEFAULT_AGGREGATION_COMPOSITE_FIELD_SEPARATOR;
        CountDistinct countDistinct = makeCountDistinct(config, makeAttributes("myCount"), asList("fieldA", "fieldB"));
        BulletRecord first = RecordBox.get().add("fieldA", s).add("fieldB", s + s).getRecord();
        BulletRecord second = RecordBox.get().add("fieldA", s + s).add("fieldB", s).getRecord();
        // first and second will look the same to the Sketch. third will not
        BulletRecord third = RecordBox.get().add("fieldA", s + s).add("fieldB", s + s).getRecord();

        countDistinct.consume(first);
        countDistinct.consume(second);
        countDistinct.consume(third);

        Clip clip = countDistinct.getResult();
        Assert.assertEquals(clip.getRecords().size(), 1);
        BulletRecord actual = clip.getRecords().get(0);
        BulletRecord expected = RecordBox.get().add("myCount", 2.0).getRecord();
        Assert.assertEquals(actual, expected);
    }
}
