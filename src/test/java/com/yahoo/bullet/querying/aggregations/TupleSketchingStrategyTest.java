/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.aggregations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.aggregations.GroupBy;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.querying.aggregations.sketches.KMVSketch;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.assertContains;
import static com.yahoo.bullet.querying.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.querying.aggregations.grouping.GroupOperation.GroupOperationType.SUM;
import static com.yahoo.bullet.TestHelpers.addMetadata;
import static com.yahoo.bullet.querying.aggregations.AggregationUtils.makeGroupFields;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class TupleSketchingStrategyTest {
    private static List<Map.Entry<Concept, String>> ALL_METADATA =
            asList(Pair.of(Concept.SKETCH_METADATA, "aggregate_stats"),
                   Pair.of(Concept.SKETCH_THETA, "theta"),
                   Pair.of(Concept.SKETCH_ESTIMATED_RESULT, "isEstimate"),
                   Pair.of(Concept.SKETCH_UNIQUES_ESTIMATE, "uniquesApprox"),
                   Pair.of(Concept.SKETCH_STANDARD_DEVIATIONS, "stddev"));

    public static TupleSketchingStrategy makeGroupBy(BulletConfig configuration, Map<String, String> fields, int size,
                                                     List<GroupOperation> operations, List<Map.Entry<Concept, String>> metadata) {
        GroupBy aggregation = new GroupBy(size, fields, new HashSet<>(operations));
        return (TupleSketchingStrategy) aggregation.getStrategy(addMetadata(configuration, metadata));
    }

    public static TupleSketchingStrategy makeGroupBy(BulletConfig configuration, Map<String, String> fields, int size,
                                                     GroupOperation... operations) {
        return makeGroupBy(configuration, fields, size, asList(operations), ALL_METADATA);
    }

    public static TupleSketchingStrategy makeGroupBy(Map<String, String> fields, int size, GroupOperation... operations) {
        return makeGroupBy(makeConfiguration(16), fields, size, asList(operations), ALL_METADATA);
    }

    public static TupleSketchingStrategy makeGroupBy(List<String> fields, int size, GroupOperation... operations) {
        return makeGroupBy(makeGroupFields(fields), size, operations);
    }

    public static TupleSketchingStrategy makeDistinct(Map<String, String> fields, int size) {
        return makeGroupBy(fields, size);
    }

    public static TupleSketchingStrategy makeDistinct(List<String> fields, int size) {
        return makeDistinct(makeGroupFields(fields), size);
    }

    public static BulletConfig makeConfiguration(int resizeFactor, float sampling, String separator, int k) {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.GROUP_AGGREGATION_SKETCH_ENTRIES, k);
        config.set(BulletConfig.GROUP_AGGREGATION_SKETCH_RESIZE_FACTOR, resizeFactor);
        config.set(BulletConfig.GROUP_AGGREGATION_SKETCH_SAMPLING, sampling);
        config.set(BulletConfig.AGGREGATION_COMPOSITE_FIELD_SEPARATOR, separator);
        return config;
    }

    public static BulletConfig makeConfiguration(int k) {
        return makeConfiguration(BulletConfig.DEFAULT_COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR,
                                 BulletConfig.DEFAULT_GROUP_AGGREGATION_SKETCH_SAMPLING,
                                 BulletConfig.DEFAULT_AGGREGATION_COMPOSITE_FIELD_SEPARATOR, k);
    }

    @Test
    public void testDistincts() {
        List<String> fields = asList("fieldA", "fieldB", "fieldC");
        TupleSketchingStrategy groupBy = makeDistinct(fields, 3);

        BulletRecord recordA = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar").add("fieldC", "baz").getRecord();
        IntStream.range(0, 9).forEach(i -> groupBy.consume(recordA));

        BulletRecord recordB = RecordBox.get().add("fieldA", "1").add("fieldB", "2").getRecord();
        IntStream.range(0, 9).forEach(i -> groupBy.consume(recordB));

        groupBy.consume(RecordBox.get().getRecord());

        Clip aggregate = groupBy.getResult();
        Assert.assertNotNull(aggregate);

        Map<String, Object> meta = aggregate.getMeta().asMap();
        Assert.assertEquals(meta.size(), 1);

        List<BulletRecord> records = aggregate.getRecords();

        Assert.assertEquals(records.size(), 3);

        BulletRecord expectedA = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar").add("fieldC", "baz").getRecord();
        BulletRecord expectedB = RecordBox.get().add("fieldA", "1").add("fieldB", "2").add("fieldC", "null").getRecord();
        BulletRecord expectedC = RecordBox.get().add("fieldA", "null").add("fieldB", "null").add("fieldC", "null").getRecord();

        // We have each distinct record exactly once
        assertContains(records, expectedA);
        assertContains(records, expectedB);
        assertContains(records, expectedC);

        Assert.assertEquals(groupBy.getRecords(), aggregate.getRecords());
        Assert.assertEquals(groupBy.getMetadata().asMap(), aggregate.getMeta().asMap());
    }

    @Test
    public void testGroupByOperations() {
        List<String> fields = asList("fieldA", "fieldB");
        TupleSketchingStrategy groupBy = makeGroupBy(fields, 3, new GroupOperation(COUNT, null, "count"),
                                                     new GroupOperation(SUM, "price", "priceSum"));

        BulletRecord recordA = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar").add("price", 3).getRecord();
        BulletRecord recordB = RecordBox.get().addNull("fieldA").add("fieldB", "bar").add("price", 1).getRecord();

        IntStream.range(0, 10).forEach(i -> groupBy.consume(recordA));
        IntStream.range(0, 9).forEach(i -> groupBy.consume(recordB));
        IntStream.range(0, 20).forEach(i -> groupBy.consume(recordA));
        groupBy.consume(recordB);

        Clip aggregate = groupBy.getResult();
        Assert.assertNotNull(aggregate);

        Map<String, Object> meta = aggregate.getMeta().asMap();
        Assert.assertEquals(meta.size(), 1);

        List<BulletRecord> records = aggregate.getRecords();

        Assert.assertEquals(records.size(), 2);

        // count = 10 + 20, price = 10*3 + 20*3
        BulletRecord expectedA = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar")
                                                .add("count", 30L).add("priceSum", 90.0).getRecord();
        // count = 9 + 1, price = 9*1 + 1*1
        BulletRecord expectedB = RecordBox.get().add("fieldA", "null").add("fieldB", "bar")
                                                .add("count", 10L).add("priceSum", 10.0).getRecord();

        assertContains(records, expectedA);
        assertContains(records, expectedB);

        Assert.assertEquals(groupBy.getRecords(), aggregate.getRecords());
        Assert.assertEquals(groupBy.getMetadata().asMap(), aggregate.getMeta().asMap());
    }

    @Test
    public void testMoreGroupsThanNominalEntries() {
        Map<String, String> fields = singletonMap("fieldA", "A");

        // Nominal Entries is 32. Aggregation size is also 32
        TupleSketchingStrategy groupBy = makeGroupBy(makeConfiguration(32), fields, 32,
                                                     singletonList(new GroupOperation(COUNT, null, "count")), ALL_METADATA);

        // Generate 4 batches of 64 records with 0 - 63 in fieldA.
        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("fieldA", i % 64).getRecord()).forEach(groupBy::consume);

        Clip aggregate = groupBy.getResult();
        Assert.assertNotNull(aggregate);

        Map<String, Object> meta = aggregate.getMeta().asMap();
        Assert.assertEquals(meta.size(), 1);

        List<BulletRecord> records = aggregate.getRecords();
        Assert.assertEquals(records.size(), 32);

        Set<String> groups = new HashSet<>();
        for (BulletRecord record : records) {
            groups.add((String) record.typedGet("A").getValue());
            Assert.assertEquals(record.typedGet("count").getValue(), 4L);
        }
        Assert.assertEquals(groups.size(), 32);

        Assert.assertEquals(groupBy.getRecords(), aggregate.getRecords());
        Assert.assertEquals(groupBy.getMetadata().asMap(), aggregate.getMeta().asMap());
    }

    @Test
    public void testCombining() {
        List<String> fields = asList("fieldA", "fieldB");
        TupleSketchingStrategy groupBy = makeGroupBy(fields, 5, new GroupOperation(COUNT, null, "count"),
                                                      new GroupOperation(SUM, "price", "priceSum"));

        BulletRecord recordA = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar").add("price", 3).getRecord();
        BulletRecord recordB = RecordBox.get().add("fieldA", "null").add("fieldB", "bar").add("price", 1).getRecord();

        IntStream.range(0, 10).mapToObj(i -> recordA).forEach(groupBy::consume);
        IntStream.range(0, 9).mapToObj(i -> recordB).forEach(groupBy::consume);
        IntStream.range(0, 20).mapToObj(i -> recordA).forEach(groupBy::consume);

        groupBy.consume(recordB);

        byte[] firstSerialized = groupBy.getData();

        // Remake it
        groupBy = makeGroupBy(fields, 5, new GroupOperation(COUNT, null, "count"),
                                         new GroupOperation(SUM, "price", "priceSum"));

        BulletRecord recordC = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar").add("price", 3).getRecord();
        BulletRecord recordD = RecordBox.get().addNull("fieldA").addNull("fieldB").add("price", 10).getRecord();

        IntStream.range(0, 30).mapToObj(i -> recordC).forEach(groupBy::consume);
        IntStream.range(0, 10).mapToObj(i -> recordD).forEach(groupBy::consume);

        byte[] secondSerialized = groupBy.getData();

        groupBy = makeGroupBy(fields, 5, new GroupOperation(COUNT, null, "count"),
                                         new GroupOperation(SUM, "price", "priceSum"));

        groupBy.combine(firstSerialized);
        groupBy.combine(secondSerialized);

        Clip aggregate = groupBy.getResult();
        Assert.assertNotNull(aggregate);

        List<BulletRecord> records = aggregate.getRecords();
        Assert.assertEquals(records.size(), 3);

        // count = 10 + 20 + 30, price = 10*3 + 20*3 + 30*3
        BulletRecord expectedA = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar")
                                                .add("count", 60L).add("priceSum", 180.0).getRecord();
        // count = 9 + 1, price = 9*1 + 1*1
        BulletRecord expectedB = RecordBox.get().add("fieldA", "null").add("fieldB", "bar")
                                                .add("count", 10L).add("priceSum", 10.0).getRecord();
        // count = 10, price = 10*10
        BulletRecord expectedC = RecordBox.get().add("fieldA", "null").add("fieldB", "null")
                                                .add("count", 10L).add("priceSum", 100.0).getRecord();

        assertContains(records, expectedA);
        assertContains(records, expectedB);
        assertContains(records, expectedC);

        Assert.assertEquals(groupBy.getRecords(), aggregate.getRecords());
        Assert.assertEquals(groupBy.getMetadata().asMap(), aggregate.getMeta().asMap());
    }

    @Test
    public void testCombiningAndConsuming() {
        List<String> fields = asList("fieldA", "fieldB");
        TupleSketchingStrategy groupBy = makeGroupBy(fields, 5, new GroupOperation(COUNT, null, "count"),
                                                     new GroupOperation(SUM, "price", "priceSum"));

        BulletRecord recordA = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar").add("price", 3).getRecord();
        BulletRecord recordB = RecordBox.get().add("fieldA", "null").add("fieldB", "bar").add("price", 1).getRecord();

        IntStream.range(0, 30).mapToObj(i -> recordA).forEach(groupBy::consume);
        IntStream.range(0, 10).mapToObj(i -> recordB).forEach(groupBy::consume);

        byte[] serialized = groupBy.getData();

        // Remake it
        groupBy = makeGroupBy(fields, 5, new GroupOperation(COUNT, null, "count"),
                                         new GroupOperation(SUM, "price", "priceSum"));

        BulletRecord recordC = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar").add("price", 3).getRecord();
        BulletRecord recordD = RecordBox.get().addNull("fieldA").addNull("fieldB").add("price", 10).getRecord();

        IntStream.range(0, 30).mapToObj(i -> recordC).forEach(groupBy::consume);
        IntStream.range(0, 10).mapToObj(i -> recordD).forEach(groupBy::consume);

        groupBy.combine(serialized);

        Clip aggregate = groupBy.getResult();
        Assert.assertNotNull(aggregate);

        List<BulletRecord> records = aggregate.getRecords();
        Assert.assertEquals(records.size(), 3);

        BulletRecord expectedA = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar")
                                                .add("count", 60L).add("priceSum", 180.0).getRecord();
        BulletRecord expectedB = RecordBox.get().add("fieldA", "null").add("fieldB", "bar")
                                                .add("count", 10L).add("priceSum", 10.0).getRecord();
        BulletRecord expectedC = RecordBox.get().add("fieldA", "null").add("fieldB", "null")
                                                .add("count", 10L).add("priceSum", 100.0).getRecord();

        assertContains(records, expectedA);
        assertContains(records, expectedB);
        assertContains(records, expectedC);

        Assert.assertEquals(groupBy.getRecords(), aggregate.getRecords());
        Assert.assertEquals(groupBy.getMetadata().asMap(), aggregate.getMeta().asMap());
    }

    @Test
    public void testMetadata() {
        Map<String, String> fields = singletonMap("fieldA", "fieldA");
        // Nominal Entries is 32. Aggregation size is also 32
        TupleSketchingStrategy groupBy = makeGroupBy(makeConfiguration(32), fields, 32,
                                                     singletonList(new GroupOperation(COUNT, null, "count")), ALL_METADATA);

        // Generate 4 batches of 64 records with 0 - 63 in fieldA.
        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("fieldA", i % 64).getRecord()).forEach(groupBy::consume);
        Clip aggregate = groupBy.getResult();
        Assert.assertNotNull(aggregate);

        List<BulletRecord> records = aggregate.getRecords();
        Assert.assertEquals(records.size(), 32);
        records.forEach(r -> Assert.assertTrue(Integer.valueOf((String) r.typedGet("fieldA").getValue()) < 64));

        Map<String, Object> meta = aggregate.getMeta().asMap();
        Assert.assertEquals(meta.size(), 1);

        Map<String, Object> stats = (Map<String, Object>) meta.get("aggregate_stats");
        Assert.assertEquals(stats.size(), 4);

        Assert.assertTrue((Boolean) stats.get("isEstimate"));

        double theta = (Double) stats.get("theta");
        Assert.assertTrue(theta <= 1.0);

        double groupEstimate = (Double) stats.get("uniquesApprox");

        Assert.assertTrue(stats.containsKey("stddev"));
        Map<String, Map<String, Double>> standardDeviations = (Map<String, Map<String, Double>>) stats.get("stddev");
        Assert.assertEquals(standardDeviations.size(), 3);

        double upperOneSigma = standardDeviations.get(KMVSketch.META_STD_DEV_1).get(KMVSketch.META_STD_DEV_UB);
        double lowerOneSigma = standardDeviations.get(KMVSketch.META_STD_DEV_1).get(KMVSketch.META_STD_DEV_LB);
        double upperTwoSigma = standardDeviations.get(KMVSketch.META_STD_DEV_2).get(KMVSketch.META_STD_DEV_UB);
        double lowerTwoSigma = standardDeviations.get(KMVSketch.META_STD_DEV_2).get(KMVSketch.META_STD_DEV_LB);
        double upperThreeSigma = standardDeviations.get(KMVSketch.META_STD_DEV_3).get(KMVSketch.META_STD_DEV_UB);
        double lowerThreeSigma = standardDeviations.get(KMVSketch.META_STD_DEV_3).get(KMVSketch.META_STD_DEV_LB);

        Assert.assertTrue(groupEstimate >= lowerOneSigma);
        Assert.assertTrue(groupEstimate <= upperOneSigma);
        Assert.assertTrue(groupEstimate >= lowerTwoSigma);
        Assert.assertTrue(groupEstimate <= upperTwoSigma);
        Assert.assertTrue(groupEstimate >= lowerThreeSigma);
        Assert.assertTrue(groupEstimate <= upperThreeSigma);
        Assert.assertTrue(groupEstimate <= upperThreeSigma);

        Assert.assertEquals(groupBy.getRecords(), aggregate.getRecords());
        Assert.assertEquals(groupBy.getMetadata().asMap(), aggregate.getMeta().asMap());
    }

    @Test
    public void testResetting() {
        List<String> fields = asList("fieldA", "fieldB");
        TupleSketchingStrategy groupBy = makeGroupBy(fields, 5, new GroupOperation(COUNT, null, "count"),
                                                     new GroupOperation(SUM, "price", "priceSum"));

        BulletRecord recordA = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar").add("price", 3).getRecord();
        BulletRecord recordB = RecordBox.get().add("fieldA", "null").add("fieldB", "bar").add("price", 1).getRecord();

        IntStream.range(0, 30).mapToObj(i -> recordA).forEach(groupBy::consume);
        IntStream.range(0, 10).mapToObj(i -> recordB).forEach(groupBy::consume);

        BulletRecord expectedA = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar")
                                                .add("count", 30L).add("priceSum", 90.0).getRecord();
        BulletRecord expectedB = RecordBox.get().add("fieldA", "null").add("fieldB", "bar")
                                                .add("count", 10L).add("priceSum", 10.0).getRecord();

        Clip aggregate = groupBy.getResult();
        Assert.assertNotNull(aggregate);
        List<BulletRecord> records = aggregate.getRecords();
        Assert.assertEquals(records.size(), 2);

        assertContains(records, expectedA);
        assertContains(records, expectedB);

        Assert.assertEquals(groupBy.getRecords(), aggregate.getRecords());
        Assert.assertEquals(groupBy.getMetadata().asMap(), aggregate.getMeta().asMap());

        groupBy.reset();

        BulletRecord recordC = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar").add("price", 3).getRecord();
        BulletRecord recordD = RecordBox.get().addNull("fieldA").addNull("fieldB").add("price", 10).getRecord();

        IntStream.range(0, 10).mapToObj(i -> recordB).forEach(groupBy::consume);
        IntStream.range(0, 30).mapToObj(i -> recordC).forEach(groupBy::consume);
        IntStream.range(0, 10).mapToObj(i -> recordD).forEach(groupBy::consume);


        aggregate = groupBy.getResult();
        Assert.assertNotNull(aggregate);
        records = aggregate.getRecords();
        Assert.assertEquals(records.size(), 3);

        expectedA = RecordBox.get().add("fieldA", "null").add("fieldB", "bar")
                                   .add("count", 10L).add("priceSum", 10.0).getRecord();
        expectedB = RecordBox.get().add("fieldA", "foo").add("fieldB", "bar")
                                   .add("count", 30L).add("priceSum", 90.0).getRecord();
        BulletRecord expectedC = RecordBox.get().add("fieldA", "null").add("fieldB", "null")
                                                .add("count", 10L).add("priceSum", 100.0).getRecord();

        assertContains(records, expectedA);
        assertContains(records, expectedB);
        assertContains(records, expectedC);

        Assert.assertEquals(groupBy.getRecords(), aggregate.getRecords());
        Assert.assertEquals(groupBy.getMetadata().asMap(), aggregate.getMeta().asMap());
    }
}
