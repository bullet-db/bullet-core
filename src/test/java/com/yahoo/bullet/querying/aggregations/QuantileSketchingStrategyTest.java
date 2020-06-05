/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.aggregations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.aggregations.Distribution;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.aggregations.LinearDistribution;
import com.yahoo.bullet.query.aggregations.ManualDistribution;
import com.yahoo.bullet.query.aggregations.RegionDistribution;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.DoublesSketch;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.assertApproxEquals;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.COUNT_FIELD;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.END_EXCLUSIVE;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.NEGATIVE_INFINITY_START;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.POSITIVE_INFINITY_END;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.PROBABILITY_FIELD;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.QUANTILE_FIELD;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.RANGE_FIELD;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.SEPARATOR;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.START_INCLUSIVE;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.VALUE_FIELD;
import static com.yahoo.bullet.TestHelpers.addMetadata;
import static java.util.Arrays.asList;

public class QuantileSketchingStrategyTest {
    private static class MockDistribution extends Distribution {
        MockDistribution(String field, DistributionType type, Integer size) {
            super(field, type, size);
        }
    }

    private static final List<Map.Entry<Concept, String>> ALL_METADATA =
        asList(Pair.of(Concept.SKETCH_ESTIMATED_RESULT, "isEst"),
               Pair.of(Concept.SKETCH_FAMILY, "family"),
               Pair.of(Concept.SKETCH_SIZE, "size"),
               Pair.of(Concept.SKETCH_NORMALIZED_RANK_ERROR, "nre"),
               Pair.of(Concept.SKETCH_ITEMS_SEEN, "n"),
               Pair.of(Concept.SKETCH_MINIMUM_VALUE, "min"),
               Pair.of(Concept.SKETCH_MAXIMUM_VALUE, "max"),
               Pair.of(Concept.SKETCH_METADATA, "meta"));

    public static QuantileSketchingStrategy makeDistribution(BulletConfig configuration, int size, String field, DistributionType type, int numberOfPoints) {
        LinearDistribution aggregation = new LinearDistribution(field, type, size, numberOfPoints);
        return new QuantileSketchingStrategy(aggregation, addMetadata(configuration, ALL_METADATA));
    }

    public static QuantileSketchingStrategy makeDistribution(String field, DistributionType type, int numberOfPoints) {
        LinearDistribution aggregation = new LinearDistribution(field, type, 20, numberOfPoints);
        BulletConfig configuration = makeConfiguration(100, 512);
        return new QuantileSketchingStrategy(aggregation, addMetadata(configuration, ALL_METADATA));
    }

    public static QuantileSketchingStrategy makeDistribution(DistributionType type, int maxPoints, int rounding, double start, double end, double increment) {
        RegionDistribution aggregation = new RegionDistribution("field", type, 20, start, end, increment);
        BulletConfig configuration = makeConfiguration(maxPoints, 128, rounding);
        return new QuantileSketchingStrategy(aggregation, addMetadata(configuration, ALL_METADATA));
    }

    public static QuantileSketchingStrategy makeDistribution(DistributionType type, List<Double> points) {
        ManualDistribution aggregation = new ManualDistribution("field", type, 20, points);
        BulletConfig configuration = makeConfiguration(10, 128);
        return new QuantileSketchingStrategy(aggregation, addMetadata(configuration, ALL_METADATA));
    }

    public static BulletConfig makeConfiguration(int maxPoints, int k, int rounding) {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES, k);
        config.set(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, maxPoints);
        config.set(BulletConfig.DISTRIBUTION_AGGREGATION_GENERATED_POINTS_ROUNDING, rounding);
        return config;
    }

    public static BulletConfig makeConfiguration(int maxPoints, int k) {
        return makeConfiguration(maxPoints, k, 4);
    }

    @Test
    public void testQuantiles() {
        QuantileSketchingStrategy distribution = makeDistribution("field", DistributionType.QUANTILE, 3);

        IntStream.range(0, 2000).mapToDouble(i -> (i * 0.1)).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                                .forEach(distribution::consume);

        Clip result = distribution.getResult();

        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");

        Assert.assertEquals(metadata.size(), 7);

        Assert.assertTrue((Boolean) metadata.get("isEst"));
        Assert.assertEquals((String) metadata.get("family"), Family.QUANTILES.getFamilyName());
        // Size should be at least 512 bytes since we inserted 2K uniques with sketch k set to 512
        Assert.assertTrue((Integer) metadata.get("size") >= 512);
        Assert.assertEquals(metadata.get("nre"), DoublesSketch.getNormalizedRankError(512));
        // 60 items
        Assert.assertEquals(metadata.get("n"), 2000L);
        Assert.assertEquals(metadata.get("min"), 0.0);
        Assert.assertEquals(metadata.get("max"), 199.9);

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 3);

        BulletRecord expectedA = RecordBox.get().add(QUANTILE_FIELD, 0.0)
                                                .add(VALUE_FIELD, 0.0).getRecord();
        BulletRecord expectedC = RecordBox.get().add(QUANTILE_FIELD, 1.0)
                                                .add(VALUE_FIELD, 199.9).getRecord();

        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(2), expectedC);

        BulletRecord actualB = records.get(1);
        Assert.assertEquals(actualB.typedGet(QUANTILE_FIELD).getValue(), 0.5);
        Double actualMedian = (Double) actualB.typedGet(VALUE_FIELD).getValue();
        // We insert 0,0.1, ... 199.9. Our median is around 100.0. Our NRE < 1%, so we can be pretty certain the median
        // from the sketch is around this.
        assertApproxEquals(actualMedian, 100.0, 2.0);

        Assert.assertEquals(distribution.getRecords(), result.getRecords());
        Assert.assertEquals(distribution.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testPMF() {
        QuantileSketchingStrategy distribution = makeDistribution(DistributionType.PMF, asList(5.0, 2.5));

        IntStream.range(0, 100).mapToDouble(i -> (i * 0.1)).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                               .forEach(distribution::consume);

        Clip result = distribution.getResult();

        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 7);
        Assert.assertFalse((Boolean) metadata.get("isEst"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 3);

        BulletRecord expectedA = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 2.5 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 25.0)
                                                .add(PROBABILITY_FIELD, 0.25).getRecord();
        BulletRecord expectedB = RecordBox.get().add(RANGE_FIELD, START_INCLUSIVE + 2.5 + SEPARATOR + 5.0 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 25.0)
                                                .add(PROBABILITY_FIELD, 0.25).getRecord();
        BulletRecord expectedC = RecordBox.get().add(RANGE_FIELD, START_INCLUSIVE + 5.0 + SEPARATOR + POSITIVE_INFINITY_END)
                                                .add(COUNT_FIELD, 50.0)
                                                .add(PROBABILITY_FIELD, 0.5).getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
        Assert.assertEquals(records.get(2), expectedC);

        Assert.assertEquals(distribution.getRecords(), result.getRecords());
        Assert.assertEquals(distribution.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testCDF() {
        QuantileSketchingStrategy distribution = makeDistribution(DistributionType.CDF, asList(5.0, 2.5));

        IntStream.range(0, 100).mapToDouble(i -> (i * 0.1)).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                               .forEach(distribution::consume);

        Clip result = distribution.getResult();

        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 7);
        Assert.assertFalse((Boolean) metadata.get("isEst"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 3);

        BulletRecord expectedA = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 2.5 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 25.0)
                                                .add(PROBABILITY_FIELD, 0.25).getRecord();
        BulletRecord expectedB = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 5.0 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 50.0)
                                                .add(PROBABILITY_FIELD, 0.5).getRecord();
        BulletRecord expectedC = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + POSITIVE_INFINITY_END)
                                                .add(COUNT_FIELD, 100.0)
                                                .add(PROBABILITY_FIELD, 1.0).getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
        Assert.assertEquals(records.get(2), expectedC);

        Assert.assertEquals(distribution.getRecords(), result.getRecords());
        Assert.assertEquals(distribution.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testCombining() {
        QuantileSketchingStrategy distribution = makeDistribution(DistributionType.CDF, asList(5.0, 2.5));

        IntStream.range(0, 25).mapToDouble(i -> (i * 0.1)).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                              .forEach(distribution::consume);

        QuantileSketchingStrategy anotherDistribution = makeDistribution(DistributionType.CDF, asList(5.0, 2.5));

        IntStream.range(50, 100).mapToDouble(i -> (i * 0.1)).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                                .forEach(anotherDistribution::consume);

        QuantileSketchingStrategy union = makeDistribution(DistributionType.CDF, asList(5.0, 2.5));
        union.combine(distribution.getData());
        union.combine(anotherDistribution.getData());
        Clip result = union.getResult();

        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 7);
        Assert.assertFalse((Boolean) metadata.get("isEst"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 3);

        BulletRecord expectedA = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 2.5 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 25.0)
                                                .add(PROBABILITY_FIELD, 1.0 / 3).getRecord();
        BulletRecord expectedB = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 5.0 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 25.0)
                                                .add(PROBABILITY_FIELD, 1.0 / 3).getRecord();
        BulletRecord expectedC = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + POSITIVE_INFINITY_END)
                                                .add(COUNT_FIELD, 75.0)
                                                .add(PROBABILITY_FIELD, 1.0).getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
        Assert.assertEquals(records.get(2), expectedC);

        Assert.assertEquals(union.getRecords(), records);
        Assert.assertEquals(union.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testCasting() {
        QuantileSketchingStrategy distribution = makeDistribution(DistributionType.PMF, Collections.singletonList(50.0));

        IntStream.range(0, 25).mapToObj(String::valueOf).map(s -> RecordBox.get().add("field", s).getRecord())
                              .forEach(distribution::consume);

        distribution.consume(RecordBox.get().add("field", "garbage").getRecord());
        distribution.consume(RecordBox.get().add("field", "1.0 garbage").getRecord());


        IntStream.range(50, 100).mapToDouble(i -> i).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                                .forEach(distribution::consume);

        Clip result = distribution.getResult();

        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 7);
        Assert.assertFalse((Boolean) metadata.get("isEst"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 2);

        BulletRecord expectedA = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 50.0 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 25.0)
                                                .add(PROBABILITY_FIELD, 1.0 / 3).getRecord();
        BulletRecord expectedB = RecordBox.get().add(RANGE_FIELD, START_INCLUSIVE + 50.0 + SEPARATOR + POSITIVE_INFINITY_END)
                                                .add(COUNT_FIELD, 50.0)
                                                .add(PROBABILITY_FIELD, 2.0 / 3).getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);

        Assert.assertEquals(distribution.getRecords(), result.getRecords());
        Assert.assertEquals(distribution.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testNegativeSize() {
        // MAX_POINTS is configured to -1 and we will use the min BulletConfig.DEFAULT_DISTRIBUTION_AGGREGATION_MAX_POINTS
        // and aggregation size, which is 1
        QuantileSketchingStrategy distribution = makeDistribution(makeConfiguration(-1, 128), 1, "field", DistributionType.PMF, 10);

        IntStream.range(0, 100).mapToDouble(i -> i).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                               .forEach(distribution::consume);

        Clip result = distribution.getResult();

        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 7);
        Assert.assertFalse((Boolean) metadata.get("isEst"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 2);

        BulletRecord expectedA = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 0.0 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 0.0)
                                                .add(PROBABILITY_FIELD, 0.0).getRecord();
        BulletRecord expectedB = RecordBox.get().add(RANGE_FIELD, START_INCLUSIVE + 0.0 + SEPARATOR + POSITIVE_INFINITY_END)
                                                .add(COUNT_FIELD, 100.0)
                                                .add(PROBABILITY_FIELD, 1.0).getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);

        Assert.assertEquals(distribution.getRecords(), result.getRecords());
        Assert.assertEquals(distribution.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testRounding() {
        QuantileSketchingStrategy distribution = makeDistribution(DistributionType.QUANTILE, 20, 6, 0.0, 1.0, 0.1);

        IntStream.range(0, 10).mapToDouble(i -> i * 0.1).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                              .forEach(distribution::consume);

        Clip result = distribution.getResult();

        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 7);
        Assert.assertFalse((Boolean) metadata.get("isEst"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 11);
        Set<String> actualQuantilePoints = records.stream().map(r -> r.typedGet(QUANTILE_FIELD).getValue().toString())
                                                           .collect(Collectors.toSet());
        Set<String> expectedQuantilePoints = new HashSet<>(Arrays.asList("0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6",
                                                                         "0.7", "0.8", "0.9", "1.0"));
        Assert.assertEquals(actualQuantilePoints, expectedQuantilePoints);

        Assert.assertEquals(distribution.getRecords(), result.getRecords());
        Assert.assertEquals(distribution.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testResetting() {
        QuantileSketchingStrategy distribution = makeDistribution(DistributionType.CDF, asList(5.0, 2.5));

        IntStream.range(0, 25).mapToDouble(i -> (i * 0.1)).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                 .forEach(distribution::consume);

        BulletRecord expectedA = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 2.5 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 25.0)
                                                .add(PROBABILITY_FIELD, 1.0).getRecord();
        BulletRecord expectedB = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 5.0 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 25.0)
                                                .add(PROBABILITY_FIELD, 1.0).getRecord();
        BulletRecord expectedC = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + POSITIVE_INFINITY_END)
                                                .add(COUNT_FIELD, 25.0)
                                                .add(PROBABILITY_FIELD, 1.0).getRecord();

        Clip result = distribution.getResult();
        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 3);
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
        Assert.assertEquals(records.get(2), expectedC);
        Assert.assertEquals(distribution.getRecords(), records);
        Assert.assertEquals(distribution.getMetadata().asMap(), result.getMeta().asMap());

        distribution.reset();

        IntStream.range(50, 100).mapToDouble(i -> (i * 0.1)).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                                .forEach(distribution::consume);

        expectedA = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 2.5 + END_EXCLUSIVE)
                                   .add(COUNT_FIELD, 0.0)
                                   .add(PROBABILITY_FIELD, 0.0).getRecord();
        expectedB = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 5.0 + END_EXCLUSIVE)
                                   .add(COUNT_FIELD, 0.0)
                                   .add(PROBABILITY_FIELD, 0.0).getRecord();
        expectedC = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + POSITIVE_INFINITY_END)
                                   .add(COUNT_FIELD, 50.0)
                                   .add(PROBABILITY_FIELD, 1.0).getRecord();

        result = distribution.getResult();
        records = result.getRecords();
        Assert.assertEquals(records.size(), 3);
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
        Assert.assertEquals(records.get(2), expectedC);
        Assert.assertEquals(distribution.getRecords(), records);
        Assert.assertEquals(distribution.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Unknown distribution input mode\\.")
    public void testGetSketchUnknownDistribution() {
        // coverage
        new QuantileSketchingStrategy(new MockDistribution("field", DistributionType.QUANTILE, 100), new BulletConfig());
    }
}
