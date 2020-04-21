/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.aggregations.LinearDistributionAggregation;
import com.yahoo.bullet.query.aggregations.ManualDistributionAggregation;
import com.yahoo.bullet.query.aggregations.RegionDistributionAggregation;
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
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.COUNT_FIELD;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.END_EXCLUSIVE;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.NEGATIVE_INFINITY_START;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.POSITIVE_INFINITY_END;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.PROBABILITY_FIELD;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.QUANTILE_FIELD;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.RANGE_FIELD;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.SEPARATOR;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.START_INCLUSIVE;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.VALUE_FIELD;
import static com.yahoo.bullet.TestHelpers.addMetadata;
import static java.util.Arrays.asList;

public class DistributionTest {
    private static final List<Map.Entry<Concept, String>> ALL_METADATA =
        asList(Pair.of(Concept.SKETCH_ESTIMATED_RESULT, "isEst"),
               Pair.of(Concept.SKETCH_FAMILY, "family"),
               Pair.of(Concept.SKETCH_SIZE, "size"),
               Pair.of(Concept.SKETCH_NORMALIZED_RANK_ERROR, "nre"),
               Pair.of(Concept.SKETCH_ITEMS_SEEN, "n"),
               Pair.of(Concept.SKETCH_MINIMUM_VALUE, "min"),
               Pair.of(Concept.SKETCH_MAXIMUM_VALUE, "max"),
               Pair.of(Concept.SKETCH_METADATA, "meta"));

    public static Distribution makeDistribution(BulletConfig configuration, int size, String field, Distribution.Type type, int numberOfPoints) {
        LinearDistributionAggregation aggregation = new LinearDistributionAggregation(field, type, size, numberOfPoints);
        return new Distribution(aggregation, addMetadata(configuration, ALL_METADATA));
    }

    public static Distribution makeDistribution(String field, Distribution.Type type, int numberOfPoints) {
        LinearDistributionAggregation aggregation = new LinearDistributionAggregation(field, type, 20, numberOfPoints);
        BulletConfig configuration = makeConfiguration(100, 512);
        return new Distribution(aggregation, addMetadata(configuration, ALL_METADATA));
    }

    public static Distribution makeDistribution(Distribution.Type type, int maxPoints, int rounding, double start, double end, double increment) {
        RegionDistributionAggregation aggregation = new RegionDistributionAggregation("field", type, 20, start, end, increment);
        BulletConfig configuration = makeConfiguration(maxPoints, 128, rounding);
        return new Distribution(aggregation, addMetadata(configuration, ALL_METADATA));
    }

    public static Distribution makeDistribution(Distribution.Type type, List<Double> points) {
        ManualDistributionAggregation aggregation = new ManualDistributionAggregation("field", type, 20, points);
        BulletConfig configuration = makeConfiguration(10, 128);
        return new Distribution(aggregation, addMetadata(configuration, ALL_METADATA));
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
/*
    @Test
    public void testInitialize() {
        Optional<List<BulletError>> optionalErrors;
        List<BulletError> errors;
        Aggregation aggregation = new Aggregation();
        aggregation.setSize(20);
        Distribution distribution = new Distribution(aggregation, new BulletConfig());

        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_ONE_FIELD_ERROR);

        aggregation.setFields(Collections.singletonMap("foo", "bar"));
        distribution = new Distribution(aggregation, new BulletConfig());
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_TYPE_ERROR);

        aggregation.setAttributes(Collections.singletonMap(Distribution.TYPE, "foo"));
        distribution = new Distribution(aggregation, new BulletConfig());
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_TYPE_ERROR);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Distribution.TYPE, Distribution.Type.CDF.getName());
        attributes.put(Distribution.NUMBER_OF_POINTS, 10L);
        aggregation.setAttributes(attributes);
        distribution = new Distribution(aggregation, new BulletConfig());
        Assert.assertFalse(distribution.initialize().isPresent());
    }

    @Test
    public void testRangeInitialization() {
        Aggregation aggregation = new Aggregation();
        aggregation.setSize(20);
        aggregation.setFields(Collections.singletonMap("foo", "bar"));
        Distribution distribution = new Distribution(aggregation, new BulletConfig());
        Optional<List<BulletError>> optionalErrors;
        List<BulletError> errors;

        // start  < 0
        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, -1, 1, 0.5));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 2);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);
        Assert.assertEquals(errors.get(1), Distribution.REQUIRES_POINTS_PROPER_RANGE);

        // end > 1
        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, 0, 2, 0.1));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 2);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);
        Assert.assertEquals(errors.get(1), Distribution.REQUIRES_POINTS_PROPER_RANGE);

        // both out of range
        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, 3, 4, 0.1));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 2);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);
        Assert.assertEquals(errors.get(1), Distribution.REQUIRES_POINTS_PROPER_RANGE);

        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, 0, 1, 0.2));
        optionalErrors = distribution.initialize();
        Assert.assertFalse(optionalErrors.isPresent());

        // start null
        aggregation.setAttributes(makeAttributes(Distribution.Type.PMF, null, 0.5, 0.2, null, null));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);

        // end null
        aggregation.setAttributes(makeAttributes(Distribution.Type.PMF, 1.0, null, 0.2, null, null));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);

        // increment null
        aggregation.setAttributes(makeAttributes(Distribution.Type.PMF, 1.0, 2.0, null, null, null));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);

        // end < start
        aggregation.setAttributes(makeAttributes(Distribution.Type.PMF, 25, -2, 0.5));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);

        aggregation.setAttributes(makeAttributes(Distribution.Type.PMF, 25, 200, 0.5));
        optionalErrors = distribution.initialize();
        Assert.assertFalse(optionalErrors.isPresent());
    }

    @Test
    public void testNumberOfPointsInitialization() {
        Aggregation aggregation = new Aggregation();
        aggregation.setSize(20);
        aggregation.setFields(Collections.singletonMap("foo", "bar"));
        Distribution distribution = new Distribution(aggregation, new BulletConfig());
        Optional<List<BulletError>> optionalErrors;
        List<BulletError> errors;

        // Null points
        aggregation.setAttributes(makeAttributes(Distribution.Type.PMF, null, null, null, null, null));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);

        // Negative points
        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, -10));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 2);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);
        Assert.assertEquals(errors.get(1), Distribution.REQUIRES_POINTS_PROPER_RANGE);

        // 0 points
        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, 0));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 2);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);
        Assert.assertEquals(errors.get(1), Distribution.REQUIRES_POINTS_PROPER_RANGE);

        aggregation.setAttributes(makeAttributes(Distribution.Type.PMF, 0));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);

        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, 1));
        optionalErrors = distribution.initialize();
        Assert.assertFalse(optionalErrors.isPresent());
    }

    @Test
    public void testProvidedPointsInitialization() {
        Aggregation aggregation = new Aggregation();
        aggregation.setSize(20);
        aggregation.setFields(Collections.singletonMap("foo", "bar"));
        Distribution distribution = new Distribution(aggregation, new BulletConfig());
        Optional<List<BulletError>> optionalErrors;
        List<BulletError> errors;

        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, asList(0.4, 0.03, 0.99, 0.5, 14.0)));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 2);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);
        Assert.assertEquals(errors.get(1), Distribution.REQUIRES_POINTS_PROPER_RANGE);

        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, null));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 2);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);
        Assert.assertEquals(errors.get(1), Distribution.REQUIRES_POINTS_PROPER_RANGE);

        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, Collections.emptyList()));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 2);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);
        Assert.assertEquals(errors.get(1), Distribution.REQUIRES_POINTS_PROPER_RANGE);

        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, Collections.singletonList(2.0)));
        optionalErrors = distribution.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 2);
        Assert.assertEquals(errors.get(0), Distribution.REQUIRES_POINTS_ERROR);
        Assert.assertEquals(errors.get(1), Distribution.REQUIRES_POINTS_PROPER_RANGE);

        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, Collections.singletonList(1.0)));
        optionalErrors = distribution.initialize();
        Assert.assertFalse(distribution.initialize().isPresent());

        aggregation.setAttributes(makeAttributes(Distribution.Type.QUANTILE, asList(0.4, 0.03, 0.99, 0.5, 0.35)));
        optionalErrors = distribution.initialize();
        Assert.assertFalse(optionalErrors.isPresent());

        aggregation.setAttributes(makeAttributes(Distribution.Type.PMF, Collections.singletonList(0.4)));
        optionalErrors = distribution.initialize();
        Assert.assertFalse(optionalErrors.isPresent());

        aggregation.setAttributes(makeAttributes(Distribution.Type.PMF, asList(0.4, 0.03, 0.99, 0.5, 14.0)));
        optionalErrors = distribution.initialize();
        Assert.assertFalse(optionalErrors.isPresent());
    }
*/
    @Test
    public void testQuantiles() {
        Distribution distribution = makeDistribution("field", Distribution.Type.QUANTILE, 3);

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
        Distribution distribution = makeDistribution(Distribution.Type.PMF, asList(5.0, 2.5));

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
        Distribution distribution = makeDistribution(Distribution.Type.CDF, asList(5.0, 2.5));

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
        Distribution distribution = makeDistribution(Distribution.Type.CDF, asList(5.0, 2.5));

        IntStream.range(0, 25).mapToDouble(i -> (i * 0.1)).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                              .forEach(distribution::consume);

        Distribution anotherDistribution = makeDistribution(Distribution.Type.CDF, asList(5.0, 2.5));

        IntStream.range(50, 100).mapToDouble(i -> (i * 0.1)).mapToObj(d -> RecordBox.get().add("field", d).getRecord())
                                .forEach(anotherDistribution::consume);

        Distribution union = makeDistribution(Distribution.Type.CDF, asList(5.0, 2.5));
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
        Distribution distribution = makeDistribution(Distribution.Type.PMF, Collections.singletonList(50.0));

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
        Distribution distribution = makeDistribution(makeConfiguration(-1, 128), 1, "field", Distribution.Type.PMF, 10);

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
        Distribution distribution = makeDistribution(Distribution.Type.QUANTILE, 20, 6, 0.0, 1.0, 0.1);

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
        Distribution distribution = makeDistribution(Distribution.Type.CDF, asList(5.0, 2.5));

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
}
