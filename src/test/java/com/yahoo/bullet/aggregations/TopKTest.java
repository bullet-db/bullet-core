/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.frequencies.ErrorType;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.addMetadata;
import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.parsing.AggregationUtils.makeGroupFields;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class TopKTest {
    private static final List<Map.Entry<Concept, String>> ALL_METADATA =
        asList(Pair.of(Concept.SKETCH_ESTIMATED_RESULT, "isEst"),
               Pair.of(Concept.SKETCH_FAMILY, "family"),
               Pair.of(Concept.SKETCH_SIZE, "size"),
               Pair.of(Concept.SKETCH_MAXIMUM_COUNT_ERROR, "error"),
               Pair.of(Concept.SKETCH_ITEMS_SEEN, "n"),
               Pair.of(Concept.SKETCH_ACTIVE_ITEMS, "actives"),
               Pair.of(Concept.SKETCH_METADATA, "meta"));


    public static TopK makeTopK(BulletConfig configuration, Map<String, Object> attributes,
                                Map<String, String> fields, int size, List<Map.Entry<Concept, String>> metadata) {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.TOP_K);
        aggregation.setFields(fields);
        aggregation.setAttributes(attributes);
        aggregation.setSize(size);

        TopK topK = new TopK(aggregation, addMetadata(configuration, metadata));
        topK.initialize();
        return topK;
    }

    public static TopK makeTopK(ErrorType type, List<String> fields, String newName, int maxMapSize, int size,
                                Long threshold) {
        return makeTopK(makeConfiguration(type, maxMapSize), makeAttributes(newName, threshold),
                        makeGroupFields(fields), size, ALL_METADATA);
    }

    public static TopK makeTopK(List<String> fields, int maxMapSize, int size) {
        return makeTopK(ErrorType.NO_FALSE_NEGATIVES, fields, null, maxMapSize, size, null);
    }

    public static BulletConfig makeConfiguration(ErrorType errorType, int maxMapSize) {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.TOP_K_AGGREGATION_SKETCH_ENTRIES, maxMapSize);
        config.set(BulletConfig.TOP_K_AGGREGATION_SKETCH_ERROR_TYPE,
                errorType == ErrorType.NO_FALSE_POSITIVES ? TopK.NO_FALSE_POSITIVES : TopK.NO_FALSE_NEGATIVES);
        return config;
    }

    @Test
    public void testInitialize() {
        TopK topK = makeTopK(null, 32, 10);
        Optional<List<BulletError>> optionalErrors = topK.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        List<BulletError> errors = optionalErrors.get();

        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), SketchingStrategy.REQUIRES_FIELD_ERROR);

        topK = makeTopK(asList("fieldA", "fieldB"), 32, 10);
        Assert.assertFalse(topK.initialize().isPresent());
    }

    @Test
    public void testExactTopK() {
        TopK topK = makeTopK(asList("A", "B"), 64, 20);
        IntStream.range(0, 996).mapToObj(i -> RecordBox.get().add("A", String.valueOf(i % 3)).add("B", i % 4).getRecord())
                               .forEach(topK::consume);

        Clip result = topK.getResult();

        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 5);
        Assert.assertFalse((Boolean) metadata.get("isEst"));
        Assert.assertEquals((String) metadata.get("family"), Family.FREQUENCY.getFamilyName());
        Assert.assertEquals(metadata.get("error"), 0L);
        Assert.assertEquals(metadata.get("n"), 996L);
        Assert.assertEquals(metadata.get("actives"), 12);

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 12);
        Set<Integer> fields = new HashSet<>();
        for (BulletRecord actual : records) {
            Assert.assertEquals(actual.fieldCount(), 3);
            int fieldA = Integer.valueOf(actual.get("A").toString());
            int fieldB = Integer.valueOf(actual.get("B").toString());
            Assert.assertTrue(fieldA < 3);
            Assert.assertTrue(fieldB < 4);
            fields.add(fieldA * 3 + fieldB * 4);
            Assert.assertEquals(actual.get(TopK.DEFAULT_NEW_NAME), 83L);
        }
        Assert.assertEquals(fields.size(), 12);

        Assert.assertEquals(topK.getRecords(), records);
        Assert.assertEquals(topK.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testExactTopKSizeLimiting() {
        TopK topK = makeTopK(ErrorType.NO_FALSE_POSITIVES, singletonList("A"), "cnt", 64, 2, null);
        IntStream.range(0, 20).mapToObj(i -> RecordBox.get().add("A", i).getRecord()).forEach(topK::consume);
        IntStream.range(0, 20).mapToObj(i -> RecordBox.get().add("A", 108.1).getRecord()).forEach(topK::consume);
        IntStream.range(0, 100).mapToObj(i -> RecordBox.get().add("A", 42L).getRecord()).forEach(topK::consume);

        Clip result = topK.getResult();

        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 5);
        Assert.assertFalse((Boolean) metadata.get("isEst"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 2);

        BulletRecord expectedA = RecordBox.get().add("A", "42").add("cnt", 100L).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "108.1").add("cnt", 20L).getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);

        Assert.assertEquals(topK.getRecords(), records);
        Assert.assertEquals(topK.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testExactTopKThreshold() {
        TopK topK = makeTopK(ErrorType.NO_FALSE_POSITIVES, asList("A", "B"), "cnt", 64, 3, 25L);
        IntStream.range(0, 20).mapToObj(i -> RecordBox.get().add("A", i).getRecord()).forEach(topK::consume);
        IntStream.range(0, 25).mapToObj(i -> RecordBox.get().add("A", 108).getRecord()).forEach(topK::consume);
        IntStream.range(0, 24).mapToObj(i -> RecordBox.get().add("A", "foo").add("B", "bar").getRecord())
                              .forEach(topK::consume);
        IntStream.range(0, 100).mapToObj(i -> RecordBox.get().add("A", 42L).getRecord()).forEach(topK::consume);

        Clip result = topK.getResult();

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 2);

        BulletRecord expectedA = RecordBox.get().add("A", "42").add("B", "null").add("cnt", 100L).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "108").add("B", "null").add("cnt", 25L).getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);

        IntStream.range(0, 11).mapToObj(i -> RecordBox.get().add("A", "foo").add("B", "bar").getRecord())
                              .forEach(topK::consume);

        result = topK.getResult();

        records = result.getRecords();
        Assert.assertEquals(records.size(), 3);

        expectedA = RecordBox.get().add("A", "42").add("B", "null").add("cnt", 100L).getRecord();
        expectedB = RecordBox.get().add("A", "foo").add("B", "bar").add("cnt", 35L).getRecord();
        BulletRecord expectedC = RecordBox.get().add("A", "108").add("B", "null").add("cnt", 25L).getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
        Assert.assertEquals(records.get(2), expectedC);

        Assert.assertEquals(topK.getRecords(), records);
        Assert.assertEquals(topK.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testApproximateTopK() {
        TopK topK = makeTopK(asList("A", "B"), 64, 4);
        IntStream.range(0, 128).mapToObj(i -> RecordBox.get().add("A", i).add("B", 128 + i).getRecord())
                               .forEach(topK::consume);
        IntStream.range(0, 60).mapToObj(i -> RecordBox.get().add("A", i % 3).getRecord()).forEach(topK::consume);
        topK.consume(RecordBox.get().add("A", 0).getRecord());
        topK.consume(RecordBox.get().add("A", 0).getRecord());
        topK.consume(RecordBox.get().add("A", 1).getRecord());

        Clip result = topK.getResult();

        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 5);
        Assert.assertTrue((Boolean) metadata.get("isEst"));

        List<BulletRecord> records = result.getRecords();
        long error = (Long) metadata.get("error");

        Assert.assertEquals(records.size(), 4);
        for (int i = 0; i < 3; ++i) {
            BulletRecord actual = records.get(i);
            int fieldA = Integer.valueOf(actual.get("A").toString());
            long count = (Long) actual.get(TopK.DEFAULT_NEW_NAME);
            if (fieldA == 0) {
                Assert.assertTrue(count >= 23L);
                Assert.assertTrue(count <= 23L + error);
            } else if (fieldA == 1) {
                Assert.assertTrue(count >= 22L);
                Assert.assertTrue(count <= 22L + error);
            } else if (fieldA == 2) {
                Assert.assertTrue(count >= 21L);
                Assert.assertTrue(count <= 21L + error);
            } else {
                Assert.fail("This case should not exist.");
            }
        }
        // The last one is one of the other records
        BulletRecord actual = records.get(3);
        long count = (Long) actual.get(TopK.DEFAULT_NEW_NAME);
        Assert.assertTrue(count >= 1L);
        Assert.assertTrue(count <= 1L + error);

        Assert.assertEquals(topK.getRecords(), records);
        Assert.assertEquals(topK.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testCombining() {
        TopK topK = makeTopK(asList("A", "B"), 64, 4);
        IntStream.range(0, 60).mapToObj(i -> RecordBox.get().add("A", i % 3).getRecord()).forEach(topK::consume);

        TopK another = makeTopK(asList("A", "B"), 64, 4);
        another.consume(RecordBox.get().add("A", 0).getRecord());
        another.consume(RecordBox.get().add("A", 0).getRecord());
        another.consume(RecordBox.get().add("A", 1).getRecord());

        TopK union = makeTopK(asList("A", "B"), 64, 4);
        union.combine(topK.getData());
        union.combine(another.getData());

        Clip result = union.getResult();

        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");
        Assert.assertEquals(metadata.size(), 5);
        Assert.assertFalse((Boolean) metadata.get("isEst"));

        List<BulletRecord> records = result.getRecords();

        Assert.assertEquals(records.size(), 3);

        BulletRecord expectedA = RecordBox.get().add("A", "0").add("B", "null").add(TopK.DEFAULT_NEW_NAME, 22L).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "1").add("B", "null").add(TopK.DEFAULT_NEW_NAME, 21L).getRecord();
        BulletRecord expectedC = RecordBox.get().add("A", "2").add("B", "null").add(TopK.DEFAULT_NEW_NAME, 20L).getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
        Assert.assertEquals(records.get(2), expectedC);

        Assert.assertEquals(union.getRecords(), records);
        Assert.assertEquals(union.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testNullAttributes() {
        TopK topK = makeTopK(makeConfiguration(ErrorType.NO_FALSE_NEGATIVES, 32), null, singletonMap("A", "foo"), 16, null);
        IntStream.range(0, 16).mapToObj(i -> RecordBox.get().add("A", i).getRecord()).forEach(topK::consume);

        Clip result = topK.getResult();
        Assert.assertNull(result.getMeta().asMap().get("meta"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 16);
        for (BulletRecord actual : records) {
            Assert.assertEquals(actual.fieldCount(), 2);
            int fieldA = Integer.valueOf(actual.get("foo").toString());
            Assert.assertTrue(fieldA < 16);
            Assert.assertEquals(actual.get(TopK.DEFAULT_NEW_NAME), 1L);
        }

        Assert.assertEquals(topK.getRecords(), records);
        Assert.assertEquals(topK.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testBadMaxMapEntries() {
        TopK topK = makeTopK(makeConfiguration(ErrorType.NO_FALSE_NEGATIVES, -1), null, singletonMap("A", "foo"),
                             BulletConfig.DEFAULT_TOP_K_AGGREGATION_SKETCH_ENTRIES, null);
        int uniqueGroups = BulletConfig.DEFAULT_TOP_K_AGGREGATION_SKETCH_ENTRIES / 4;

        IntStream.range(0, uniqueGroups).mapToObj(i -> RecordBox.get().add("A", i).getRecord())
                                        .forEach(topK::consume);

        Clip result = topK.getResult();
        Assert.assertNull(result.getMeta().asMap().get("meta"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), uniqueGroups);
        for (BulletRecord actual : records) {
            Assert.assertEquals(actual.fieldCount(), 2);
            int fieldA = Integer.valueOf(actual.get("foo").toString());
            Assert.assertTrue(fieldA < uniqueGroups);
            Assert.assertEquals(actual.get(TopK.DEFAULT_NEW_NAME), 1L);
        }

        Assert.assertEquals(topK.getRecords(), records);
        Assert.assertEquals(topK.getMetadata().asMap(), result.getMeta().asMap());

        // Not a power of 2
        TopK another = makeTopK(makeConfiguration(ErrorType.NO_FALSE_NEGATIVES, 5), null, singletonMap("A", "foo"),
                                BulletConfig.DEFAULT_TOP_K_AGGREGATION_SKETCH_ENTRIES, null);
        IntStream.range(0, uniqueGroups).mapToObj(i -> RecordBox.get().add("A", i).getRecord())
                                        .forEach(another::consume);

        result = another.getResult();
        Assert.assertNull(result.getMeta().asMap().get("meta"));

        records = result.getRecords();
        Assert.assertEquals(records.size(), uniqueGroups);

        Assert.assertEquals(another.getRecords(), records);
        Assert.assertEquals(another.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testRenaming() {
        HashMap<String, String> fields = new HashMap<>();
        fields.put("A", "foo");
        fields.put("fieldB", "");

        TopK topK = makeTopK(makeConfiguration(ErrorType.NO_FALSE_NEGATIVES, -1), null, fields, 16, null);
        IntStream.range(0, 16).mapToObj(i -> RecordBox.get().add("A", i).add("fieldB", 32 - i).getRecord())
                              .forEach(topK::consume);

        Clip result = topK.getResult();
        Assert.assertNull(result.getMeta().asMap().get("meta"));

        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.size(), 16);
        for (BulletRecord actual : records) {
            Assert.assertEquals(actual.fieldCount(), 3);
            int fieldA = Integer.valueOf(actual.get("foo").toString());
            int fieldB = Integer.valueOf(actual.get("fieldB").toString());
            Assert.assertTrue(fieldA < 16);
            Assert.assertTrue(fieldB > 16);
            Assert.assertEquals(actual.get(TopK.DEFAULT_NEW_NAME), 1L);
        }

        Assert.assertEquals(topK.getRecords(), records);
        Assert.assertEquals(topK.getMetadata().asMap(), result.getMeta().asMap());
    }

    @Test
    public void testResetting() {
        TopK topK = makeTopK(asList("A", "B"), 64, 4);
        IntStream.range(0, 60).mapToObj(i -> RecordBox.get().add("A", i % 3).getRecord()).forEach(topK::consume);

        BulletRecord expectedA = RecordBox.get().add("A", "0").add("B", "null").add(TopK.DEFAULT_NEW_NAME, 20L).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "1").add("B", "null").add(TopK.DEFAULT_NEW_NAME, 20L).getRecord();
        BulletRecord expectedC = RecordBox.get().add("A", "2").add("B", "null").add(TopK.DEFAULT_NEW_NAME, 20L).getRecord();

        Clip result = topK.getResult();
        List<BulletRecord> records = result.getRecords();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
        Assert.assertEquals(records.get(2), expectedC);
        Assert.assertEquals(topK.getRecords(), records);
        Assert.assertEquals(topK.getMetadata().asMap(), result.getMeta().asMap());

        topK.reset();

        topK.consume(RecordBox.get().add("A", 0).getRecord());
        topK.consume(RecordBox.get().add("A", 0).getRecord());
        topK.consume(RecordBox.get().add("A", 1).getRecord());

        result = topK.getResult();
        records = result.getRecords();
        Assert.assertEquals(records.size(), 2);

        expectedA = RecordBox.get().add("A", "0").add("B", "null").add(TopK.DEFAULT_NEW_NAME, 2L).getRecord();
        expectedB = RecordBox.get().add("A", "1").add("B", "null").add(TopK.DEFAULT_NEW_NAME, 1L).getRecord();
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);

        Assert.assertEquals(topK.getRecords(), records);
        Assert.assertEquals(topK.getMetadata().asMap(), result.getMeta().asMap());
    }
}
