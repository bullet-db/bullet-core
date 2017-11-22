/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet;

import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.Metadata.Concept;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;

public class BulletConfigTest {
    private static Map<String, String> allMetadataAsMap() {
        Map<String, String> meta = new HashMap<>();
        for (Map<String, String> m : BulletConfig.DEFAULT_RESULT_METADATA_METRICS) {
            meta.put(m.get(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY), m.get(BulletConfig.RESULT_METADATA_METRICS_NAME_KEY));
        }
        return meta;
    }

    private static class CustomConfig extends BulletConfig {
        private static final Validator CUSTOM = new Validator();
        static {
            CUSTOM.define("foo").defaultTo(42).checkIf(Validator::isPositiveInt);
            CUSTOM.define("bar").defaultTo(0.4).checkIf(Validator::isPositive);
            CUSTOM.relate("foo > bar", "foo", "bar").checkIf(Validator::isGreaterOrEqual);
        }

        @Override
        public BulletConfig validate() {
            CUSTOM.normalize(this);
            return super.validate();
        }
    }

    @Test
    public void testNoFiles() {
        BulletConfig config = new BulletConfig();
        Assert.assertEquals(config.get(BulletConfig.SPECIFICATION_MAX_DURATION), 120000L);

        config = new BulletConfig(null);
        Assert.assertEquals(config.get(BulletConfig.SPECIFICATION_MAX_DURATION), 120000L);

        config = new BulletConfig("");
        Assert.assertEquals(config.get(BulletConfig.SPECIFICATION_MAX_DURATION), 120000L);
    }

    @Test
    public void testMissingFile() {
        BulletConfig config = new BulletConfig("/path/to/non/existant/file");
        Assert.assertEquals(config.get(BulletConfig.SPECIFICATION_MAX_DURATION), 120000L);
    }

    @Test
    public void testCustomConfig() {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        Assert.assertEquals(config.get(BulletConfig.SPECIFICATION_MAX_DURATION), 10000L);
        Assert.assertEquals(config.get(BulletConfig.AGGREGATION_MAX_SIZE), 100L);
        Assert.assertEquals(config.get(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES), 16384L);
    }

    @Test
    public void testCustomProperties() {
        BulletConfig config = new BulletConfig(null);
        Assert.assertNull(config.get("foo"));
        config.set("foo", "bar");
        Assert.assertEquals(config.get("foo"), "bar");
    }

    @Test
    public void testGettingWithDefault() {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        Assert.assertEquals(config.getOrDefault(BulletConfig.AGGREGATION_COMPOSITE_FIELD_SEPARATOR, ";"), "|");
        Assert.assertEquals(config.getOrDefault("does.not.exist", "foo"), "foo");
        Assert.assertEquals(config.getOrDefault("fake.setting", "bar"), "bar");
    }

    @Test
    public void testGettingMultipleProperties() {
        BulletConfig config = new BulletConfig(null);
        config.clear();
        config.set("1", 1);
        config.set("pi", 3.14);
        config.set("foo", "bar");
        config.set("true", true);

        Optional<Set<String>> keys = Optional.of(new HashSet<>(Arrays.asList("1", "true")));
        Map<String, Object> mappings = config.getAll(keys);
        Assert.assertEquals(mappings.size(), 2);
        Assert.assertEquals(mappings.get("1"), 1);
        Assert.assertEquals(mappings.get("true"), true);

        mappings = config.getAll(Optional.empty());
        Assert.assertEquals(mappings.size(), 4);
        Assert.assertEquals(mappings.get("1"), 1);
        Assert.assertEquals(mappings.get("pi"), 3.14);
        Assert.assertEquals(mappings.get("foo"), "bar");
        Assert.assertEquals(mappings.get("true"), true);
    }

    @Test
    public void testGettingMaskedProperties() {
        BulletConfig config = new BulletConfig(null);
        config.clear();
        config.set("1", 1);
        config.set("pi", 3.14);
        config.set("foo", "bar");
        config.set("true", true);

        Optional<Set<String>> keys = Optional.of(new HashSet<>(Arrays.asList("1", "true")));
        Map<String, Object> mappings = config.getAllBut(keys);
        Assert.assertEquals(mappings.size(), 2);
        Assert.assertEquals(mappings.get("foo"), "bar");
        Assert.assertEquals(mappings.get("pi"), 3.14);

        mappings = config.getAllBut(Optional.empty());
        Assert.assertEquals(mappings.size(), 4);
        Assert.assertEquals(mappings.get("1"), 1);
        Assert.assertEquals(mappings.get("pi"), 3.14);
        Assert.assertEquals(mappings.get("foo"), "bar");
        Assert.assertEquals(mappings.get("true"), true);
    }

    @Test
    public void testMerging() {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");

        int configSize = config.getAll(Optional.empty()).size();
        Assert.assertEquals(config.get(BulletConfig.SPECIFICATION_MAX_DURATION), 10000L);
        Assert.assertEquals(config.get(BulletConfig.AGGREGATION_MAX_SIZE), 100L);

        Config another = new BulletConfig(null);
        another.clear();
        another.set(BulletConfig.SPECIFICATION_MAX_DURATION, 42L);
        config.set("pi", 3.14);

        config.merge(another);

        // Test null
        config.merge(null);

        Assert.assertEquals(config.getAll(Optional.empty()).size(), configSize + 1);
        Assert.assertEquals(config.get(BulletConfig.SPECIFICATION_MAX_DURATION), 42L);
        Assert.assertEquals(config.get(BulletConfig.AGGREGATION_MAX_SIZE), 100L);
        Assert.assertEquals(config.get("pi"), 3.14);
    }

    @Test
    public void testPropertiesWithPrefix() {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        String prefix = "bullet.pubsub";
        String fieldValue = "com.yahoo.bullet.pubsub.MockPubSub";

        int configSize = config.getAllWithPrefix(Optional.empty(), prefix, false).size();
        Assert.assertEquals(configSize, 2);

        Map<String, Object> properties = config.getAllWithPrefix(Optional.empty(), prefix, false);
        Assert.assertEquals(properties.get(BulletConfig.PUBSUB_CLASS_NAME), fieldValue);
    }

    @Test
    public void testPropertiesStripPrefix() {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        String prefix = "bullet.pubsub.";
        String fieldName = "class.name";
        String fieldValue = "com.yahoo.bullet.pubsub.MockPubSub";

        int configSize = config.getAllWithPrefix(Optional.empty(), prefix, true).size();
        Assert.assertEquals(configSize, 2);

        Map<String, Object> properties = config.getAllWithPrefix(Optional.empty(), prefix, true);
        Assert.assertNull(properties.get(BulletConfig.PUBSUB_CLASS_NAME));
        Assert.assertEquals(properties.get(fieldName), fieldValue);
    }

    @Test
    public void testGetAsAGivenType() {
        BulletConfig config = new BulletConfig("src/test/resources/custom_config.yaml");

        long defaulted = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, Long.class);
        Assert.assertEquals(defaulted, 100);

        Map customMap = config.getAs("my.custom.map", Map.class);
        Assert.assertNotNull(customMap);
        Assert.assertEquals(customMap.size(), 2);
        Assert.assertEquals(customMap.get("first"), 10L);
        Assert.assertEquals(customMap.get("second"), 42L);

        List customList = config.getAs("my.custom.list", List.class);
        Assert.assertNotNull(customList);
        Assert.assertEquals(customList.size(), 2);
        Assert.assertEquals(customList.get(0), "foo");
        Assert.assertEquals(customList.get(1), "bar");
    }

    @Test
    public void testGetOrDefaultAsAGivenType() {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");


        long notDefaulted = config.getOrDefaultAs(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, 42L, Long.class);
        Assert.assertEquals(notDefaulted, 100);

        String defaulted = config.getOrDefaultAs("foo", "value", String.class);
        Assert.assertEquals(defaulted, "value");

        List anotherDefaulted = config.getOrDefaultAs("foo", Arrays.asList("foo", "bar"), List.class);
        Assert.assertEquals(anotherDefaulted, Arrays.asList("foo", "bar"));
    }

    @Test
    public void testGettingRequiredConfig() {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");


        long present = config.getRequiredConfigAs(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, Long.class);
        Assert.assertEquals(present, 100);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = ".*was missing.*")
    public void testMissingRequiredConfig() {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");

        config.getRequiredConfigAs("does.not.exist", Long.class);
    }

    @Test
    public void testMetadataConversion() {
        List<Map<String, String>> metadata = new ArrayList<>();
        Map<String, String> expected = new HashMap<>();
        for (Concept concept : Metadata.KNOWN_CONCEPTS) {
            Map<String, String> entry = new HashMap<>();
            String name = concept.getName();
            String key = concept.getName().substring(0, 3);
            entry.put(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, name);
            entry.put(BulletConfig.RESULT_METADATA_METRICS_NAME_KEY, key);
            metadata.add(entry);
            expected.put(name, key);
        }

        BulletConfig config = new BulletConfig().validate();
        Assert.assertEquals(config.get(BulletConfig.RESULT_METADATA_METRICS), allMetadataAsMap());

        config.set(BulletConfig.RESULT_METADATA_METRICS, metadata);
        config.validate();
        Assert.assertEquals(config.get(BulletConfig.RESULT_METADATA_METRICS), expected);
    }

    @Test
    public void testUnknownMetadata() {
        List<Map<String, String>> metadata = new ArrayList<>();
        Map<String, String> expected = new HashMap<>();
        for (Concept concept : Arrays.asList(Concept.QUERY_ID, Concept.ITEMS_SEEN)) {
            Map<String, String> entry = new HashMap<>();
            String name = concept.getName();
            String key = concept.getName().substring(0, 3);
            entry.put(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, name);
            entry.put(BulletConfig.RESULT_METADATA_METRICS_NAME_KEY, key);
            metadata.add(entry);
            expected.put(name, key);
        }

        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RESULT_METADATA_METRICS, metadata);
        config.validate();
        Assert.assertEquals(config.get(BulletConfig.RESULT_METADATA_METRICS), expected);

        // Add an unknown one
        Map<String, String> entry = new HashMap<>();
        entry.put("foo", "bar");
        entry.put("baz", "qux");
        metadata.add(entry);

        config.set(BulletConfig.RESULT_METADATA_METRICS, metadata);
        config.validate();
        // Now it's all defaults
        Assert.assertEquals(config.get(BulletConfig.RESULT_METADATA_METRICS), allMetadataAsMap());
    }

    @Test
    public void testBadMetadata() {
        List<Map<String, Object>> metadata = new ArrayList<>();
        Map<String, String> expected = new HashMap<>();
        for (Concept concept : Arrays.asList(Concept.QUERY_ID, Concept.ITEMS_SEEN)) {
            Map<String, Object> entry = new HashMap<>();
            String name = concept.getName();
            String key = concept.getName().substring(0, 3);
            entry.put(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, name);
            entry.put(BulletConfig.RESULT_METADATA_METRICS_NAME_KEY, key);
            metadata.add(entry);
            expected.put(name, key);
        }

        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RESULT_METADATA_METRICS, metadata);
        config.validate();
        Assert.assertEquals(config.get(BulletConfig.RESULT_METADATA_METRICS), expected);

        // Add a badly typed one
        Map<String, Object> entry = new HashMap<>();
        entry.put(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, new ArrayList<>());
        entry.put(BulletConfig.RESULT_METADATA_METRICS_NAME_KEY, new ArrayList<>());
        metadata.add(entry);

        config.set(BulletConfig.RESULT_METADATA_METRICS, metadata);
        config.validate();
        // Now it's all defaults
        Assert.assertEquals(config.get(BulletConfig.RESULT_METADATA_METRICS), allMetadataAsMap());
    }

    @Test
    public void testIncompleteMetadata() {
        List<Map<String, String>> metadata = new ArrayList<>();
        Map<String, String> expected = new HashMap<>();
        for (Concept concept : Arrays.asList(Concept.QUERY_ID, Concept.ITEMS_SEEN)) {
            Map<String, String> entry = new HashMap<>();
            String name = concept.getName();
            String key = concept.getName().substring(0, 3);
            entry.put(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, name);
            entry.put(BulletConfig.RESULT_METADATA_METRICS_NAME_KEY, key);
            metadata.add(entry);
            expected.put(name, key);
        }

        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RESULT_METADATA_METRICS, metadata);
        config.validate();
        Assert.assertEquals(config.get(BulletConfig.RESULT_METADATA_METRICS), expected);

        // Add only one entry
        Map<String, String> entry = new HashMap<>();
        entry.put(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, Concept.QUERY_ID.getName());
        metadata.add(entry);

        config.set(BulletConfig.RESULT_METADATA_METRICS, metadata);
        config.validate();
        // Now it's all defaults
        Assert.assertEquals(config.get(BulletConfig.RESULT_METADATA_METRICS), allMetadataAsMap());
    }

    @Test
    public void testStringification() {
        BulletConfig config = new BulletConfig();
        config.clear();
        String key = BulletConfig.AGGREGATION_DEFAULT_SIZE;
        config.set(key, 20);
        assertJSONEquals(config.toString(), "{'" + key + "': 20 }");
    }

    @Test
    public void testCustomConfigValidation() {
        CustomConfig config = new CustomConfig();
        Assert.assertNull(config.get("foo"));
        Assert.assertNull(config.get("bar"));
        Assert.assertEquals(config.get(BulletConfig.AGGREGATION_DEFAULT_SIZE), (long) BulletConfig.DEFAULT_AGGREGATION_SIZE);

        config.set("foo", 42);
        config.set("bar", 10.1);
        config.validate();
        Assert.assertEquals(config.get("foo"), 42);
        Assert.assertEquals(config.get("bar"), 10.1);

        config.set("foo", 4.2);
        config.set("bar", 12);
        config.validate();
        // Entry defaults before relationship
        Assert.assertEquals(config.get("foo"), 42);
        Assert.assertEquals(config.get("bar"), 12);

        config.set("foo", 13);
        config.set("bar", 16);
        config.validate();
        // Relationship defaults both
        Assert.assertEquals(config.get("foo"), 42);
        Assert.assertEquals(config.get("bar"), 0.4);
    }
}
