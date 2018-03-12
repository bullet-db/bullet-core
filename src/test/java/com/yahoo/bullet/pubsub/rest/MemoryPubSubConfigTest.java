/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.common.Config;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MemoryPubSubConfigTest {
    @Test
    public void testNoFiles() {
        MemoryPubSubConfig config = new MemoryPubSubConfig((String) null);
        Assert.assertEquals(config.get(MemoryPubSubConfig.CONNECT_RETRY_LIMIT), 10L);

        config = new MemoryPubSubConfig((Config) null);
        Assert.assertEquals(config.get(MemoryPubSubConfig.CONNECT_RETRY_LIMIT), 10L);

        config = new MemoryPubSubConfig("");
        Assert.assertEquals(config.get(MemoryPubSubConfig.CONNECT_RETRY_LIMIT), 10L);
    }

    @Test
    public void testMissingFile() {
        MemoryPubSubConfig config = new MemoryPubSubConfig("/path/to/non/existant/file");
        Assert.assertEquals(config.get(MemoryPubSubConfig.CONNECT_RETRY_LIMIT), 10L);
    }

    @Test
    public void testCustomConfig() {
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        Assert.assertEquals(config.get(MemoryPubSubConfig.CONNECT_RETRY_LIMIT), 88L);
        Assert.assertEquals(config.get(MemoryPubSubConfig.PUBSUB_CLASS_NAME), "com.yahoo.bullet.pubsub.MockPubSub");
        List<String> queries = ((List<String>) config.getAs(MemoryPubSubConfig.QUERY_URIS, List.class));
        Assert.assertEquals(queries.size(), 2);
        Assert.assertEquals(queries.get(0), "http://localhost:9901/CUSTOM/query");
        Assert.assertEquals(queries.get(1), "http://localhost:9902/CUSTOM/query");
    }

    @Test
    public void testCustomProperties() {
        MemoryPubSubConfig config = new MemoryPubSubConfig((String) null);
        Assert.assertNull(config.get("foo"));
        config.set("foo", "bar");
        Assert.assertEquals(config.get("foo"), "bar");
    }

    @Test
    public void testGettingWithDefault() {
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        Assert.assertEquals(config.getOrDefault(MemoryPubSubConfig.CONNECT_RETRY_LIMIT, "51"), 88L);
        Assert.assertEquals(config.getOrDefault("does.not.exist", "foo"), "foo");
        Assert.assertEquals(config.getOrDefault("fake.setting", "bar"), "bar");
    }

    @Test
    public void testGettingMultipleProperties() {
        MemoryPubSubConfig config = new MemoryPubSubConfig((String) null);
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
    public void testMerging() {
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");

        int configSize = config.getAll(Optional.empty()).size();
        Assert.assertEquals(config.get(MemoryPubSubConfig.CONNECT_RETRY_LIMIT), 88L);
        Assert.assertEquals(config.get(MemoryPubSubConfig.PUBSUB_CLASS_NAME), "com.yahoo.bullet.pubsub.MockPubSub");

        Config another = new MemoryPubSubConfig((String) null);
        another.clear();
        another.set(MemoryPubSubConfig.CONNECT_RETRY_LIMIT, 51L);
        // This is a bad setting
        another.set(MemoryPubSubConfig.AGGREGATION_MAX_SIZE, -1);
        // Some other non-Bullet setting
        config.set("pi", 3.14);

        config.merge(another);

        Assert.assertEquals(config.getAll(Optional.empty()).size(), configSize + 1);
        Assert.assertEquals(config.get(MemoryPubSubConfig.CONNECT_RETRY_LIMIT), 51L);
        // Bad setting gets defaulted.
        Assert.assertEquals(config.get(MemoryPubSubConfig.AGGREGATION_MAX_SIZE), MemoryPubSubConfig.DEFAULT_AGGREGATION_MAX_SIZE);
        // Other setting is preserved.
        Assert.assertEquals(config.get("pi"), 3.14);

        // Test null and verify it is unchanged
        config.merge(null);
        Assert.assertEquals(config.getAll(Optional.empty()).size(), configSize + 1);
        Assert.assertEquals(config.get(MemoryPubSubConfig.CONNECT_RETRY_LIMIT), 51L);
        Assert.assertEquals(config.get(MemoryPubSubConfig.AGGREGATION_MAX_SIZE), MemoryPubSubConfig.DEFAULT_AGGREGATION_MAX_SIZE);
        Assert.assertEquals(config.get("pi"), 3.14);
    }

    @Test
    public void testPropertiesWithPrefix() {
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        String prefix = "bullet.pubsub";
        String fieldValue = "com.yahoo.bullet.pubsub.MockPubSub";

        int configSize = config.getAllWithPrefix(Optional.empty(), prefix, false).size();
        Assert.assertEquals(configSize, 9);

        Map<String, Object> properties = config.getAllWithPrefix(Optional.empty(), prefix, false);
        Assert.assertEquals(properties.get(MemoryPubSubConfig.PUBSUB_CLASS_NAME), fieldValue);
    }

    @Test
    public void testPropertiesStripPrefix() {
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        String prefix = "bullet.pubsub.";
        String fieldName = "class.name";
        String fieldValue = "com.yahoo.bullet.pubsub.MockPubSub";

        int configSize = config.getAllWithPrefix(Optional.empty(), prefix, true).size();
        Assert.assertEquals(configSize, 9);

        Map<String, Object> properties = config.getAllWithPrefix(Optional.empty(), prefix, true);
        Assert.assertNull(properties.get(MemoryPubSubConfig.PUBSUB_CLASS_NAME));
        Assert.assertEquals(properties.get(fieldName), fieldValue);
    }

    @Test
    public void testGetAsAGivenType() {
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/custom_config.yaml");

        int defaulted = config.getAs(MemoryPubSubConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, Integer.class);
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
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");

        int notDefaulted = config.getOrDefaultAs(MemoryPubSubConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, 42, Integer.class);
        Assert.assertEquals(notDefaulted, 100);

        String defaulted = config.getOrDefaultAs("foo", "value", String.class);
        Assert.assertEquals(defaulted, "value");

        List anotherDefaulted = config.getOrDefaultAs("foo", Arrays.asList("foo", "bar"), List.class);
        Assert.assertEquals(anotherDefaulted, Arrays.asList("foo", "bar"));
    }

    @Test
    public void testValidatorIsACopy() {
        Assert.assertTrue(MemoryPubSubConfig.getValidator() != MemoryPubSubConfig.getValidator());
    }
}
