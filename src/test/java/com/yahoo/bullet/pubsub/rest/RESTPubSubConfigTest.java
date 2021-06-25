/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RESTPubSubConfigTest {
    @Test
    public void testNoFiles() {
        RESTPubSubConfig config = new RESTPubSubConfig((String) null);
        Assert.assertEquals(config.get(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT), 5000);

        config = new RESTPubSubConfig((Config) null);
        Assert.assertEquals(config.get(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT), 5000);

        config = new RESTPubSubConfig("");
        Assert.assertEquals(config.get(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT), 5000);
    }

    @Test
    public void testMissingFile() {
        RESTPubSubConfig config = new RESTPubSubConfig("/path/to/non/existant/file");
        Assert.assertEquals(config.get(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT), 5000);
    }

    @Test
    public void testCustomConfig() {
        RESTPubSubConfig config = new RESTPubSubConfig("src/test/resources/test_config.yaml");
        Assert.assertEquals(config.get(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT), 88);
        Assert.assertEquals(config.get(RESTPubSubConfig.PUBSUB_CLASS_NAME), "com.yahoo.bullet.pubsub.MockPubSub");
        List<String> queries = ((List<String>) config.getAs(RESTPubSubConfig.QUERY_URLS, List.class));
        Assert.assertEquals(queries.size(), 2);
        Assert.assertEquals(queries.get(0), "http://localhost:9901/CUSTOM/query");
        Assert.assertEquals(queries.get(1), "http://localhost:9902/CUSTOM/query");
    }

    @Test
    public void testCustomProperties() {
        RESTPubSubConfig config = new RESTPubSubConfig((String) null);
        Assert.assertNull(config.get("foo"));
        config.set("foo", "bar");
        Assert.assertEquals(config.get("foo"), "bar");
    }

    @Test
    public void testGettingWithDefault() {
        RESTPubSubConfig config = new RESTPubSubConfig("src/test/resources/test_config.yaml");
        Assert.assertEquals(config.getOrDefault(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT, "51"), 88);
        Assert.assertEquals(config.getOrDefault("does.not.exist", "foo"), "foo");
        Assert.assertEquals(config.getOrDefault("fake.setting", "bar"), "bar");
    }

    @Test
    public void testGettingMultipleProperties() {
        RESTPubSubConfig config = new RESTPubSubConfig((String) null);
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
        RESTPubSubConfig config = new RESTPubSubConfig("src/test/resources/test_config.yaml");

        int configSize = config.getAll(Optional.empty()).size();
        Assert.assertEquals(config.get(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT), 88);
        Assert.assertEquals(config.get(RESTPubSubConfig.PUBSUB_CLASS_NAME), "com.yahoo.bullet.pubsub.MockPubSub");

        Config another = new RESTPubSubConfig((String) null);
        another.clear();
        another.set(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT, 51L);
        // This is a bad setting
        another.set(RESTPubSubConfig.AGGREGATION_MAX_SIZE, -1);
        // Some other non-Bullet setting
        config.set("pi", 3.14);

        config.merge(another);

        Assert.assertEquals(config.getAll(Optional.empty()).size(), configSize + 1);
        Assert.assertEquals(config.get(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT), 51);
        // Bad setting gets defaulted.
        Assert.assertEquals(config.get(RESTPubSubConfig.AGGREGATION_MAX_SIZE), RESTPubSubConfig.DEFAULT_AGGREGATION_MAX_SIZE);
        // Other setting is preserved.
        Assert.assertEquals(config.get("pi"), 3.14);

        // Test null and verify it is unchanged
        config.merge(null);
        Assert.assertEquals(config.getAll(Optional.empty()).size(), configSize + 1);
        Assert.assertEquals(config.get(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT), 51);
        Assert.assertEquals(config.get(RESTPubSubConfig.AGGREGATION_MAX_SIZE), RESTPubSubConfig.DEFAULT_AGGREGATION_MAX_SIZE);
        Assert.assertEquals(config.get("pi"), 3.14);
    }

    @Test
    public void testPropertiesWithPrefix() {
        RESTPubSubConfig config = new RESTPubSubConfig("src/test/resources/test_config.yaml");
        String prefix = "bullet.pubsub";
        String pubSubClassValue = "com.yahoo.bullet.pubsub.MockPubSub";
        String pubSubMessageSerDeClassValue = "com.yahoo.bullet.pubsub.ByteArrayPubSubMessageSerDe";

        int configSize = config.getAllWithPrefix(Optional.empty(), prefix, false).size();
        Assert.assertEquals(configSize, 10);

        Map<String, Object> properties = config.getAllWithPrefix(Optional.empty(), prefix, false);
        Assert.assertEquals(properties.get(BulletConfig.PUBSUB_CLASS_NAME), pubSubClassValue);
        Assert.assertEquals(properties.get(BulletConfig.PUBSUB_MESSAGE_SERDE_CLASS_NAME), pubSubMessageSerDeClassValue);
    }

    @Test
    public void testPropertiesStripPrefix() {
        RESTPubSubConfig config = new RESTPubSubConfig("src/test/resources/test_config.yaml");
        String prefix = "bullet.pubsub.";
        String pubsubClassKey = "class.name";
        String pubSubClassValue = "com.yahoo.bullet.pubsub.MockPubSub";
        String pubsubMessageSerDeClassKey = "message.serde.class.name";
        String pubSubMessageSerDeClassValue = "com.yahoo.bullet.pubsub.ByteArrayPubSubMessageSerDe";

        int configSize = config.getAllWithPrefix(Optional.empty(), prefix, true).size();
        Assert.assertEquals(configSize, 10);

        Map<String, Object> properties = config.getAllWithPrefix(Optional.empty(), prefix, true);
        Assert.assertNull(properties.get(RESTPubSubConfig.PUBSUB_CLASS_NAME));
        Assert.assertEquals(properties.get(pubsubClassKey), pubSubClassValue);
        Assert.assertEquals(properties.get(pubsubMessageSerDeClassKey), pubSubMessageSerDeClassValue);
    }

    @Test
    public void testGetAsAGivenType() {
        RESTPubSubConfig config = new RESTPubSubConfig("src/test/resources/custom_config.yaml");

        int defaulted = config.getAs(RESTPubSubConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, Integer.class);
        Assert.assertEquals(defaulted, 100);

        Map customMap = config.getAs("my.custom.map", Map.class);
        Assert.assertNotNull(customMap);
        Assert.assertEquals(customMap.size(), 2);
        Assert.assertEquals(((Number) customMap.get("first")).longValue(), 10L);
        Assert.assertEquals(((Number) customMap.get("second")).longValue(), 42L);

        List customList = config.getAs("my.custom.list", List.class);
        Assert.assertNotNull(customList);
        Assert.assertEquals(customList.size(), 2);
        Assert.assertEquals(customList.get(0), "foo");
        Assert.assertEquals(customList.get(1), "bar");
    }

    @Test
    public void testGetOrDefaultAsAGivenType() {
        RESTPubSubConfig config = new RESTPubSubConfig("src/test/resources/test_config.yaml");

        int notDefaulted = config.getOrDefaultAs(RESTPubSubConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, 42, Integer.class);
        Assert.assertEquals(notDefaulted, 100);

        String defaulted = config.getOrDefaultAs("foo", "value", String.class);
        Assert.assertEquals(defaulted, "value");

        List anotherDefaulted = config.getOrDefaultAs("foo", Arrays.asList("foo", "bar"), List.class);
        Assert.assertEquals(anotherDefaulted, Arrays.asList("foo", "bar"));
    }

    @Test
    public void testValidatorIsACopy() {
        Assert.assertTrue(RESTPubSubConfig.getValidator() != RESTPubSubConfig.getValidator());
    }

    @Test
    public void testValidate() {
        RESTPubSubConfig config = new RESTPubSubConfig("src/test/resources/test_config.yaml");

        // Test validate() corrects BulletConfig settings
        config.set(BulletConfig.AGGREGATION_DEFAULT_SIZE, -88);
        Assert.assertEquals(config.get(BulletConfig.AGGREGATION_DEFAULT_SIZE), -88);
        config.validate();
        Assert.assertEquals(config.get(BulletConfig.AGGREGATION_DEFAULT_SIZE), BulletConfig.DEFAULT_AGGREGATION_SIZE);

        // Test validate() corrects RESTPubSubConfig settings
        config.set(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT, -88);
        Assert.assertEquals(config.get(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT), -88);
        config.validate();
        Assert.assertEquals(config.get(RESTPubSubConfig.SUBSCRIBER_CONNECT_TIMEOUT), RESTPubSubConfig.DEFAULT_SUBSCRIBER_CONNECT_TIMEOUT);
    }
}
