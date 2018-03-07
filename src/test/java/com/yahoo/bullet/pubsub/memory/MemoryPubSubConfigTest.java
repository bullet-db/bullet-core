/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.common.Config;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

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

}
