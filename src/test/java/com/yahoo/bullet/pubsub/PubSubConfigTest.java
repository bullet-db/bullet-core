package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.BulletConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class PubSubConfigTest {
    public static final String CONTEXT_PROCESSING = PubSub.Context.QUERY_PROCESSING.toString();
    public static final String  RECORD_INJECT_TIMESTAMP_KEY_VALUE = "bullet_project_timestamp";

    @Test
    public void testCustomFileCreate() throws IOException {
        PubSubConfig config = new PubSubConfig("src/test/resources/test_config.yaml");
        //Test custom properties
        Assert.assertEquals((long) config.get(BulletConfig.AGGREGATION_MAX_SIZE), 100);
        Assert.assertEquals((long) config.get(BulletConfig.SPECIFICATION_MAX_DURATION), 10000);
        Assert.assertTrue(config.get(PubSubConfig.CONTEXT_NAME).toString().equals(CONTEXT_PROCESSING));
        //Test default bullet properties
        Assert.assertTrue(config.get(BulletConfig.RECORD_INJECT_TIMESTAMP_KEY).toString().equals(RECORD_INJECT_TIMESTAMP_KEY_VALUE));
    }
}
