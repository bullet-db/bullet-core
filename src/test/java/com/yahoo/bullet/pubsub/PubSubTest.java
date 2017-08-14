package com.yahoo.bullet.pubsub;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

public class PubSubTest {
    @Test
    public void testMockPubSubCreation() throws IOException, PubSubException {
        PubSubConfig config = new PubSubConfig("src/test/resources/test_config.yaml");
        String mockMessage = UUID.randomUUID().toString();
        config.set(MockPubSub.MOCK_MESSAGE_NAME, mockMessage);
        PubSub testPubSub = PubSub.from(config);

        Assert.assertEquals(testPubSub.getClass(), MockPubSub.class);
        Assert.assertTrue(testPubSub.getSubscriber().receive().getContent().equals(mockMessage));
    }
}
