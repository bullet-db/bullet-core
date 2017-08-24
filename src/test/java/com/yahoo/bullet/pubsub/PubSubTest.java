package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.BulletConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

public class PubSubTest {
    @Test
    public void testMockPubSubCreation() throws Exception {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        String mockMessage = UUID.randomUUID().toString();
        config.set(MockPubSub.MOCK_MESSAGE_NAME, mockMessage);
        PubSub testPubSub = PubSub.from(config);

        Assert.assertEquals(testPubSub.getClass(), MockPubSub.class);
        Assert.assertTrue(testPubSub.getSubscriber().receive().getContent().equals(mockMessage));
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testIllegalPubSubParameter() throws Exception {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        try {
            PubSub.from(config);
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getClass(), InvocationTargetException.class);
            throw e;
        }
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testIllegalPubSubClassName() throws Exception {
        BulletConfig config = new BulletConfig(null);
        try {
            PubSub.from(config);
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getClass(), NullPointerException.class);
            throw e;
        }
    }
}
