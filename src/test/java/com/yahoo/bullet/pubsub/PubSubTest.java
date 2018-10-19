/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.BulletConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

public class PubSubTest {
    @Test
    public void testMockPubSubCreation() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        String mockMessage = UUID.randomUUID().toString();
        config.set(MockPubSub.MOCK_MESSAGE_NAME, mockMessage);
        PubSub testPubSub = PubSub.from(config);

        Assert.assertEquals(testPubSub.getClass(), MockPubSub.class);
        Assert.assertEquals(testPubSub.getSubscriber().receive().getContent(), mockMessage);

        testPubSub.close();
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testIllegalPubSubParameter() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        try {
            PubSub.from(config);
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getClass(), InvocationTargetException.class);
            throw e;
        }
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testIllegalPubSubClassName() throws PubSubException {
        BulletConfig config = new BulletConfig(null);
        config.set(BulletConfig.PUBSUB_CLASS_NAME, null);
        try {
            PubSub.from(config);
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getClass(), NullPointerException.class);
            throw e;
        }
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testConstructorRuntimeExceptionThrowsPubSubException() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(MockPubSub.MOCK_MESSAGE_NAME, "");
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "");
        PubSub.from(config);
    }

    @Test
    public void testSwitchingContext() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(MockPubSub.MOCK_MESSAGE_NAME, "");
        PubSub pubSub = PubSub.from(config);

        Assert.assertEquals(pubSub.getClass(), MockPubSub.class);
        Assert.assertEquals(pubSub.getContext(), PubSub.Context.QUERY_SUBMISSION);
        Assert.assertTrue(pubSub.getSubscriber().receive().getContent().isEmpty());

        // No switch
        pubSub.switchContext(PubSub.Context.QUERY_SUBMISSION, new BulletConfig());
        Assert.assertEquals(pubSub.getContext(), PubSub.Context.QUERY_SUBMISSION);
        Assert.assertTrue(pubSub.getSubscriber().receive().getContent().isEmpty());

        // Switch
        BulletConfig newConfig = new BulletConfig("src/test/resources/test_config.yaml");
        newConfig.set(MockPubSub.MOCK_MESSAGE_NAME, "foo");
        pubSub.switchContext(PubSub.Context.QUERY_PROCESSING, newConfig);
        Assert.assertEquals(pubSub.getContext(), PubSub.Context.QUERY_PROCESSING);
        Assert.assertEquals(pubSub.getSubscriber().receive().getContent(), "foo");

        pubSub.close();
    }
}
