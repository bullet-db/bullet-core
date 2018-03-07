/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class MemoryPubSubTest {

    @Test
    public void testSettings() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        MemoryPubSub pubSub = new MemoryPubSub(config);

        // Test custom values from file
        MemoryQueryPublisher publisher = (MemoryQueryPublisher) pubSub.getPublisher();
        String queryUri = publisher.queryURI;
        Assert.assertEquals(queryUri, "http://localhost:9901/CUSTOM/query");

        // Test defaults
        MemorySubscriber resultSubscriber = (MemorySubscriber) pubSub.getSubscriber();
        List<String> uris = resultSubscriber.uris;
        Assert.assertEquals(uris.size(), 1);
        Assert.assertEquals(uris.get(0), "http://localhost:9901/bullet/api/pubsub/result");
    }

    @Test
    public void testGetPublisher() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");
        MemoryPubSub pubSub = new MemoryPubSub(config);
        Publisher publisher = pubSub.getPublisher();
        Assert.assertNotNull(publisher);
        Assert.assertTrue(publisher instanceof MemoryQueryPublisher);

        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_PROCESSING");
        pubSub = new MemoryPubSub(config);
        publisher = pubSub.getPublisher();
        Assert.assertNotNull(publisher);
        Assert.assertTrue(publisher instanceof MemoryResultPublisher);
    }

    @Test
    public void testGetPublishers() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");
        MemoryPubSub pubSub = new MemoryPubSub(config);
        List<Publisher> publishers = pubSub.getPublishers(8);
        Assert.assertNotNull(publishers);
        Assert.assertTrue(publishers.get(0) instanceof MemoryQueryPublisher);
        Assert.assertTrue(publishers.get(7) instanceof MemoryQueryPublisher);

        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_PROCESSING");
        pubSub = new MemoryPubSub(config);
        publishers = pubSub.getPublishers(8);
        Assert.assertNotNull(publishers);
        Assert.assertTrue(publishers.get(0) instanceof MemoryResultPublisher);
        Assert.assertTrue(publishers.get(7) instanceof MemoryResultPublisher);
    }

    @Test
    public void testGetSubscriber() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");
        MemoryPubSub pubSub = new MemoryPubSub(config);
        Subscriber subscriber = pubSub.getSubscriber();
        Assert.assertNotNull(subscriber);
        Assert.assertTrue(subscriber instanceof MemorySubscriber);

        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_PROCESSING");
        pubSub = new MemoryPubSub(config);
        subscriber = pubSub.getSubscriber();
        Assert.assertNotNull(subscriber);
        Assert.assertTrue(subscriber instanceof MemorySubscriber);
    }

    @Test
    public void testGetSubscribers() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");
        MemoryPubSub pubSub = new MemoryPubSub(config);
        List<Subscriber> subscribers = pubSub.getSubscribers(8);
        Assert.assertNotNull(subscribers);
        Assert.assertTrue(subscribers.get(0) instanceof MemorySubscriber);
        Assert.assertTrue(subscribers.get(7) instanceof MemorySubscriber);

        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_PROCESSING");
        pubSub = new MemoryPubSub(config);
        subscribers = pubSub.getSubscribers(8);
        Assert.assertNotNull(subscribers);
        Assert.assertTrue(subscribers.get(0) instanceof MemorySubscriber);
        Assert.assertTrue(subscribers.get(7) instanceof MemorySubscriber);
    }
}
