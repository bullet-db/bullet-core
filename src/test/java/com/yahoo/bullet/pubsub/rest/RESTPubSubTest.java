/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;

public class RESTPubSubTest {
    @Test
    public void testSettings() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        RESTPubSub pubSub = new RESTPubSub(config);

        // Test custom values from file
        RESTQueryPublisher publisher = (RESTQueryPublisher) pubSub.getPublisher();
        String queryURL = publisher.getQueryURL();
        Assert.assertEquals(queryURL, "http://localhost:9901/CUSTOM/query");

        // Test defaults
        RESTSubscriber resultSubscriber = (RESTSubscriber) pubSub.getSubscriber();
        List<String> urls = resultSubscriber.getUrls();
        Assert.assertEquals(urls.size(), 1);
        Assert.assertEquals(urls.get(0), "http://localhost:9901/api/bullet/pubsub/result");
    }

    @Test
    public void testGetPublisher() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");
        RESTPubSub pubSub = new RESTPubSub(config);
        Publisher publisher = pubSub.getPublisher();
        Assert.assertNotNull(publisher);
        Assert.assertTrue(publisher instanceof RESTQueryPublisher);

        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_PROCESSING");
        pubSub = new RESTPubSub(config);
        publisher = pubSub.getPublisher();
        Assert.assertNotNull(publisher);
        Assert.assertTrue(publisher instanceof RESTResultPublisher);
    }

    @Test
    public void testGetPublishers() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");
        RESTPubSub pubSub = new RESTPubSub(config);
        List<Publisher> publishers = pubSub.getPublishers(8);
        Assert.assertNotNull(publishers);
        Assert.assertTrue(publishers.get(0) instanceof RESTQueryPublisher);
        Assert.assertTrue(publishers.get(7) instanceof RESTQueryPublisher);

        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_PROCESSING");
        pubSub = new RESTPubSub(config);
        publishers = pubSub.getPublishers(8);
        Assert.assertNotNull(publishers);
        Assert.assertTrue(publishers.get(0) instanceof RESTResultPublisher);
        Assert.assertTrue(publishers.get(7) instanceof RESTResultPublisher);
    }

    @Test
    public void testGetSubscriber() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");
        RESTPubSub pubSub = new RESTPubSub(config);
        Subscriber subscriber = pubSub.getSubscriber();
        Assert.assertNotNull(subscriber);
        Assert.assertTrue(subscriber instanceof RESTSubscriber);

        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_PROCESSING");
        pubSub = new RESTPubSub(config);
        subscriber = pubSub.getSubscriber();
        Assert.assertNotNull(subscriber);
        Assert.assertTrue(subscriber instanceof RESTSubscriber);
    }

    @Test
    public void testGetSubscribers() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");
        RESTPubSub pubSub = new RESTPubSub(config);
        List<Subscriber> subscribers = pubSub.getSubscribers(8);
        Assert.assertNotNull(subscribers);
        Assert.assertTrue(subscribers.get(0) instanceof RESTSubscriber);
        Assert.assertTrue(subscribers.get(7) instanceof RESTSubscriber);

        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_PROCESSING");
        pubSub = new RESTPubSub(config);
        subscribers = pubSub.getSubscribers(8);
        Assert.assertNotNull(subscribers);
        Assert.assertTrue(subscribers.get(0) instanceof RESTSubscriber);
        Assert.assertTrue(subscribers.get(7) instanceof RESTSubscriber);
    }
}
