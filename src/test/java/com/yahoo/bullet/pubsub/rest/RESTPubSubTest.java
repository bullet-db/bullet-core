/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class RESTPubSubTest {

    @Test
    public void testSettings() throws PubSubException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        RESTPubSub pubSub = new RESTPubSub(config);

        // Test custom values from file
        RESTQueryPublisher publisher = (RESTQueryPublisher) pubSub.getPublisher();
        String queryUri = publisher.queryURI;
        Assert.assertEquals(queryUri, "http://localhost:9901/CUSTOM/query");

        // Test defaults
        RESTSubscriber resultSubscriber = (RESTSubscriber) pubSub.getSubscriber();
        List<String> uris = resultSubscriber.uris;
        Assert.assertEquals(uris.size(), 1);
        Assert.assertEquals(uris.get(0), "http://localhost:9901/bullet/api/pubsub/result");
    }

    public static AsyncHttpClient mockClientWith(BoundRequestBuilder builder) {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doReturn(builder).when(mockClient).preparePost(anyString());
        doReturn(builder).when(mockClient).prepareGet(anyString());
        return mockClient;
    }

    public static BoundRequestBuilder mockBuilderWith(CompletableFuture<Response> future) throws Exception {
        ListenableFuture<Response> mockListenable = (ListenableFuture<Response>) mock(ListenableFuture.class);
        doReturn(future.get()).when(mockListenable).get();
        doReturn(future).when(mockListenable).toCompletableFuture();

        BoundRequestBuilder mockBuilder = mock(BoundRequestBuilder.class);
        doReturn(mockBuilder).when(mockBuilder).setHeader(any(), anyString());
        doReturn(mockBuilder).when(mockBuilder).setBody(anyString());
        doReturn(mockListenable).when(mockBuilder).execute();
        return mockBuilder;
    }

    public static CompletableFuture<Response> getOkFuture(Response response) throws Exception {
        CompletableFuture<Response> finished = CompletableFuture.completedFuture(response);
        CompletableFuture<Response> mock = mock(CompletableFuture.class);
        // This is the weird bit. We mock the call to exceptionally to return the finished response so that chaining
        // a thenAcceptAsync on it will call the consumer of it with the finished response. This is why it looks
        // weird: mocking the exceptionally to take the "good" path.
        doReturn(finished).when(mock).exceptionally(any());
        doReturn(finished.get()).when(mock).get();
        // So if we do get to thenAccept on our mock, we should throw an exception because we shouldn't get there.
        doThrow(new RuntimeException("Good futures don't throw")).when(mock).thenAcceptAsync(any());
        return mock;
    }

    public static Response getOkResponse(String data) {
        return getResponse(RESTPubSub.OK_200, "Ok", data);
    }

    public static Response getNotOkResponse(int status) {
        return getResponse(status, "Error", null);
    }

    public static Response getResponse(int status, String statusText, String body) {
        Response mock = mock(Response.class);
        doReturn(status).when(mock).getStatusCode();
        doReturn(statusText).when(mock).getStatusText();
        doReturn(body).when(mock).getResponseBody();
        return mock;
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
