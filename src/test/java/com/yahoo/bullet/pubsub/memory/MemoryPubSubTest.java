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
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.testng.Assert;
import org.testng.annotations.Test;
import javax.ws.rs.core.Response.Status;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

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
        return getResponse(Status.OK.getStatusCode(), "Ok", data);
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

    @Test
    public void testToString() throws Exception {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        MemoryPubSub pubSub = new MemoryPubSub(config);
        Assert.assertEquals(pubSub.toString(), "context: QUERY_SUBMISSION\n" +
                "config: {bullet.query.aggregation.group.sketch.resize.factor=8, fake.setting=null, " +
                "bullet.query.aggregation.count.distinct.sketch.resize.factor=8, " +
                "bullet.pubsub.memory.query.uris=[http://localhost:9901/CUSTOM/query, http://localhost:9902/CUSTOM/query], " +
                "bullet.query.window.min.emit.every=1000, bullet.pubsub.memory.connect.timeout.ms=30000, " +
                "bullet.pubsub.memory.connect.retry.limit=88, bullet.pubsub.class.name=com.yahoo.bullet.pubsub.MockPubSub, " +
                "bullet.query.default.duration=1000, bullet.query.rate.limit.time.interval=100, " +
                "bullet.query.aggregation.group.sketch.entries=512, bullet.query.aggregation.raw.max.size=100, " +
                "bullet.query.rate.limit.enable=true, bullet.query.aggregation.top.k.sketch.error.type=NFN, " +
                "bullet.result.metadata.enable=true, bullet.record.inject.timestamp.enable=false, " +
                "bullet.query.aggregation.count.distinct.sketch.entries=16384, bullet.query.window.disable=false, " +
                "bullet.query.aggregation.count.distinct.sketch.family=Alpha, bullet.query.aggregation.default.size=500, " +
                "bullet.query.aggregation.group.sketch.sampling=1.0, bullet.query.aggregation.distribution.max.points=100, " +
                "bullet.query.aggregation.group.max.size=500, bullet.pubsub.memory.result.uri=http://localhost:9901/bullet/api/pubsub/result, " +
                "bullet.query.aggregation.distribution.generated.points.rounding=6, bullet.query.aggregation.composite.field.separator=|, " +
                "bullet.query.aggregation.distribution.sketch.entries=1024, bullet.pubsub.context.name=QUERY_SUBMISSION, " +
                "bullet.query.aggregation.top.k.sketch.entries=1024, bullet.pubsub.memory.subscriber.max.uncommitted.messages=100, " +
                "bullet.query.max.duration=10000, bullet.result.metadata.metrics={Window Expected Emit Time=Expected Emit Time, " +
                    "Sketch Maximum Value=Maximum Value, Window Metadata=Window, Query Body=Body, Sketch Metadata=Sketch, " +
                    "Window Emit Time=Emit Time, Query Receive Time=Receive Time, Sketch Family=Family, " +
                    "Sketch Uniques Estimate=Uniques Estimate, Sketch Items Seen=Items Seen, Query Metadata=Query, " +
                    "Window Number=Number, Sketch Minimum Value=Minimum Value, Window Size=Size, Query Finish Time=Finish Time, " +
                    "Sketch Theta=Theta, Window Name=Name, Query ID=ID, Sketch Maximum Count Error=Maximum Count Error, " +
                    "Sketch Standard Deviations=Standard Deviations, Sketch Normalized Rank Error=Normalized Rank Error, " +
                    "Sketch Estimated Result=Was Estimated, Sketch Size=Size, Sketch Active Items=Active Items}, " +
                "bullet.query.rate.limit.max.emit.count=100, bullet.record.inject.timestamp.key=bullet_project_timestamp, " +
                "bullet.query.aggregation.max.size=500, bullet.query.aggregation.count.distinct.sketch.sampling=1.0}");
    }
}
