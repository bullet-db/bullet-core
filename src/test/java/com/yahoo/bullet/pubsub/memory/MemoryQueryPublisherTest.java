/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MemoryQueryPublisherTest {

    private AsyncHttpClient mockClientWith(BoundRequestBuilder builder) {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doReturn(builder).when(mockClient).preparePost(anyString());
        return mockClient;
    }

    private BoundRequestBuilder mockBuilderWith(CompletableFuture<Response> future) {
        ListenableFuture<Response> mockListenable = (ListenableFuture<Response>) mock(ListenableFuture.class);
        doReturn(future).when(mockListenable).toCompletableFuture();

        BoundRequestBuilder mockBuilder = mock(BoundRequestBuilder.class);
        doReturn(mockBuilder).when(mockBuilder).setHeader(any(), anyString());
        doReturn(mockBuilder).when(mockBuilder).setBody(anyString());
        doReturn(mockListenable).when(mockBuilder).execute();
        return mockBuilder;
    }

    private CompletableFuture<Response> getOkFuture(Response response) {
        CompletableFuture<Response> finished = CompletableFuture.completedFuture(response);
        CompletableFuture<Response> mock = mock(CompletableFuture.class);
        // This is the weird bit. We mock the call to exceptionally to return the finished response so that chaining
        // a thenAcceptAsync on it will call the consumer of it with the finished response. This is why it looks
        // weird: mocking the exceptionally to take the "good" path.
        doReturn(finished).when(mock).exceptionally(any());
        // So if we do get to thenAccept on our mock, we should throw an exception because we shouldn't get there.
        doThrow(new RuntimeException("Good futures don't throw")).when(mock).thenAcceptAsync(any());
        return mock;
    }

    private Response getOkResponse(String data) {
        return getResponse(MemoryPubSub.OK_200, "Ok", data);
    }

    private Response getResponse(int status, String statusText, String body) {
        Response mock = mock(Response.class);
        doReturn(status).when(mock).getStatusCode();
        doReturn(statusText).when(mock).getStatusText();
        doReturn(body).when(mock).getResponseBody();
        return mock;
    }

    @Test(timeOut = 5000L)
    public void testReadingOkResponse() throws Exception {
        //PubSubMessage expected = new PubSubMessage("foo", "response");
        CompletableFuture<Response> response = getOkFuture(getOkResponse(new PubSubMessage("foo", "bar").asJSON()));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        config.set(MemoryPubSubConfig.RESULT_URI, "my/custom/uri");
        MemoryQueryPublisher publisher = new MemoryQueryPublisher(config, mockClient);

        publisher.send(new PubSubMessage("foo", "bar"));
        verify(mockClient).preparePost("http://localhost:9901/CUSTOM/query");
        verify(mockBuilder).setBody("{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":null,\"content\":\"my/custom/uri\"}}");
        verify(mockBuilder).setHeader("content-type", "text/plain");


//        //MemorySubscriber subscriber = new MemorySubscriber(config, 88, Arrays.asList("baz"), mockClient);
//
//
//        // This is async (but practically still very fast since we mocked the response), so need a timeout.
//        PubSubMessage actual = fetchAsync().get();
//
//        Assert.assertNotNull(actual);
//        Assert.assertEquals(actual.getId(), expected.getId());
//        Assert.assertEquals(actual.getContent(), expected.getContent());
    }

//    @Test
//    public void testSend() throws PubSubException {
//        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
//        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
//        List<String> queryURIS = Arrays.asList("foo", "bar");
//        config.set(MemoryPubSubConfig.RESULT_URI, "baz");
//        config.set(MemoryPubSubConfig.QUERY_URIS, queryURIS);
//        MemoryQueryPublisher publisher = new MemoryQueryPublisher(config, mockClient);
//        publisher.send(new PubSubMessage("foo", "bar"));
//        verify(mockClient).preparePost()


//        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
//        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");
//        MemoryPubSub pubSub = new MemoryPubSub(config);
//        Publisher publisher = pubSub.getPublisher();
//        Assert.assertNotNull(publisher);
//        Assert.assertTrue(publisher instanceof MemoryQueryPublisher);
//
//        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_PROCESSING");
//        pubSub = new MemoryPubSub(config);
//        publisher = pubSub.getPublisher();
//        Assert.assertNotNull(publisher);
//        Assert.assertTrue(publisher instanceof MemoryResultPublisher);
//    }
    // queryURI is pulled from the config correctly and the client is setup with it correctly
    // test resultURI is pulled from the config correctly and put into the metadata correctly
}
