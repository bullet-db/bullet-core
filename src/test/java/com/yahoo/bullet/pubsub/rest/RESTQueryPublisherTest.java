/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static com.yahoo.bullet.pubsub.rest.RESTPubSubTest.getOkFuture;
import static com.yahoo.bullet.pubsub.rest.RESTPubSubTest.getOkResponse;
import static com.yahoo.bullet.pubsub.rest.RESTPubSubTest.mockBuilderWith;
import static com.yahoo.bullet.pubsub.rest.RESTPubSubTest.mockClientWith;
import static com.yahoo.bullet.pubsub.rest.RESTPubSubTest.getNotOkResponse;

public class RESTQueryPublisherTest {
    @Test
    public void testSendResultUrlPutInMetadataAckPreserved() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getOkResponse(null));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/url");

        publisher.send(new PubSubMessage("foo", "bar", Metadata.Signal.ACKNOWLEDGE));
        verify(mockClient).preparePost("my/custom/query/url");
        verify(mockBuilder).setBody("{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":\"ACKNOWLEDGE\",\"content\":\"my/custom/url\"}}");
        verify(mockBuilder).setHeader("content-type", "application/json");
    }

    @Test
    public void testSendResultUrlPutInMetadataCompletePreserved() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getOkResponse(null));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        RESTPubSubConfig config = new RESTPubSubConfig("src/test/resources/test_config.yaml");
        config.set(RESTPubSubConfig.RESULT_URL, "my/custom/url");
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/result/url");

        publisher.send(new PubSubMessage("foo", "bar", Metadata.Signal.COMPLETE));
        verify(mockClient).preparePost("my/custom/query/url");
        verify(mockBuilder).setBody("{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":\"COMPLETE\",\"content\":\"my/custom/result/url\"}}");
        verify(mockBuilder).setHeader("content-type", "application/json");
    }

    @Test
    public void testSendMetadataCreated() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getOkResponse(null));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/result/url");

        publisher.send("foo", "bar");
        verify(mockClient).preparePost("my/custom/query/url");
        verify(mockBuilder).setBody("{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":null,\"content\":\"my/custom/result/url\"}}");
        verify(mockBuilder).setHeader("content-type", "application/json");
    }

    @Test
    public void testClose() throws Exception {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doNothing().when(mockClient).close();
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, null, null);

        publisher.close();
        verify(mockClient).close();
    }

    @Test
    public void testCloseDoesNotThrow() throws Exception {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doThrow(new IOException("error!")).when(mockClient).close();
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, null, null);

        publisher.close();
        verify(mockClient).close();
    }

    @Test
    public void testHandleBadResponse() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getNotOkResponse(500));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/result/url");

        publisher.send(new PubSubMessage("foo", "bar", Metadata.Signal.COMPLETE));
        verify(mockClient).preparePost("my/custom/query/url");
        verify(mockBuilder).setBody("{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":\"COMPLETE\",\"content\":\"my/custom/result/url\"}}");
        verify(mockBuilder).setHeader("content-type", "application/json");
    }

    @Test(timeOut = 5000L)
    public void testException() throws Exception {
        // This will hit a non-existent url and fail, testing our exceptions
        AsyncHttpClientConfig clientConfig = new DefaultAsyncHttpClientConfig.Builder().setConnectTimeout(100)
                                                                                       .setMaxRequestRetry(1)
                                                                                       .setReadTimeout(-1)
                                                                                       .setRequestTimeout(-1)
                                                                                       .build();
        AsyncHttpClient client = new DefaultAsyncHttpClient(clientConfig);
        AsyncHttpClient spyClient = spy(client);
        RESTQueryPublisher publisher = new RESTQueryPublisher(spyClient, "http://this/does/not/exist:8080", "my/custom/result/url");

        publisher.send(new PubSubMessage("foo", "bar"));
        verify(spyClient).preparePost("http://this/does/not/exist:8080");
    }
}
