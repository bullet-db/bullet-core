/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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

    private BoundRequestBuilder mockBuilderThatThrows(Throwable throwable) {
        ListenableFuture<Response> mockListenable = (ListenableFuture<Response>) mock(ListenableFuture.class);
        doThrow(throwable).when(mockListenable).toCompletableFuture();

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

    private Response getNotOkResponse(int status) {
        return getResponse(status, "Error", null);
    }

    private Response getResponse(int status, String statusText, String body) {
        Response mock = mock(Response.class);
        doReturn(status).when(mock).getStatusCode();
        doReturn(statusText).when(mock).getStatusText();
        doReturn(body).when(mock).getResponseBody();
        return mock;
    }

    @Test
    public void testSendResultUriPutInMetadataAckPreserved() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getOkResponse(null));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        config.set(MemoryPubSubConfig.RESULT_URI, "my/custom/uri");
        MemoryQueryPublisher publisher = new MemoryQueryPublisher(config, mockClient);

        publisher.send(new PubSubMessage("foo", "bar", Metadata.Signal.ACKNOWLEDGE));
        verify(mockClient).preparePost("http://localhost:9901/CUSTOM/query");
        verify(mockBuilder).setBody("{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":\"ACKNOWLEDGE\",\"content\":\"my/custom/uri\"}}");
        verify(mockBuilder).setHeader("content-type", "text/plain");
    }

    @Test
    public void testSendResultUriPutInMetadataCompletePreserved() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getOkResponse(null));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        config.set(MemoryPubSubConfig.RESULT_URI, "my/custom/uri");
        MemoryQueryPublisher publisher = new MemoryQueryPublisher(config, mockClient);

        publisher.send(new PubSubMessage("foo", "bar", Metadata.Signal.COMPLETE));
        verify(mockClient).preparePost("http://localhost:9901/CUSTOM/query");
        verify(mockBuilder).setBody("{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":\"COMPLETE\",\"content\":\"my/custom/uri\"}}");
        verify(mockBuilder).setHeader("content-type", "text/plain");
    }

    @Test
    public void testSendMetadataCreated() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getOkResponse(null));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        config.set(MemoryPubSubConfig.RESULT_URI, "my/custom/uri");
        MemoryQueryPublisher publisher = new MemoryQueryPublisher(config, mockClient);

        publisher.send("foo", "bar");
        verify(mockClient).preparePost("http://localhost:9901/CUSTOM/query");
        verify(mockBuilder).setBody("{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":null,\"content\":\"my/custom/uri\"}}");
        verify(mockBuilder).setHeader("content-type", "text/plain");
    }

    @Test
    public void testClose() throws Exception {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doNothing().when(mockClient).close();
        MemoryQueryPublisher publisher = new MemoryQueryPublisher(new MemoryPubSubConfig((String) null), mockClient);

        publisher.close();
        verify(mockClient).close();
    }

    @Test
    public void testCloseDoesNotThrow() throws Exception {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doThrow(new IOException("error!")).when(mockClient).close();
        MemoryQueryPublisher publisher = new MemoryQueryPublisher(new MemoryPubSubConfig((String) null), mockClient);

        publisher.close();
        verify(mockClient).close();
    }

    @Test
    public void testHandleBadResponse() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getNotOkResponse(500));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        config.set(MemoryPubSubConfig.RESULT_URI, "my/custom/uri");
        MemoryQueryPublisher publisher = new MemoryQueryPublisher(config, mockClient);

        publisher.send(new PubSubMessage("foo", "bar", Metadata.Signal.COMPLETE));
        verify(mockClient).preparePost("http://localhost:9901/CUSTOM/query");
        verify(mockBuilder).setBody("{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":\"COMPLETE\",\"content\":\"my/custom/uri\"}}");
        verify(mockBuilder).setHeader("content-type", "text/plain");
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
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        MemoryQueryPublisher publisher = new MemoryQueryPublisher(config, spyClient);

        publisher.send(new PubSubMessage("foo", "bar"));
        verify(spyClient).preparePost("http://localhost:9901/CUSTOM/query");
    }
}
