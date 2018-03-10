/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static com.yahoo.bullet.pubsub.memory.MemoryPubSubTest.getOkFuture;
import static com.yahoo.bullet.pubsub.memory.MemoryPubSubTest.getOkResponse;
import static com.yahoo.bullet.pubsub.memory.MemoryPubSubTest.mockBuilderWith;
import static com.yahoo.bullet.pubsub.memory.MemoryPubSubTest.mockClientWith;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MemoryResultPublisherTest {
    @Test
    public void testSend() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getOkResponse(null));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        MemoryResultPublisher publisher = new MemoryResultPublisher(config, mockClient);

        PubSubMessage message = new PubSubMessage("someId", "someContent", new Metadata(null, "custom/url"));
        publisher.send(message);
        verify(mockClient).preparePost("custom/url");
        verify(mockBuilder).setBody("{\"id\":\"someId\",\"sequence\":-1,\"content\":\"someContent\",\"metadata\":{\"signal\":null,\"content\":\"custom/url\"}}");
        verify(mockBuilder).setHeader("content-type", "text/plain");
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testSendBadURI() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getOkResponse(null));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        MemoryResultPublisher publisher = new MemoryResultPublisher(config, mockClient);

        PubSubMessage message = new PubSubMessage("someId", "someContent", new Metadata(null, 88));
        publisher.send(message);
    }

    @Test
    public void testClose() throws Exception {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doNothing().when(mockClient).close();
        MemoryResultPublisher publisher = new MemoryResultPublisher(new MemoryPubSubConfig((String) null), mockClient);

        publisher.close();
        verify(mockClient).close();
    }

    @Test
    public void testCloseDoesNotThrow() throws Exception {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doThrow(new IOException("error!")).when(mockClient).close();
        MemoryResultPublisher publisher = new MemoryResultPublisher(new MemoryPubSubConfig((String) null), mockClient);

        publisher.close();
        verify(mockClient).close();
    }
}
