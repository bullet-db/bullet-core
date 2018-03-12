/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.pubsub.PubSubMessage;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.yahoo.bullet.pubsub.memory.MemoryPubSubTest.getNotOkResponse;
import static com.yahoo.bullet.pubsub.memory.MemoryPubSubTest.getOkFuture;
import static com.yahoo.bullet.pubsub.memory.MemoryPubSubTest.getOkResponse;
import static com.yahoo.bullet.pubsub.memory.MemoryPubSubTest.mockBuilderWith;
import static com.yahoo.bullet.pubsub.memory.MemoryPubSubTest.mockClientWith;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MemorySubscriberTest {

    @Test
    public void testGetMessages() throws Exception {
        PubSubMessage responseData = new PubSubMessage("someID", "someContent");
        CompletableFuture<Response> response = getOkFuture(getOkResponse(responseData.asJSON()));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        MemorySubscriber subscriber = new MemorySubscriber(config, 88, Arrays.asList("uri", "anotherURI"), mockClient, 10);

        List<PubSubMessage> messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 2);
        Assert.assertEquals(messages.get(0).asJSON(), "{\"id\":\"someID\",\"sequence\":-1,\"content\":\"someContent\",\"metadata\":null}");
    }

    @Test
    public void testGetMessages204() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getNotOkResponse(204));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        MemorySubscriber subscriber = new MemorySubscriber(config, 88, Arrays.asList("uri", "anotherURI"), mockClient, 10);

        List<PubSubMessage> messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 0);
    }

    @Test
    public void testGetMessages500() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getNotOkResponse(500));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        MemorySubscriber subscriber = new MemorySubscriber(config, 88, Arrays.asList("uri", "anotherURI"), mockClient, 10);

        List<PubSubMessage> messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 0);
    }

    @Test
    public void testGetMessagesDoesNotThrow() throws Exception {
        // PubSubMessage will throw an error when it fails to parse this into a PubSubMessage
        CompletableFuture<Response> response = getOkFuture(getOkResponse("thisCannotBeTurnedIntoAPubSubMessage"));
        BoundRequestBuilder mockBuilder = mockBuilderWith(response);
        AsyncHttpClient mockClient = mockClientWith(mockBuilder);
        MemoryPubSubConfig config = new MemoryPubSubConfig("src/test/resources/test_config.yaml");
        MemorySubscriber subscriber = new MemorySubscriber(config, 88, Arrays.asList("uri", "anotherURI"), mockClient, 10);

        List<PubSubMessage> messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 0);
    }

    @Test
    public void testClose() throws Exception {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doNothing().when(mockClient).close();
        MemorySubscriber subscriber = new MemorySubscriber(new MemoryPubSubConfig((String) null), 88, Arrays.asList("uri", "anotherURI"), mockClient, 10);

        subscriber.close();
        verify(mockClient).close();
    }

    @Test
    public void testCloseDoesNotThrow() throws Exception {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doThrow(new IOException("error!")).when(mockClient).close();
        MemorySubscriber subscriber = new MemorySubscriber(new MemoryPubSubConfig((String) null), 88, Arrays.asList("uri", "anotherURI"), mockClient, 10);

        subscriber.close();
        verify(mockClient).close();
    }
}
