/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.PubSubMessage;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RESTSubscriberTest {
    private CloseableHttpClient mockClient(int responseCode, String message) throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = mock(StatusLine.class);
        doReturn(responseCode).when(mockStatusLine).getStatusCode();
        doReturn(mockStatusLine).when(mockResponse).getStatusLine();
        HttpEntity mockEntity = mock(HttpEntity.class);
        InputStream inputStream = new ByteArrayInputStream(message.getBytes(RESTPubSub.UTF_8));
        doReturn(inputStream).when(mockEntity).getContent();
        doReturn(mockEntity).when(mockResponse).getEntity();
        doReturn(mockResponse).when(mockClient).execute(any());

        return mockClient;
    }

    @Test
    public void testGetMessages() throws Exception {
        String message = new PubSubMessage("foo", "bar").asJSON();
        CloseableHttpClient mockClient = mockClient(RESTPubSub.OK_200, message);
        RESTSubscriber subscriber = new RESTSubscriber(88, Arrays.asList("url", "anotherURL"), mockClient, 10, 3000);
        List<PubSubMessage> messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 2);
        Assert.assertEquals(messages.get(0).asJSON(), "{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":null}");
    }

    @Test
    public void testGetMessages204() throws Exception {
        String message = new PubSubMessage("foo", "bar").asJSON();
        CloseableHttpClient mockClient = mockClient(RESTPubSub.NO_CONTENT_204, message);
        RESTSubscriber subscriber = new RESTSubscriber(88, Arrays.asList("url", "anotherURL"), mockClient, 10, 3000);

        List<PubSubMessage> messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 0);
    }

    @Test
    public void testGetMessages500() throws Exception {
        String message = new PubSubMessage("foo", "bar").asJSON();
        CloseableHttpClient mockClient = mockClient(500, message);
        RESTSubscriber subscriber = new RESTSubscriber(88, Arrays.asList("url", "anotherURL"), mockClient, 10, 3000);

        List<PubSubMessage> messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 0);
    }

    @Test
    public void testGetMessagesDoesNotThrow() throws Exception {
        String message = new PubSubMessage("foo", "bar").asJSON();
        CloseableHttpClient mockClient = mockClient(500, message);
        List<String> urls = new ArrayList<>();
        // A null url will throw an error - make sure it handled eloquently
        urls.add(null);
        RESTSubscriber subscriber = new RESTSubscriber(88, urls, mockClient, 10, 3000);

        List<PubSubMessage> messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 0);
    }

    @Test
    public void testClose() throws Exception {
        String message = new PubSubMessage("foo", "bar").asJSON();
        CloseableHttpClient mockClient = mockClient(500, message);
        doNothing().when(mockClient).close();
        RESTSubscriber subscriber = new RESTSubscriber(88, Arrays.asList("url", "anotherURL"), mockClient, 10, 3000);

        subscriber.close();
        verify(mockClient).close();
    }

    @Test
    public void testCloseDoesNotThrow() throws Exception {
        String message = new PubSubMessage("foo", "bar").asJSON();
        CloseableHttpClient mockClient = mockClient(500, message);
        doThrow(new IOException("error!")).when(mockClient).close();
        RESTSubscriber subscriber = new RESTSubscriber(88, Arrays.asList("url", "anotherURL"), mockClient, 10, 3000);

        subscriber.close();
        verify(mockClient).close();
    }

    @Test
    public void testMinWait() throws Exception {
        String message = new PubSubMessage("someID", "someContent").asJSON();
        CloseableHttpClient mockClient = mockClient(RESTPubSub.OK_200, message);
        RESTSubscriber subscriber = new RESTSubscriber(88, Arrays.asList("url", "anotherURL"), mockClient, 100, 3000);

        // First response should give content (2 events since we have 2 endpoints in the config)
        List<PubSubMessage> messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 2);
        // Second response should give nothing since the wait duration hasn't passed
        messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 0);

        // After waiting it should return messages again
        Thread.sleep(150);
        messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 2);
    }
}
