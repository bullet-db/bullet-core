/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RESTResultPublisherTest {
    @Test
    public void testSendPullsURLFromMessage() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        RESTResultPublisher publisher = new RESTResultPublisher(mockClient, 5000);
        Metadata metadata = new Metadata(null, "my/custom/url");
        publisher.send(new PubSubMessage("foo", "bar", metadata));

        ArgumentCaptor<HttpPost> argumentCaptor = ArgumentCaptor.forClass(HttpPost.class);
        verify(mockClient).execute(argumentCaptor.capture());
        HttpPost post = argumentCaptor.getValue();

        String actualURI = post.getURI().toString();
        String actualMessage = EntityUtils.toString(post.getEntity(), RESTPubSub.UTF_8);
        String actualHeader = post.getHeaders(RESTPublisher.CONTENT_TYPE)[0].getValue();

        String expectedURI = "my/custom/url";
        String expectedMessage = "{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":null,\"content\":\"my/custom/url\"}}";
        String expectedHeader = RESTPublisher.APPLICATION_JSON;

        Assert.assertEquals(expectedMessage, actualMessage);
        Assert.assertEquals(expectedHeader, actualHeader);
        Assert.assertEquals(expectedURI, actualURI);
    }

    @Test
    public void testClose() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        doNothing().when(mockClient).close();
        RESTResultPublisher publisher = new RESTResultPublisher(mockClient, 5000);

        publisher.close();
        verify(mockClient).close();
    }

    @Test
    public void testCloseDoesNotThrow() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        doThrow(new IOException("error!")).when(mockClient).close();
        RESTResultPublisher publisher = new RESTResultPublisher(mockClient, 5000);

        publisher.close();
        verify(mockClient).close();
    }
}
