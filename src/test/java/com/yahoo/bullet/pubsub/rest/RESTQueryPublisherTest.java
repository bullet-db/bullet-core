/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpPost;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;
import java.io.IOException;
import org.testng.Assert;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

public class RESTQueryPublisherTest {
    @Test
    public void testSendResultUrlPutInMetadataAckPreserved() throws Exception {
        CloseableHttpAsyncClient mockClient = mock(CloseableHttpAsyncClient.class);
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/url");
        publisher.send(new PubSubMessage("foo", "bar", Metadata.Signal.ACKNOWLEDGE));

        ArgumentCaptor<HttpPost> argumentCaptor = ArgumentCaptor.forClass(HttpPost.class);
        ArgumentCaptor<RESTPublisher.RequestCallback> argumentCaptor2 = ArgumentCaptor.forClass(RESTPublisher.RequestCallback.class);
        verify(mockClient).execute(argumentCaptor.capture(), argumentCaptor2.capture());
        HttpPost post = argumentCaptor.getValue();
        String actualMessage = IOUtils.toString(post.getEntity().getContent(), RESTPubSub.UTF_8);
        String expectedMessage = "{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":\"ACKNOWLEDGE\",\"content\":\"my/custom/url\"}}";
        String actualHeader = post.getHeaders(RESTPublisher.CONTENT_TYPE)[0].getValue();
        String expectedHeader = RESTPublisher.APPLICATION_JSON;
        Assert.assertEquals(expectedMessage, actualMessage);
        Assert.assertEquals(expectedHeader, actualHeader);
        Assert.assertEquals("my/custom/query/url", post.getURI().toString());
    }

    @Test
    public void testSendResultUrlPutInMetadataCompletePreserved() throws Exception {
        CloseableHttpAsyncClient mockClient = mock(CloseableHttpAsyncClient.class);
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/url");
        publisher.send(new PubSubMessage("foo", "bar", Metadata.Signal.COMPLETE));

        ArgumentCaptor<HttpPost> argumentCaptor = ArgumentCaptor.forClass(HttpPost.class);
        ArgumentCaptor<RESTPublisher.RequestCallback> argumentCaptor2 = ArgumentCaptor.forClass(RESTPublisher.RequestCallback.class);
        verify(mockClient).execute(argumentCaptor.capture(), argumentCaptor2.capture());
        HttpPost post = argumentCaptor.getValue();
        String actualMessage = IOUtils.toString(post.getEntity().getContent(), RESTPubSub.UTF_8);
        String expectedMessage = "{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":\"COMPLETE\",\"content\":\"my/custom/url\"}}";
        String actualHeader = post.getHeaders(RESTPublisher.CONTENT_TYPE)[0].getValue();
        String expectedHeader = RESTPublisher.APPLICATION_JSON;
        Assert.assertEquals(expectedMessage, actualMessage);
        Assert.assertEquals(expectedHeader, actualHeader);
        Assert.assertEquals("my/custom/query/url", post.getURI().toString());
    }

    @Test
    public void testSendMetadataCreated() throws Exception {
        CloseableHttpAsyncClient mockClient = mock(CloseableHttpAsyncClient.class);
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/url");
        publisher.send("foo", "bar");

        ArgumentCaptor<HttpPost> argumentCaptor = ArgumentCaptor.forClass(HttpPost.class);
        ArgumentCaptor<RESTPublisher.RequestCallback> argumentCaptor2 = ArgumentCaptor.forClass(RESTPublisher.RequestCallback.class);
        verify(mockClient).execute(argumentCaptor.capture(), argumentCaptor2.capture());
        HttpPost post = argumentCaptor.getValue();
        String actualMessage = IOUtils.toString(post.getEntity().getContent(), RESTPubSub.UTF_8);
        String expectedMessage = "{\"id\":\"foo\",\"sequence\":-1,\"content\":\"bar\",\"metadata\":{\"signal\":null,\"content\":\"my/custom/url\"}}";
        String actualHeader = post.getHeaders(RESTPublisher.CONTENT_TYPE)[0].getValue();
        String expectedHeader = RESTPublisher.APPLICATION_JSON;
        Assert.assertEquals(expectedMessage, actualMessage);
        Assert.assertEquals(expectedHeader, actualHeader);
        Assert.assertEquals("my/custom/query/url", post.getURI().toString());
    }

    @Test
    public void testClose() throws Exception {
        CloseableHttpAsyncClient mockClient = mock(CloseableHttpAsyncClient.class);
        doNothing().when(mockClient).close();
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/url");
        publisher.close();
        verify(mockClient).close();
    }

    @Test
    public void testCloseDoesNotThrow() throws Exception {
        CloseableHttpAsyncClient mockClient = mock(CloseableHttpAsyncClient.class);
        doThrow(new IOException("error!")).when(mockClient).close();
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, null, null);

        publisher.close();
        verify(mockClient).close();
    }

    @Test
    public void testBadResponseDoesNotThrow() throws Exception {
        CloseableHttpAsyncClient mockClient = mock(CloseableHttpAsyncClient.class);
        // This won't work because this method doesn't declare that it throws - figure out how to make the HttpPost
        // object throw somehow?
        doThrow(new IOException("error!")).when(mockClient).execute(any(), any());
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/result/url");

        publisher.send(new PubSubMessage("foo", "bar", Metadata.Signal.COMPLETE));
        verify(mockClient).execute(any(), any());
    }

//    @Test(timeOut = 5000L)
//    public void testException() throws Exception {
//        // This will hit a non-existent url and fail, testing our exceptions
//        AsyncHttpClientConfig clientConfig = new DefaultAsyncHttpClientConfig.Builder().setConnectTimeout(100)
//                                                                                       .setMaxRequestRetry(1)
//                                                                                       .setReadTimeout(-1)
//                                                                                       .setRequestTimeout(-1)
//                                                                                       .build();
//        AsyncHttpClient client = new DefaultAsyncHttpClient(clientConfig);
//        AsyncHttpClient spyClient = spy(client);
//        RESTQueryPublisher publisher = new RESTQueryPublisher(spyClient, "http://this/does/not/exist:8080", "my/custom/result/url");
//
//        publisher.send(new PubSubMessage("foo", "bar"));
//        verify(spyClient).preparePost("http://this/does/not/exist:8080");
//    }
}
