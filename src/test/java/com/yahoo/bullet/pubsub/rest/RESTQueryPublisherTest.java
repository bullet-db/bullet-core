/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.mockito.ArgumentCaptor;

import org.testng.annotations.Test;
import java.io.IOException;
import org.testng.Assert;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

public class RESTQueryPublisherTest {
    @Test
    public void testSendResultUrlPutInMetadataAckPreserved() throws Exception {
        CloseableHttpAsyncClient mockClient = mock(CloseableHttpAsyncClient.class);
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/url");
        publisher.send(new PubSubMessage("foo", "bar", Metadata.Signal.ACKNOWLEDGE));

        ArgumentCaptor<HttpPost> argumentCaptor = ArgumentCaptor.forClass(HttpPost.class);
        ArgumentCaptor<RESTPublisher.RESTRequest> argumentCaptor2 = ArgumentCaptor.forClass(RESTPublisher.RESTRequest.class);
        verify(mockClient).execute(argumentCaptor.capture(), argumentCaptor2.capture());
        HttpPost post = argumentCaptor.getValue();
        String actualMessage = EntityUtils.toString(post.getEntity(), RESTPubSub.UTF_8);
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
        ArgumentCaptor<RESTPublisher.RESTRequest> argumentCaptor2 = ArgumentCaptor.forClass(RESTPublisher.RESTRequest.class);
        verify(mockClient).execute(argumentCaptor.capture(), argumentCaptor2.capture());
        HttpPost post = argumentCaptor.getValue();
        String actualMessage = EntityUtils.toString(post.getEntity(), RESTPubSub.UTF_8);
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
        ArgumentCaptor<RESTPublisher.RESTRequest> argumentCaptor2 = ArgumentCaptor.forClass(RESTPublisher.RESTRequest.class);
        verify(mockClient).execute(argumentCaptor.capture(), argumentCaptor2.capture());
        HttpPost post = argumentCaptor.getValue();
        String actualMessage = EntityUtils.toString(post.getEntity(), RESTPubSub.UTF_8);
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
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/result/url");
        PubSubMessage message = mock(PubSubMessage.class);
        // This will compel the HttpPost object to throw an exception in RESTPublisher.sendToURL()
        doReturn(null).when(message).asJSON();
        publisher.send(message);
        Assert.assertTrue(true);
        verify(mockClient, never()).execute(any(), any());
    }

    @Test
    public void testrestRequestCompletedGood() throws Exception {
        HttpResponse mockResponse = mock(HttpResponse.class);
        StatusLine mockStatusLine = mock(StatusLine.class);
        doReturn(RESTPubSub.OK_200).when(mockStatusLine).getStatusCode();
        doReturn(mockStatusLine).when(mockResponse).getStatusLine();
        RESTPublisher.RESTRequest restRequest = spy(RESTPublisher.RESTRequest.class);
        restRequest.completed(mockResponse);
        verify(restRequest, never()).error(anyString());
        verify(restRequest, never()).error(anyString(), any());
    }

    @Test
    public void testrestRequestCompletedBad() throws Exception {
        RESTPublisher.RESTRequest restRequest = spy(RESTPublisher.RESTRequest.class);
        restRequest.completed(null);
        verify(restRequest).error(anyString(), any());
    }

    @Test
    public void testrestRequestFailed() throws Exception {
        RESTPublisher.RESTRequest restRequest = spy(RESTPublisher.RESTRequest.class);
        restRequest.failed(new RuntimeException("error"));
        verify(restRequest).error(anyString(), any());
    }

    @Test
    public void testrestRequestCancelled() throws Exception {
        RESTPublisher.RESTRequest restRequest = spy(RESTPublisher.RESTRequest.class);
        restRequest.cancelled();
        verify(restRequest).error(anyString());
    }
}
