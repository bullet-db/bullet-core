/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;
import java.io.IOException;

import org.testng.Assert;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class RESTQueryPublisherTest {
    private static final byte[] CONTENT = "bar".getBytes(PubSubMessage.CHARSET);

    @Test
    public void testSendResultUrlPutInMetadataAckPreserved() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = mock(StatusLine.class);
        doReturn(200).when(mockStatusLine).getStatusCode();
        doReturn(mockStatusLine).when(mockResponse).getStatusLine();
        doReturn(mockResponse).when(mockClient).execute(any());
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/url", 5000);
        PubSubMessage actual = publisher.send(new PubSubMessage("foo", CONTENT, Metadata.Signal.ACKNOWLEDGE));

        Assert.assertTrue(actual.getMetadata() instanceof RESTMetadata);
        RESTMetadata actualMeta = (RESTMetadata) actual.getMetadata();
        Assert.assertEquals(actualMeta.getUrl(), "my/custom/url");

        ArgumentCaptor<HttpPost> argumentCaptor = ArgumentCaptor.forClass(HttpPost.class);
        verify(mockClient).execute(argumentCaptor.capture());
        HttpPost post = argumentCaptor.getValue();
        String actualMessage = EntityUtils.toString(post.getEntity(), RESTPubSub.UTF_8);
        String expectedMessage = "{'id':'foo','content':[98,97,114],'metadata':{'url':'my/custom/url','signal':ACKNOWLEDGE,'content':null,'created':" + actual.getMetadata().getCreated() + "}}";
        String actualHeader = post.getHeaders(RESTPublisher.CONTENT_TYPE)[0].getValue();
        String expectedHeader = RESTPublisher.APPLICATION_JSON;
        assertJSONEquals(actualMessage, expectedMessage);
        Assert.assertEquals(expectedHeader, actualHeader);
        Assert.assertEquals("my/custom/query/url", post.getURI().toString());
    }

    @Test
    public void testSendResultUrlPutInMetadataCompletePreserved() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/url", 5000);
        PubSubMessage actual = publisher.send(new PubSubMessage("foo", CONTENT, Metadata.Signal.COMPLETE));

        ArgumentCaptor<HttpPost> argumentCaptor = ArgumentCaptor.forClass(HttpPost.class);
        verify(mockClient).execute(argumentCaptor.capture());
        HttpPost post = argumentCaptor.getValue();
        String actualMessage = EntityUtils.toString(post.getEntity(), RESTPubSub.UTF_8);
        String expectedMessage = "{'id':'foo','content':[98,97,114],'metadata':{'url':'my/custom/url','signal':COMPLETE,'content':null,'created': " + actual.getMetadata().getCreated() + "}}";
        String actualHeader = post.getHeaders(RESTPublisher.CONTENT_TYPE)[0].getValue();
        String expectedHeader = RESTPublisher.APPLICATION_JSON;
        assertJSONEquals(actualMessage, expectedMessage);
        Assert.assertEquals(actualHeader, expectedHeader);
        Assert.assertEquals("my/custom/query/url", post.getURI().toString());
    }

    @Test
    public void testSendMetadataCreated() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/url", 5000);
        PubSubMessage actual = publisher.send("foo", CONTENT);

        ArgumentCaptor<HttpPost> argumentCaptor = ArgumentCaptor.forClass(HttpPost.class);
        verify(mockClient).execute(argumentCaptor.capture());
        HttpPost post = argumentCaptor.getValue();
        String actualMessage = EntityUtils.toString(post.getEntity(), RESTPubSub.UTF_8);
        String expectedMessage = "{'id':'foo','content':[98,97,114],'metadata':{'url':'my/custom/url','signal':null,'content':null,'created':" + actual.getMetadata().getCreated() + "}}";
        String actualHeader = post.getHeaders(RESTPublisher.CONTENT_TYPE)[0].getValue();
        String expectedHeader = RESTPublisher.APPLICATION_JSON;
        assertJSONEquals(actualMessage, expectedMessage);
        Assert.assertEquals(actualHeader, expectedHeader);
        Assert.assertEquals("my/custom/query/url", post.getURI().toString());
    }

    @Test
    public void testClose() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        doNothing().when(mockClient).close();
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/url", 5000);
        publisher.close();
        verify(mockClient).close();
    }

    @Test
    public void testCloseDoesNotThrow() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        doThrow(new IOException("error!")).when(mockClient).close();
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, null, null, 5000);

        publisher.close();
        verify(mockClient).close();
    }

    @Test
    public void testBadResponseDoesNotThrow() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        RESTQueryPublisher publisher = new RESTQueryPublisher(mockClient, "my/custom/query/url", "my/custom/result/url", 5000);
        PubSubMessage message = mock(PubSubMessage.class);
        // This will compel the HttpPost object to throw an exception in RESTPublisher.sendToURL()
        doReturn(null).when(message).asJSON();
        publisher.send(message);
        Assert.assertTrue(true);
        verify(mockClient, never()).execute(any());
    }
}
