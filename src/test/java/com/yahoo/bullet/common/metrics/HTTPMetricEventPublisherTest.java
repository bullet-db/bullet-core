/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common.metrics;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static com.yahoo.bullet.TestHelpers.mockHTTPClient;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class HTTPMetricEventPublisherTest {
    private static final String TIMESTAMP = "timestamp";

    private static class FailingMetricEventPublisher extends HTTPMetricEventPublisher {
        private FailingMetricEventPublisher(CloseableHttpClient client) {
            super("url", "group", emptyMap(), client, 0, 0);
        }

        @Override
        public CompletableFuture<Boolean> publish(Map<String, String> dimensions, Map<String, Number> metrics) {
            throw new RuntimeException("Tried to publish when should not have");
        }
    }

    private static class MockHTTPMetricEventPublisher extends HTTPMetricEventPublisher {
        private CompletableFuture<Boolean> publishFuture;
        private boolean shouldThrow = false;

        private MockHTTPMetricEventPublisher(boolean shouldThrow, Map<String, String> dimensions, CloseableHttpClient client) {
            super("url", "group", emptyMap(), client, 0, 0);
            this.shouldThrow = shouldThrow;
        }

        private MockHTTPMetricEventPublisher(Map<String, String> dimensions, CloseableHttpClient client) {
            super("url", "group", dimensions, client, 0, 0);
        }

        private MockHTTPMetricEventPublisher(CloseableHttpClient client) {
            this(emptyMap(), client);
        }

        @Override
        public CompletableFuture<Boolean> publish(MetricEvent event) {
            if (shouldThrow) {
                publishFuture = new CompletableFuture<>();
                publishFuture.completeExceptionally(new RuntimeException("Testing"));
            } else {
                publishFuture = super.publish(event);
            }
            return publishFuture;
        }

        private void waitForPublish() throws Exception {
            publishFuture.get();
        }
    }

    private static class UnabortableHTTPMetricEventPublisher extends MockHTTPMetricEventPublisher {
        private UnabortableHTTPMetricEventPublisher(CloseableHttpClient client) {
            super(emptyMap(), client);
        }

        @Override
        HttpUriRequest getPost(String body) {
            HttpUriRequest request = mock(HttpUriRequest.class);
            doThrow(new RuntimeException("Testing")).when(request).abort();
            return request;
        }
    }

    private static String getPayload(HttpEntityEnclosingRequestBase requestBase) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            requestBase.getEntity().writeTo(bos);
            byte[] data = bos.toByteArray();
            return new String(data, ContentType.DEFAULT_TEXT.getCharset());
        } catch (Exception e) {
            return null;
        }
    }

    private static void assertJSONEqualsNoTimestamp(String actual, String expected) {
        assertJSONEquals(actual, expected, TIMESTAMP);
    }

    @Test
    public void testClosing() throws Exception {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        doThrow(new IOException("Testing")).when(client).close();
        HTTPMetricEventPublisher publisher = new HTTPMetricEventPublisher("url", "group", emptyMap(), client, 0, 0);
        publisher.close();
        verify(client).close();

        client = mock(CloseableHttpClient.class);
        publisher = new HTTPMetricEventPublisher("url", "group", emptyMap(), client, 0, 0);
        publisher.close();
        verify(client).close();
    }

    @Test
    public void testPublishingAMetric() throws Exception {
        CloseableHttpClient client = mockHTTPClient(200);
        HTTPMetricEventPublisher publisher = new HTTPMetricEventPublisher("url", "group", emptyMap(), client, 0, 0);
        boolean result = publisher.publish("metric", 1L).get();
        Assert.assertTrue(result);
    }

    @Test
    public void testPublishingMetrics() throws Exception {
        CloseableHttpClient client = mockHTTPClient(200);
        HTTPMetricEventPublisher publisher = new HTTPMetricEventPublisher("url", "group", emptyMap(), client, 0, 0);
        boolean result = publisher.publish(singletonMap("metric", 1L)).get();
        Assert.assertTrue(result);
    }

    @Test
    public void testPublishingDimensionsAndMetrics() throws Exception {
        CloseableHttpClient client = mockHTTPClient(200);
        HTTPMetricEventPublisher publisher = new HTTPMetricEventPublisher("url", "group", emptyMap(), client, 0, 0);
        boolean result = publisher.publish(singletonMap("dimension", "value"), singletonMap("metric", 1L)).get();
        Assert.assertTrue(result);
    }

    @Test
    public void testFiringAMetric() throws Exception {
        CloseableHttpClient client = mockHTTPClient(200);
        MockHTTPMetricEventPublisher publisher = new MockHTTPMetricEventPublisher(singletonMap("foo", "bar"), client);
        publisher.fire("metric", 1L);
        publisher.waitForPublish();

        ArgumentCaptor<HttpUriRequest> requestCaptor = ArgumentCaptor.forClass(HttpUriRequest.class);
        verify(client).execute(requestCaptor.capture());
        String payload = getPayload((HttpEntityEnclosingRequestBase) requestCaptor.getValue());
        assertJSONEqualsNoTimestamp(payload, new MetricEvent("group", singletonMap("foo", "bar"), singletonMap("metric", 1L)).asJSON());
    }

    @Test
    public void testFiringMetrics() throws Exception {
        CloseableHttpClient client = mockHTTPClient(200);
        MockHTTPMetricEventPublisher publisher = new MockHTTPMetricEventPublisher(client);
        publisher.fire(singletonMap("metric", 1L));
        publisher.waitForPublish();

        ArgumentCaptor<HttpUriRequest> requestCaptor = ArgumentCaptor.forClass(HttpUriRequest.class);
        verify(client).execute(requestCaptor.capture());
        String payload = getPayload((HttpEntityEnclosingRequestBase) requestCaptor.getValue());
        assertJSONEqualsNoTimestamp(payload, new MetricEvent("group", emptyMap(), singletonMap("metric", 1L)).asJSON());
    }

    @Test
    public void testFiringDimensionsAndMetrics() throws Exception {
        CloseableHttpClient client = mockHTTPClient(200);
        MockHTTPMetricEventPublisher publisher = new MockHTTPMetricEventPublisher(client);
        publisher.fire(singletonMap("dimension", "value"), singletonMap("metric", 1L));
        publisher.waitForPublish();

        ArgumentCaptor<HttpUriRequest> requestCaptor = ArgumentCaptor.forClass(HttpUriRequest.class);
        verify(client).execute(requestCaptor.capture());
        String payload = getPayload((HttpEntityEnclosingRequestBase) requestCaptor.getValue());
        assertJSONEqualsNoTimestamp(payload, new MetricEvent("group", singletonMap("dimension", "value"), singletonMap("metric", 1L)).asJSON());
    }

    @Test
    public void testExceptionWhileRequesting() throws Exception {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        HTTPMetricEventPublisher publisher = new HTTPMetricEventPublisher("url", "group", emptyMap(), client, 0, 0);
        boolean result = publisher.publish("metric", 1L).get();
        Assert.assertFalse(result);
    }

    @Test
    public void testExceptionWhilePublishing() throws Exception {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        doThrow(new RuntimeException("Testing")).when(client).execute(any());
        HTTPMetricEventPublisher publisher = new UnabortableHTTPMetricEventPublisher(client);
        boolean result = publisher.publish("metric", 1L).get();
        Assert.assertFalse(result);
    }

    @Test
    public void testFailingToPublishAMetric() throws Exception {
        CloseableHttpClient client = mockHTTPClient(400);
        HTTPMetricEventPublisher publisher = new HTTPMetricEventPublisher("url", "group", emptyMap(), client, 0, 0);
        boolean result = publisher.publish("metric", 1L).get();
        Assert.assertFalse(result);
    }

    @Test
    public void testNotFiringEmptyMetrics() {
        CloseableHttpClient client = mockHTTPClient(400);
        FailingMetricEventPublisher publisher = new FailingMetricEventPublisher(client);
        publisher.fire(singletonMap("foo", "bar"), emptyMap());
        verifyZeroInteractions(client);
    }

    @Test
    public void testExceptionWhileFiring() {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        MockHTTPMetricEventPublisher publisher = new MockHTTPMetricEventPublisher(true, emptyMap(), client);
        publisher.fire("metric", 1L);
        publisher.publishFuture.thenRun(Assert::fail);
    }

    @Test
    public void testFailingToFireAMetric() throws Exception {
        CloseableHttpClient client = mockHTTPClient(400);
        MockHTTPMetricEventPublisher publisher = new MockHTTPMetricEventPublisher(client);
        publisher.fire("metric", 1L);
        Boolean result = publisher.publishFuture.get();
        Assert.assertFalse(result);
    }

    @Test
    public void testRetrying() throws Exception {
        CloseableHttpClient client = mockHTTPClient(400, 200);
        HTTPMetricEventPublisher publisher = new HTTPMetricEventPublisher("url", "group", emptyMap(), client, 2, 0);
        boolean result = publisher.publish("metric", 1L).get();
        Assert.assertTrue(result);
    }

    @Test
    public void testRetryingButFailing() throws Exception {
        CloseableHttpClient client = mockHTTPClient(400, 400);
        HTTPMetricEventPublisher publisher = new HTTPMetricEventPublisher("url", "group", emptyMap(), client, 2, 0);
        boolean result = publisher.publish("metric", 1L).get();
        Assert.assertFalse(result);
    }
}
