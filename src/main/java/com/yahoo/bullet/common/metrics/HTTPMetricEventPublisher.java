/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common.metrics;

import com.yahoo.bullet.common.BulletConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.http.entity.ContentType.APPLICATION_JSON;

/**
 * This class publishes a {@link MetricEvent} to a provided URL. It can retry publishes.
 */
@Slf4j
public class HTTPMetricEventPublisher extends MetricEventPublisher {
    private final String url;
    private final String group;
    private final int retries;
    private final int retryIntervalMS;
    private final Map<String, String> dimensions;
    @Getter @Setter
    private CloseableHttpClient client;

    /**
     * Constructor taking a {@link BulletConfig}.
     *
     * @param config The config to use containing the necessary information.
     */
    public HTTPMetricEventPublisher(BulletConfig config) {
        super(new HTTPMetricPublisherConfig(config).validate());
        this.url = this.config.getRequiredConfigAs(HTTPMetricPublisherConfig.URL, String.class);
        this.group = this.config.getRequiredConfigAs(HTTPMetricPublisherConfig.GROUP, String.class);
        this.dimensions = (Map<String, String>) this.config.getRequiredConfigAs(HTTPMetricPublisherConfig.DIMENSIONS, Map.class);
        this.retries = this.config.getRequiredConfigAs(HTTPMetricPublisherConfig.RETRIES, Integer.class);
        this.retryIntervalMS = this.config.getRequiredConfigAs(HTTPMetricPublisherConfig.RETRY_INTERVAL_MS, Integer.class);
        this.client = createClient();
        log.info("Using metrics URL: {} with retries: {} with interval: {}", url, retries, retryIntervalMS);
        log.info("Using static dimensions {}", dimensions);
    }

    /**
     * Creates a {@link CloseableHttpClient} client to use for HTTP requests.
     *
     * @return A created client that has the max concurrent connections set to the max per route.
     */
    protected CloseableHttpClient createClient() {
        final int concurrency = config.getRequiredConfigAs(HTTPMetricPublisherConfig.MAX_CONCURRENCY, Integer.class);
        PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
        // The max per route should be the max
        manager.setMaxTotal(concurrency);
        manager.setDefaultMaxPerRoute(concurrency);
        return HttpClients.custom().setConnectionManager(manager).build();
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            log.error("Could not close the HTTP client", e);
        }
    }

    @Override
    public String getGroup() {
        return group;
    }

    @Override
    public Map<String, String> getDimensions(Map<String, String> extraDimensions) {
        Map<String, String> dimensions = new HashMap<>(this.dimensions);
        dimensions.putAll(extraDimensions);
        return dimensions;
    }

    @Override
    public CompletableFuture<Boolean> publish(MetricEvent payload) {
        String json = payload.asJSON();
        log.debug("Publishing metric {}", json);
        HttpUriRequest post = getPost(json);
        return CompletableFuture.supplyAsync(() -> submitWithRetry(post, retries))
                                .thenApply(HTTPMetricEventPublisher::onHTTPResult)
                                .exceptionally(HTTPMetricEventPublisher::onHTTPFail);

    }

    /**
     * Exposed for testing.
     *
     * @param body The body to POST with.
     * @return A {@link HttpUriRequest} that is a POST.
     */
    HttpUriRequest getPost(String body) {
        return RequestBuilder.post()
                .setUri(url)
                .setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON.toString())
                .setEntity(new StringEntity(body, ContentType.DEFAULT_TEXT)).build();
    }

    private boolean submitWithRetry(HttpUriRequest post, int retries) {
        int count = 0;
        boolean status;
        do {
            count++;
            log.debug("Attempt {} of {}", count, retries);
            status = request(post);
        } while (shouldRetry(status, count, retries, retryIntervalMS));
        return status;
    }

    private boolean request(HttpUriRequest request) {
        try (CloseableHttpResponse response = client.execute(request)) {
            log.debug("Received response {}", response);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                return true;
            }
            log.error("Received {} with response {} from URL", statusCode, response);
            return false;
        } catch (Exception e) {
            request.abort();
            log.error("Unable to publish request", e);
            return false;
        }
    }

    private boolean shouldRetry(boolean status, int count, int retries, int interval) {
        boolean shouldNotRetry = status || count >= retries;
        if (shouldNotRetry) {
            return false;
        }
        try {
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            log.debug("Metrics publishing retry interrupted", e);
        }
        return true;
    }

    private static boolean onHTTPResult(boolean result) {
        log.debug("Received {} from publishing metrics", result);
        if (!result) {
            log.error("Failed to submit metrics");
        }
        return result;
    }

    private static boolean onHTTPFail(Throwable throwable) {
        log.error("Failed to submit metrics");
        log.error("Received", throwable);
        return false;
    }
}
