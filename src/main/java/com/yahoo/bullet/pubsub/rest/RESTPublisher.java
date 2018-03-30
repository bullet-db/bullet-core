/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.HttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import java.io.IOException;

@Slf4j
public abstract class RESTPublisher implements Publisher {
    public static final String APPLICATION_JSON = "application/json";
    public static final String CONTENT_TYPE = "content-type";

    private CloseableHttpClient client;

    /**
     * Create a RESTQueryPublisher from a {@link CloseableHttpClient}.
     *
     * @param client The client.
     */
    public RESTPublisher(CloseableHttpClient client) {
        this.client = client;
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            log.error("Caught exception when closing AsyncHttpClient...: ", e);
        }
    }

    /**
     * Send the {@link PubSubMessage} to the given url.
     *
     * @param url The url to which to send the message.
     * @param message The message to send.
     */
    protected void sendToURL(String url, PubSubMessage message) {
        log.debug("Sending message: {} to url: {}", message, url);
        try {
            HttpPost httpPost = new HttpPost(url);
            httpPost.setEntity(new StringEntity(message.asJSON()));
            httpPost.setHeader(CONTENT_TYPE, APPLICATION_JSON);
            client.execute(httpPost);
        } catch (Exception e) {
            log.error("Error encoding message in preparation for POST: ", e);
        }


    }







    static class RESTRequest implements FutureCallback<HttpResponse> {
        private String url;
        private String message;
        private int maxRetries;
        private int retries;
        private CloseableHttpAsyncClient client;

        public RESTRequest(String url, String message, int maxRetries, CloseableHttpAsyncClient client) {
            this.url = url;
            this.message = message;
            this.maxRetries = maxRetries;
            this.client = client;
            this.retries = 0;
        }

        @Override
        public void completed(HttpResponse response) {
            if (response == null || response.getStatusLine().getStatusCode() != RESTPubSub.OK_200) {
                error("Couldn't reach REST pubsub server. Got response: {}", response);
                return;
            }
            log.debug("Successfully wrote message with status code {}. Response was: {}", response.getStatusLine().getStatusCode(), response);
        }

        @Override
        public void failed(Exception e) {
            error("Failed to post message to RESTPubSub endpoint. Failed with error: ", e);
            retries++;
            send();
        }

        @Override
        public void cancelled() {
            error("Failed to post message to RESTPubSub endpoint. Request was cancelled.");
        }

        public void send() {
            this.retries++;
            log.debug("Sending message: {} to url: {}", message, url);
            try {
                synchronized (client) {
                    HttpPost httpPost = new HttpPost(url);
                    httpPost.setEntity(new StringEntity(message));
                    httpPost.setHeader(CONTENT_TYPE, APPLICATION_JSON);
                    client.execute(httpPost, this);
                }
            } catch (Exception e) {
                log.error("Error encoding message in preparation for POST: ", e);
            }
        }

        // Exposed for testing
        void error(String s) {
            log.error(s);
        }

        // Exposed for testing
        void error(String s, Object o) {
            log.error(s, o);
        }
    }
}
