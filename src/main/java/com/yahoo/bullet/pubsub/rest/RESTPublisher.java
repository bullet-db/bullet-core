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
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

@Slf4j
public abstract class RESTPublisher implements Publisher {
    public static final String APPLICATION_JSON = "application/json";
    public static final String CONTENT_TYPE = "content-type";

    private CloseableHttpAsyncClient client;

    /**
     * Create a RESTQueryPublisher from a {@link CloseableHttpAsyncClient}.
     *
     * @param client The client.
     */
    public RESTPublisher(CloseableHttpAsyncClient client) {
        this.client = client;
        client.start();
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
            client.execute(httpPost, null);
        } catch (UnsupportedEncodingException e) {
            log.error("Error encoding message in preparation for POST: ", e);
        }
    }

//    private Consumer<Response> createResponseConsumer(String id) {
//        // Create a closure with id
//        return response -> handleResponse(id, response);
//    }
//
//    private void handleResponse(String id, Response response) {
//        if (response == null || response.getStatusCode() != RESTPubSub.OK_200) {
//            log.error("Failed to write message with id: {}. Couldn't reach pubsub server. Got response: {}", id, response);
//            return;
//        }
//        log.debug("Successfully wrote message with id {}. Response was: {} {}", id, response.getStatusCode(), response.getStatusText());
//    }
//
//    private Response handleException(Throwable throwable) {
//        log.error("Received error while posting query", throwable);
//        return null;
//    }
}
