/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.function.Consumer;

@Slf4j
public abstract class RESTPublisher implements Publisher {
    private AsyncHttpClient client;

    /**
     * Create a RESTQueryPublisher from a {@link RESTPubSubConfig} and a {@link AsyncHttpClient}.
     *
     * @param client The client.
     */
    public RESTPublisher(AsyncHttpClient client) {
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
        client.preparePost(url)
              .setBody(message.asJSON())
              .setHeader("content-type", "text/plain")
              .execute()
              .toCompletableFuture()
              .exceptionally(this::handleException)
              .thenAcceptAsync(createResponseConsumer(message.getId()));
    }

    private Consumer<Response> createResponseConsumer(String id) {
        // Create a closure with id
        return response -> handleResponse(id, response);
    }

    private void handleResponse(String id, Response response) {
        if (response == null || response.getStatusCode() != RESTPubSub.OK_200) {
            log.error("Failed to write message with id: {}. Couldn't reach pubsub server. Got response: {}", id, response);
            return;
        }
        log.debug("Successfully wrote message with id {}. Response was: {} {}", id, response.getStatusCode(), response.getStatusText());
    }

    private Response handleException(Throwable throwable) {
        log.error("Received error while posting query: {}", throwable);
        return null;
    }
}
