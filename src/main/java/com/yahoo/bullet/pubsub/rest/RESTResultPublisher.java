/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.CloseableHttpClient;

@Slf4j
public class RESTResultPublisher extends RESTPublisher {
    /**
     * Create a RESTQueryPublisher from a {@link CloseableHttpClient}.
     *
     * @param client The client.
     */
    public RESTResultPublisher(CloseableHttpClient client) {
        super(client);
    }

    @Override
    public void send(PubSubMessage message) {
        String url = (String) message.getMetadata().getContent();
        log.debug("Extracted url to which to send results: {}", url);
        sendToURL(url, message);
    }
}
