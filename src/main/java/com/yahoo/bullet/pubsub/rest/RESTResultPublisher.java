/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
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
     * @param connectTimeout The minimum time (ms) to wait for a connection to be made.
     */
    public RESTResultPublisher(CloseableHttpClient client, int connectTimeout) {
        super(client, connectTimeout);
    }

    @Override
    public PubSubMessage send(PubSubMessage message) {
        String url = ((RESTMetadata) message.getMetadata()).getUrl();
        log.debug("Extracted url to send results to: {}", url);
        sendToURL(url, message);
        return message;
    }
}
