/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.CloseableHttpClient;

@Slf4j
public class RESTQueryPublisher extends RESTPublisher {
    @Getter(AccessLevel.PACKAGE)
    private String queryURL;
    private String resultURL;

    /**
     * Create a RESTQueryPublisher from a {@link CloseableHttpClient}, queryURL and resultURL. The BulletConfig must
     * contain a valid url in the bullet.pubsub.rest.query.urls field.
     *
     * @param client The client.
     * @param queryURL The URL to which to POST queries.
     * @param resultURL The URL that will be added to the Metadata (results will be sent to this URL from the backend).
     * @param connectTimeout The minimum time (ms) to wait for a connection to be made.
     */
    public RESTQueryPublisher(CloseableHttpClient client, String queryURL, String resultURL, int connectTimeout) {
        super(client, connectTimeout);
        this.queryURL = queryURL;
        this.resultURL = resultURL;
    }

    @Override
    public PubSubMessage send(PubSubMessage message) {
        // Put resultURL in the metadata so the ResponsePublisher knows to which host to send the response
        Metadata metadata = message.getMetadata();
        metadata = metadata == null ? new RESTMetadata(resultURL) : new RESTMetadata(resultURL, metadata);
        message.setMetadata(metadata);
        sendToURL(queryURL, message);
        return message;
    }
}
