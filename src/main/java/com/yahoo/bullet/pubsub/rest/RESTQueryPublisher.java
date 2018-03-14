/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;

import java.util.List;

@Slf4j
public class RESTQueryPublisher extends RESTPublisher {
    String queryURL;
    String resultURL;

    /**
     * Create a RESTQueryPublisher from a {@link BulletConfig} and a {@link AsyncHttpClient}. The BulletConfig must
     * contain a valid url in the bullet.pubsub.rest.query.urls field.
     *
     * @param config The config.
     * @param client The client.
     */
    public RESTQueryPublisher(BulletConfig config, AsyncHttpClient client) {
        super(new RESTPubSubConfig(config), client);
        this.queryURL = ((List<String>) this.config.getAs(RESTPubSubConfig.QUERY_URLS, List.class)).get(0);
        this.resultURL = this.config.getAs(RESTPubSubConfig.RESULT_URL, String.class);
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        // Put responseURL in the metadata so the ResponsePublisher knows to which host to send the response
        Metadata metadata = message.getMetadata();
        metadata = metadata == null ? new Metadata() : metadata;
        metadata.setContent(resultURL);
        PubSubMessage newMessage = new PubSubMessage(message.getId(), message.getContent(), metadata, message.getSequence());
        sendToURL(queryURL, newMessage);
    }
}
