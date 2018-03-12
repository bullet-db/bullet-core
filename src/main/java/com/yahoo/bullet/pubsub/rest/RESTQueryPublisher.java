/*
 *  Copyright 2017, Yahoo Inc.
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
    String queryURI;
    String resultURI;

    /**
     * Create a RESTQueryPublisher from a {@link BulletConfig} and a {@link AsyncHttpClient}.
     *
     * @param config The config.
     * @param client The client.
     */
    public RESTQueryPublisher(BulletConfig config, AsyncHttpClient client) {
        super(new RESTPubSubConfig(config), client);
        this.queryURI = ((List<String>) this.config.getAs(RESTPubSubConfig.QUERY_URIS, List.class)).get(0);
        this.resultURI = this.config.getAs(RESTPubSubConfig.RESULT_URI, String.class);
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        // Put responseURI in the metadata so the ResponsePublisher knows to which host to send the response
        Metadata metadata = message.getMetadata();
        metadata = metadata == null ? new Metadata() : metadata;
        metadata.setContent(resultURI);
        PubSubMessage newMessage = new PubSubMessage(message.getId(), message.getContent(), metadata, message.getSequence());
        sendToURI(queryURI, newMessage);
    }
}
