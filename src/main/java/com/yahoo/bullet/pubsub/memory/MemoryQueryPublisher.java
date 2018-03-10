/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;

import java.util.List;

@Slf4j
public class MemoryQueryPublisher extends MemoryPublisher {
    String queryURI;
    String resultURI;

    /**
     * Create a MemoryQueryPublisher from a {@link BulletConfig} and a {@link AsyncHttpClient}.
     *
     * @param config The config.
     * @param client The client.
     */
    public MemoryQueryPublisher(BulletConfig config, AsyncHttpClient client) {
        super(new MemoryPubSubConfig(config), client);
        this.queryURI = ((List<String>) this.config.getAs(MemoryPubSubConfig.QUERY_URIS, List.class)).get(0);
        this.resultURI = this.config.getAs(MemoryPubSubConfig.RESULT_URI, String.class);
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
