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

@Slf4j
public class MemoryQueryPublisher extends MemoryPublisher {
    String queryURI;
    String resultURI;

    /**
     * Create a MemoryQueryPublisher from a {@link BulletConfig}.
     *
     * @param config The config.
     */
    public MemoryQueryPublisher(BulletConfig config) {
        super(config);
        this.queryURI = this.config.getAs(MemoryPubSubConfig.QUERY_URI, String.class);
        this.resultURI = this.config.getAs(MemoryPubSubConfig.RESULT_URI, String.class);
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        // Put responseURI in the metadata so the ResponsePublisher knows to which host to send the response
        Metadata metadata = new Metadata(message.getMetadata().getSignal(), resultURI);
        PubSubMessage newMessage = new PubSubMessage(message.getId(), message.getContent(), metadata, message.getSequence());
        send(queryURI, newMessage);
    }
}
