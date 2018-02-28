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
    String writeURI;
    String respondURI;

    /**
     * Create a MemroyQueryPublisher from a {@link BulletConfig}.
     *
     * @param config The config.
     */
    public MemoryQueryPublisher(BulletConfig config) {
        super(config);
        this.writeURI = createWriteURI();
        this.respondURI = createRespondURI();
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        // Put responseURI in the metadata so the ResponsePublisher knows to which host to send the response
        Metadata metadata = new Metadata(message.getMetadata().getSignal(), respondURI);
        PubSubMessage newMessage = new PubSubMessage(message.getId(), message.getContent(), metadata, message.getSequence());
        send(writeURI, newMessage);
    }

    private String createWriteURI() {
        return getHostPath() + this.config.getAs(MemoryPubSubConfig.WRITE_QUERY_PATH, String.class);
    }

    private String createRespondURI() {
        return getHostPath() + this.config.getAs(MemoryPubSubConfig.WRITE_RESPONSE_PATH, String.class);
    }

    private String getHostPath() {
        String server = this.config.getAs(MemoryPubSubConfig.SERVER, String.class);
        String contextPath = this.config.getAs(MemoryPubSubConfig.CONTEXT_PATH, String.class);
        return server + contextPath;
    }
}
