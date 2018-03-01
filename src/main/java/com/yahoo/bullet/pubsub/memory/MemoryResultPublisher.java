/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemoryResultPublisher extends MemoryPublisher {

    /**
     * Create a MemoryResultPublisher from a {link BulletConfig}.
     *
     * @param config The config.
     */
    public MemoryResultPublisher(BulletConfig config) {
        super(config);
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        String uri;
        try {
            uri = (String) message.getMetadata().getContent();
            log.debug("Extracted uri to which to send results: " + uri);
        } catch (Throwable e) {
            log.error("Failed to extract uri from Metadata. Caught: " + e);
            return;
        }
        send(uri, message);
    }
}
