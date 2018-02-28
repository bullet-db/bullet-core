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
public class MemoryResponsePublisher extends MemoryPublisher {

    /**
     * Create a MemoryResponsePublisher from a {link BulletConfig}.
     *
     * @param config The config.
     */
    public MemoryResponsePublisher(BulletConfig config) {
        super(config);
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        String uri;
        try {
            uri = (String) message.getMetadata().getContent();
        } catch (Throwable e) {
            log.error("Failed to extract uri from Metadata. Caught: " + e);
            return;
        }
        send(uri, message);
    }
}
