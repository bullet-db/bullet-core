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
import org.asynchttpclient.AsyncHttpClient;

@Slf4j
public class MemoryResultPublisher extends MemoryPublisher {

    /**
     * Create a MemoryQueryPublisher from a {@link BulletConfig} and a {@link AsyncHttpClient}.
     *
     * @param config The config.
     * @param client The client.
     */
    public MemoryResultPublisher(BulletConfig config, AsyncHttpClient client) {
        super(new MemoryPubSubConfig(config), client);
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        String uri;
        uri = (String) message.getMetadata().getContent();
        log.debug("Extracted uri to which to send results: " + uri);
        send(uri, message);
    }
}
