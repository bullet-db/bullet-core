/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.BulletConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Use this as a {@link PubSubResponder} if async results should be sent to a {@link PubSub} interface. This simply
 * creates an instance of a {@link PubSub} from your provided {@link BulletConfig} and tries to create a
 * {@link Publisher} from it to respond with results to.
 */
@Slf4j
public class BulletPubSubResponder extends PubSubResponder {
    protected PubSub pubSub;
    protected Publisher publisher;

    /**
     * Constructor.
     *
     * @param config The {@link BulletConfig} to use.
     */
    public BulletPubSubResponder(BulletConfig config) {
        super(config);
        try {
            pubSub = PubSub.from(config);
            publisher = pubSub.getPublisher();
        } catch (Exception e) {
            throw new RuntimeException("Unable to create a PubSub instance or a Publisher from it", e);
        }
    }

    @Override
    public void respond(String id, PubSubMessage message) {
        log.debug("Responding with message {}", id);
        log.trace("Responding to {} with payload {}", id, message);
        try {
            publisher.send(message);
        } catch (PubSubException e) {
            log.error("Unable to publish message. Ignoring {}: {}", id, message);
            log.error("Error", e);
        }
    }

    @Override
    public void close() {
        try {
            publisher.close();
        } catch (Exception e) {
            log.error("Unable to close the publisher", e);
        }
    }
}
