/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.BulletConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * This can be extended by any class that needs to respond to a {@link PubSubMessage}.
 */
@Slf4j
public abstract class PubSubResponder implements AutoCloseable {
    @Getter
    protected BulletConfig config;

    /**
     * Constructor.
     *
     * @param config The {@link BulletConfig} to use.
     */
    public PubSubResponder(BulletConfig config) {
        this.config = config;
    }

    /**
     * Respond to a {@link PubSubMessage}.
     *
     * @param id The id of the response.
     * @param message The actual {@link PubSubMessage} containing the response.
     */
    public abstract void respond(String id, PubSubMessage message);

    @Override
    public void close() {
    }
}
