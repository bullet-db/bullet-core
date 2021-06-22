/*
 *  Copyright 2021 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.BulletConfig;

public class IdentityPubSubMessageSerDe extends PubSubMessageSerDe {
    private static final long serialVersionUID = -1709000962888195381L;

    /**
     * Constructor.
     *
     * @param config The {@link BulletConfig} to configure this class.
     */
    public IdentityPubSubMessageSerDe(BulletConfig config) {
        super(config);
    }

    @Override
    public PubSubMessage toMessage(PubSubMessage message) {
        return message;
    }

    @Override
    public PubSubMessage fromMessage(PubSubMessage message) {
        return message;
    }
}
