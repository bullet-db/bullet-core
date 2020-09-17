/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.Metadata;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class RESTMetadata extends Metadata {
    private static final long serialVersionUID = 5718947090573796171L;
    @Getter
    private final String url;

    /**
     * Only for use within the RESTPubSub.
     *
     * @param url The URL to use to identify where to send messages to.
     * @param metadata An instance of a {@link Metadata} to wrap.
     */
    RESTMetadata(String url, Metadata metadata) {
        super(metadata.getSignal(), metadata.getContent());
        this.url = url;
    }

    @Override
    public Metadata copy() {
        return new RESTMetadata(url, this);
    }
}
