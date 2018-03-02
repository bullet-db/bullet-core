/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public class MemoryPubSub extends PubSub {

    /**
     * Create a MemoryPubSub from a {@link BulletConfig}.
     *
     * @param config The config.
     * @throws PubSubException
     */
    public MemoryPubSub(BulletConfig config) throws PubSubException {
        super(config);
        this.config = new MemoryPubSubConfig(config);
    }

    @Override
    public Publisher getPublisher() throws PubSubException {
        if (context == Context.QUERY_PROCESSING) {
            return new MemoryResultPublisher(config);
        } else {
            return new MemoryQueryPublisher(config);
        }
    }

    @Override
    public List<Publisher> getPublishers(int n) throws PubSubException {
        return Collections.nCopies(n, getPublisher());
    }

    @Override
    public Subscriber getSubscriber() throws PubSubException {
        int maxUncommittedMessages = config.getAs(MemoryPubSubConfig.MAX_UNCOMMITTED_MESSAGES, Number.class).intValue();
        if (context == Context.QUERY_PROCESSING) {
            List<String> uris = (List<String>) this.config.getAs(MemoryPubSubConfig.QUERY_URIS, List.class);
            return new MemorySubscriber(config, maxUncommittedMessages, uris);
        } else {
            List<String> uri = Collections.singletonList(this.config.getAs(MemoryPubSubConfig.RESULT_URI, String.class));
            return new MemorySubscriber(config, maxUncommittedMessages, uri);
        }
    }

    @Override
    public List<Subscriber> getSubscribers(int n) throws PubSubException {
        return Collections.nCopies(n, getSubscriber());
    }

}
