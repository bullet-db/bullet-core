/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.common.BulletConfig;
import lombok.extern.slf4j.Slf4j;
import java.util.List;

@Slf4j
public class MemoryQuerySubscriber extends MemorySubscriber {

    /**
     * Create a MemoryQuerySubscriber from a {@link BulletConfig}.
     *
     * @param config The config.
     * @param maxUncommittedMessages The maximum number of messages that will be buffered before a commit() must be called.
     */
    public MemoryQuerySubscriber(BulletConfig config, int maxUncommittedMessages) {
        super(config, maxUncommittedMessages);
    }

    @Override
    protected List<String> getUris() {
        return (List<String>) this.config.getAs(MemoryPubSubConfig.BACKED_QUERY_PATHS, List.class);
    }
}
