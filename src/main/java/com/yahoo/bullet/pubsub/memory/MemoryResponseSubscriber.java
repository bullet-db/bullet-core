/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.common.BulletConfig;
import lombok.extern.slf4j.Slf4j;
import java.util.Collections;
import java.util.List;

@Slf4j
public class MemoryResponseSubscriber extends MemorySubscriber {

    /**
     * Create a MemoryResponseSubscriber from a {@link BulletConfig}.
     *
     * @param config The config.
     * @param maxUncommittedMessages The maximum number of records that will be buffered before commit must be called.
     */
    public MemoryResponseSubscriber(BulletConfig config, int maxUncommittedMessages) {
        super(config, maxUncommittedMessages);
    }

    @Override
    protected List<String> getUris() {
        String server = this.config.getAs(MemoryPubSubConfig.SERVER, String.class);
        String contextPath = this.config.getAs(MemoryPubSubConfig.CONTEXT_PATH, String.class);
        String path = this.config.getAs(MemoryPubSubConfig.RESULT_PATH, String.class);
        return Collections.singletonList(server + contextPath + path);
    }
}
