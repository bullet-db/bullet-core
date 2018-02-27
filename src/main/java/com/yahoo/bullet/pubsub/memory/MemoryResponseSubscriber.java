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

    public MemoryResponseSubscriber(BulletConfig config, int maxUncommittedMessages) {
        super(config, maxUncommittedMessages);
    }

    @Override
    protected List<String> getURIs() {
        String server = this.config.getAs(MemoryPubSubConfig.SERVER, String.class);
        String contextPath = this.config.getAs(MemoryPubSubConfig.CONTEXT_PATH, String.class);
        String path = this.config.getAs(MemoryPubSubConfig.READ_RESPONSE_PATH, String.class);
        return Collections.singletonList(server + contextPath + path);
    }
}
