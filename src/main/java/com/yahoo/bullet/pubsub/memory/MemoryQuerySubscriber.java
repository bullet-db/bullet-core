/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.common.BulletConfig;
import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class MemoryQuerySubscriber extends MemorySubscriber {

    public MemoryQuerySubscriber(BulletConfig config, int maxUncommittedMessages) {
        super(config, maxUncommittedMessages);
    }

    @Override
    protected List<String> getURIs() {
        return Arrays.asList(this.config.getAs(MemoryPubSubConfig.BACKED_READ_QUERY_PATHS, String.class).split(","));
    }
}
