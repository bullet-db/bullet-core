/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

public class MemoryPubSubClientUtils {
    public static final int NO_TIMEOUT = -1;
    public static final int OK_200 = 200;
    public static final int NO_CONTENT_204 = 204;

    /**
     * Get a {@link AsyncHttpClient} configured with the given config.
     *
     * @param config The config.
     * @return The client.
     */
    public static AsyncHttpClient getClient(MemoryPubSubConfig config) {
        int connectTimeout = config.getAs(MemoryPubSubConfig.CONNECT_TIMEOUT_MS, Number.class).intValue();
        int retryLimit = config.getAs(MemoryPubSubConfig.CONNECT_RETRY_LIMIT, Number.class).intValue();
        AsyncHttpClientConfig clientConfig = new DefaultAsyncHttpClientConfig.Builder().setConnectTimeout(connectTimeout)
                                                                                       .setMaxRequestRetry(retryLimit)
                                                                                       .setReadTimeout(NO_TIMEOUT)
                                                                                       .setRequestTimeout(NO_TIMEOUT)
                                                                                       .build();
        return new DefaultAsyncHttpClient(clientConfig);
    }
}
