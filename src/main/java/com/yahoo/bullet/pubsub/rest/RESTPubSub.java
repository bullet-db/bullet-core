/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

import java.util.Collections;
import java.util.List;

@Slf4j
public class RESTPubSub extends PubSub {
    private static final int NO_TIMEOUT = -1;
    public static final int OK_200 = 200;
    public static final int NO_CONTENT_204 = 204;

    /**
     * Create a RESTPubSub from a {@link BulletConfig}.
     *
     * @param config The config.
     * @throws PubSubException
     */
    public RESTPubSub(BulletConfig config) throws PubSubException {
        super(config);
        this.config = new MemoryPubSubConfig(config);
    }

    @Override
    public Publisher getPublisher() throws PubSubException {
        if (context == Context.QUERY_PROCESSING) {
            return new RESTResultPublisher(config, getClient());
        } else {
            return new RESTQueryPublisher(config, getClient());
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
            Long minWait = this.config.getAs(MemoryPubSubConfig.QUERY_MIN_WAIT, Long.class);
            return new MemorySubscriber(new MemoryPubSubConfig(config), maxUncommittedMessages, uris, getClient(), minWait);
        } else {
            List<String> uri = Collections.singletonList(this.config.getAs(MemoryPubSubConfig.RESULT_URI, String.class));
            Long minWait = this.config.getAs(MemoryPubSubConfig.RESULT_MIN_WAIT, Long.class);
            return new MemorySubscriber(new MemoryPubSubConfig(config), maxUncommittedMessages, uri, getClient(), minWait);
        }
    }

    @Override
    public List<Subscriber> getSubscribers(int n) throws PubSubException {
        return Collections.nCopies(n, getSubscriber());
    }

    private AsyncHttpClient getClient() {
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
