/*
 *  Copyright 2018, Yahoo Inc.
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        this.config = new RESTPubSubConfig(config);
    }

    @Override
    public Publisher getPublisher() {
        if (context == Context.QUERY_PROCESSING) {
            return new RESTResultPublisher(config, getClient());
        } else {
            return new RESTQueryPublisher(config, getClient());
        }
    }

    @Override
    public List<Publisher> getPublishers(int n) {
        return IntStream.range(0, n).mapToObj(i -> getPublisher()).collect(Collectors.toList());
    }

    @Override
    public Subscriber getSubscriber() {
        int maxUncommittedMessages = config.getAs(RESTPubSubConfig.MAX_UNCOMMITTED_MESSAGES, Number.class).intValue();
        RESTPubSubConfig pubsubConfig = new RESTPubSubConfig(config);
        AsyncHttpClient client = getClient();

        if (context == Context.QUERY_PROCESSING) {
            List<String> urls = (List<String>) this.config.getAs(RESTPubSubConfig.QUERY_URLS, List.class);
            Long minWait = this.config.getAs(RESTPubSubConfig.QUERY_SUBSCRIBER_MIN_WAIT, Long.class);
            return new RESTSubscriber(pubsubConfig, maxUncommittedMessages, urls, client, minWait);
        } else {
            List<String> url = Collections.singletonList(this.config.getAs(RESTPubSubConfig.RESULT_URL, String.class));
            Long minWait = this.config.getAs(RESTPubSubConfig.RESULT_SUBSCRIBER_MIN_WAIT, Long.class);
            return new RESTSubscriber(pubsubConfig, maxUncommittedMessages, url, client, minWait);
        }
    }

    @Override
    public List<Subscriber> getSubscribers(int n) {
        return IntStream.range(0, n).mapToObj(i -> getSubscriber()).collect(Collectors.toList());
    }

    private AsyncHttpClient getClient() {
        int connectTimeout = config.getAs(RESTPubSubConfig.CONNECT_TIMEOUT_MS, Number.class).intValue();
        int retryLimit = config.getAs(RESTPubSubConfig.CONNECT_RETRY_LIMIT, Number.class).intValue();
        AsyncHttpClientConfig clientConfig = new DefaultAsyncHttpClientConfig.Builder().setConnectTimeout(connectTimeout)
                                                                                       .setMaxRequestRetry(retryLimit)
                                                                                       .setReadTimeout(NO_TIMEOUT)
                                                                                       .setRequestTimeout(NO_TIMEOUT)
                                                                                       .build();
        return new DefaultAsyncHttpClient(clientConfig);
    }
}
