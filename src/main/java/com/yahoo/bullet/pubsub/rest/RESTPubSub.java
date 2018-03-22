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
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
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
            return new RESTResultPublisher(getClient());
        } else {
            String queryURL = ((List<String>) config.getAs(RESTPubSubConfig.QUERY_URLS, List.class)).get(0);
            String resultURL = config.getAs(RESTPubSubConfig.RESULT_URL, String.class);
            return new RESTQueryPublisher(getClient(), queryURL, resultURL);
        }
    }

    @Override
    public List<Publisher> getPublishers(int n) {
        return IntStream.range(0, n).mapToObj(i -> getPublisher()).collect(Collectors.toList());
    }

    @Override
    public Subscriber getSubscriber() {
        int maxUncommittedMessages = config.getAs(RESTPubSubConfig.MAX_UNCOMMITTED_MESSAGES, Integer.class);
        CloseableHttpAsyncClient client = getClient();
        List<String> urls;
        Long minWait;

        if (context == Context.QUERY_PROCESSING) {
            urls = (List<String>) config.getAs(RESTPubSubConfig.QUERY_URLS, List.class);
            minWait = config.getAs(RESTPubSubConfig.QUERY_SUBSCRIBER_MIN_WAIT, Long.class);
        } else {
            urls = Collections.singletonList(config.getAs(RESTPubSubConfig.RESULT_URL, String.class));
            minWait = config.getAs(RESTPubSubConfig.RESULT_SUBSCRIBER_MIN_WAIT, Long.class);
        }
        return new RESTSubscriber(maxUncommittedMessages, urls, client, minWait);
    }

    @Override
    public List<Subscriber> getSubscribers(int n) {
        return IntStream.range(0, n).mapToObj(i -> getSubscriber()).collect(Collectors.toList());
    }

    private CloseableHttpAsyncClient getClient() {
        return HttpAsyncClients.createDefault();
//        Long connectTimeout = config.getAs(RESTPubSubConfig.CONNECT_TIMEOUT, Long.class);
//        int retryLimit = config.getAs(RESTPubSubConfig.CONNECT_RETRY_LIMIT, Integer.class);
//        AsyncHttpClientConfig clientConfig =
//                new DefaultAsyncHttpClientConfig.Builder().setConnectTimeout(connectTimeout.intValue())
//                                                          .setMaxRequestRetry(retryLimit)
//                                                          .setReadTimeout(NO_TIMEOUT)
//                                                          .setRequestTimeout(NO_TIMEOUT)
//                                                          .build();
//        return new DefaultAsyncHttpClient(clientConfig);
    }
}
