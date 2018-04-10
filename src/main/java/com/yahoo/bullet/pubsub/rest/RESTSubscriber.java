/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.BufferingSubscriber;
import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RESTSubscriber extends BufferingSubscriber {
    @Getter(AccessLevel.PACKAGE)
    private List<String> urls;
    private CloseableHttpClient client;
    private long minWait;
    @Setter(AccessLevel.PACKAGE)
    private long lastRequest;
    private int connectTimeout;

    /**
     * Create a RESTSubscriber.
     *
     * @param maxUncommittedMessages The maximum number of records that will be buffered before commit() must be called.
     * @param urls The URLs which will be used to make the http request.
     * @param client The client to use to make http requests.
     * @param minWait The minimum time (ms) to wait between subsequent http requests.
     * @param connectTimeout The minimum time (ms) to wait for a connection to be made.
     */
    public RESTSubscriber(int maxUncommittedMessages, List<String> urls, CloseableHttpClient client, long minWait, int connectTimeout) {
        super(maxUncommittedMessages);
        this.client = client;
        this.urls = urls;
        this.minWait = minWait;
        this.lastRequest = 0;
        this.connectTimeout = connectTimeout;
    }

    @Override
    public List<PubSubMessage> getMessages() {
        List<PubSubMessage> messages = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastRequest <= minWait) {
            return messages;
        }
        lastRequest = currentTime;
        for (String url : urls) {
            try {
                log.debug("Getting messages from url: {}", url);
                HttpResponse response = client.execute(makeHttpGet(url));
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == RESTPubSub.OK_200) {
                    String message = EntityUtils.toString(response.getEntity(), RESTPubSub.UTF_8);
                    messages.add(PubSubMessage.fromJSON(message));
                } else if (statusCode != RESTPubSub.NO_CONTENT_204) {
                    // NO_CONTENT_204 indicates there are no new messages - anything else indicates a problem
                    log.error("Http call failed with status code {} and response {}.", statusCode, response);
                }
            } catch (Exception e) {
                log.error("Http call to {} failed with error: {}", url, e);
            }
        }
        return messages;
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            log.warn("Caught exception when closing AsyncHttpClient: ", e);
        }
    }

    private HttpGet makeHttpGet(String url) {
        HttpGet httpGet = new HttpGet(url);
        RequestConfig requestConfig =
                RequestConfig.custom().setConnectTimeout(connectTimeout)
                                      .setSocketTimeout(connectTimeout)
                                      .build();
        httpGet.setConfig(requestConfig);
        return httpGet;
    }
}
