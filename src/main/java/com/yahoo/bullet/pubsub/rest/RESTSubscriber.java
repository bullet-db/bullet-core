/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.BufferingSubscriber;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

@Slf4j
public class RESTSubscriber extends BufferingSubscriber {
    @Getter(AccessLevel.PACKAGE)
    private List<String> urls;
    private CloseableHttpAsyncClient client;
    private long minWait;
    private long lastRequest;

    /**
     * Create a RESTSubscriber.
     *
     * @param maxUncommittedMessages The maximum number of records that will be buffered before commit() must be called.
     * @param urls The URLs which will be used to make the http request.
     * @param client The client to use to make http requests.
     * @param minWait The minimum time (ms) to wait between subsequent http requests.
     */
    public RESTSubscriber(int maxUncommittedMessages, List<String> urls, CloseableHttpAsyncClient client, long minWait) {
        super(maxUncommittedMessages);
        this.client = client;
        this.client.start();
        this.urls = urls;
        this.minWait = minWait;
        this.lastRequest = 0;
    }

    @Override
    public List<PubSubMessage> getMessages() throws PubSubException {
        List<PubSubMessage> messages = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastRequest <= minWait) {
            return messages;
        }
        lastRequest = currentTime;
        for (String url : urls) {
            try {
                log.debug("Getting messages from url: {}", url);
                HttpGet httpGet = new HttpGet(url);
                HttpResponse response = client.execute(httpGet, null).get();
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == RESTPubSub.OK_200) {
                    String message = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
                    messages.add(PubSubMessage.fromJSON(message));
                } else if (statusCode != RESTPubSub.NO_CONTENT_204) {
                    // NO_CONTENT_204 indicates there are no new messages - anything else indicates a problem
                    log.error("Http call failed with status code {} and response {}.", statusCode, response);
                }
            } catch (Exception e) {
                log.error("Http call failed with error: ", e);
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
}
