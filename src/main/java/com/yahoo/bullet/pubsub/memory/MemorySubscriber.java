/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.BufferingSubscriber;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;

@Slf4j
public class MemorySubscriber extends BufferingSubscriber {
    protected MemoryPubSubConfig config;
    protected AsyncHttpClient client;
    List<String> uris;

    /**
     * Create a MemorySubscriber from a {@link BulletConfig}.
     *
     * @param config The config.
     * @param maxUncommittedMessages The maximum number of records that will be buffered before commit() must be called.
     */
    public MemorySubscriber(BulletConfig config, int maxUncommittedMessages, List<String> uris, AsyncHttpClient client) {
        super(maxUncommittedMessages);
        this.config = new MemoryPubSubConfig(config);
        this.client = client;
        this.uris = uris;
    }

    @Override
    public List<PubSubMessage> getMessages() throws PubSubException {
        List<PubSubMessage> messages = new ArrayList<>();
        for (String uri : uris) {
            try {
                log.debug("Getting messages from uri: " + uri);
                Response response = client.prepareGet(uri).execute().get();
                int statusCode = response.getStatusCode();
                if (statusCode == MemoryPubSub.NO_CONTENT_204) {
                    // NO_CONTENT_204 indicates there are no new messages
                    continue;
                }
                if (statusCode != MemoryPubSub.OK_200) {
                    log.error("Http call failed with status code {} and response {}.", statusCode, response);
                    continue;
                }
                messages.add(PubSubMessage.fromJSON(response.getResponseBody()));
            } catch (Exception e) {
                log.error("Http call failed with error: " + e);
            }
        }
        return messages;
    }

    @Override
    public void close() {
    }
}
