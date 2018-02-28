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
public abstract class MemorySubscriber extends BufferingSubscriber {
    protected MemoryPubSubConfig config;
    protected AsyncHttpClient client;
    List<String> uris;

    /**
     * Create a MemorySubscriber from a {@link BulletConfig}.
     *
     * @param config The config.
     * @param maxUncommittedMessages The maximum number of records that will be buffered before commit() must be called.
     */
    public MemorySubscriber(BulletConfig config, int maxUncommittedMessages) {
        super(maxUncommittedMessages);
        this.config = new MemoryPubSubConfig(config);
        client = MemoryPubSubClientUtils.getClient(this.config);
        this.uris = getUris();
    }

    @Override
    public List<PubSubMessage> getMessages() throws PubSubException {
        List<PubSubMessage> messages = new ArrayList<>();
        for (String uri : uris) {
            try {
                Response response = client.prepareGet(uri).execute().get();
                int statusCode = response.getStatusCode();
                if (statusCode == MemoryPubSubClientUtils.NO_CONTENT_204) {
                    // SC_NO_CONTENT (204) indicates there are no new messages
                    continue;
                }
                if (statusCode != MemoryPubSubClientUtils.OK_200) {
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

    /**
     * Each subclass should implement this function to return the appropriate list of complete uris from which to read.
     * The backend can read from all in-memory pubsub hosts, the WS should just read from the single in-memory pubsub
     * instance that is running on that host.
     * @return The list of uris from which to read.
     */
    protected abstract List<String> getUris();

    @Override
    public void close() {
    }
}
