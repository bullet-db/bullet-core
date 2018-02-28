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
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MemorySubscriber extends BufferingSubscriber {
    MemoryPubSubConfig config;
    List<String> uris;
    HttpClient client;

    /**
     * Create a MemorySubscriber from a {@link BulletConfig}.
     *
     * @param config The config.
     * @param maxUncommittedMessages The maximum number of records that will be buffered before commit() must be called.
     */
    public MemorySubscriber(BulletConfig config, int maxUncommittedMessages) {
        super(maxUncommittedMessages);
        this.config = new MemoryPubSubConfig(config);

        this.uris = getUris();
        this.client = HttpClients.createDefault();
    }

    @Override
    public List<PubSubMessage> getMessages() throws PubSubException {
        List<PubSubMessage> messages = new ArrayList<>();
        for (String uri : uris) {
            try {
                HttpGet get = new HttpGet(uri);
                HttpResponse response = client.execute(get);
                int statusCode = response.getStatusLine().getStatusCode();

                if (statusCode == HttpStatus.SC_NO_CONTENT) {
                    // SC_NO_CONTENT (204) indicates there are no new messages
                    continue;
                }
                if (statusCode != HttpStatus.SC_OK) {
                    log.error("Http call failed with status code {} and response {}.", statusCode, response);
                    continue;
                }
                messages.add(PubSubMessage.fromJSON(readResponseContent(response)));
            } catch (Exception e) {
                log.error("Http call failed with error: " + e);
            }
        }
        return messages;
    }

    private String readResponseContent(HttpResponse response) throws UnsupportedOperationException, IOException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
        StringBuffer result = new StringBuffer();
        String line = null;
        while ((line = rd.readLine()) != null) {
            result.append(line);
            result.append('\n');
        }
        return result.toString();
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
