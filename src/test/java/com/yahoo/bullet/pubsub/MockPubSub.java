/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.BulletConfig;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.Mockito.doReturn;

class MockPubSub extends PubSub {
    public static final String MOCK_MESSAGE_NAME = "MOCK_MESSAGE";
    private String mockMessage;

    public MockPubSub(BulletConfig config) throws PubSubException {
        super(config);
        mockMessage = config.get(MOCK_MESSAGE_NAME).toString();
    }

    @Override
    public Subscriber getSubscriber() {
        Subscriber mockSubscriber = Mockito.mock(Subscriber.class);
        try {
            doReturn(new PubSubMessage("", mockMessage)).when(mockSubscriber).receive();
        } catch (Exception e) {
            mockSubscriber = null;
        }
        return mockSubscriber;
    }

    @Override
    public Publisher getPublisher() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Subscriber> getSubscribers(int n) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Publisher> getPublishers(int n) {
        throw new UnsupportedOperationException();
    }
}
