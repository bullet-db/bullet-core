/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.SerializerDeserializer;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BufferingSubscriberTest {
    @Getter
    private final static class ExampleBufferingSubscriber extends BufferingSubscriber {
        private List<PubSubMessage> testMessages;
        private int callCount = 0;

        public ExampleBufferingSubscriber(int max, List<PubSubMessage> messages) {
            super(max);
            testMessages = new LinkedList<>();
            add(messages);
        }

        public ExampleBufferingSubscriber(int max, int rateLimitMax, long rateLimitInterval, List<PubSubMessage> messages) {
            super(max, rateLimitMax, rateLimitInterval);
            testMessages = new LinkedList<>();
            add(messages);
        }

        public void add(List<PubSubMessage> messages) {
            testMessages.addAll(messages);
        }

        @Override
        public List<PubSubMessage> getMessages() {
            callCount++;
            if (testMessages == null || testMessages.isEmpty()) {
                return testMessages;
            }
            return Collections.singletonList(testMessages.remove(0));
        }

        @Override
        public void close() {
            testMessages.clear();
        }
    }

    private static List<PubSubMessage> make(int n) {
        return IntStream.range(0, n).mapToObj(i -> new PubSubMessage(String.valueOf(i), SerializerDeserializer.toBytes(UUID.randomUUID())))
                                    .collect(Collectors.toList());
    }

    @Test
    public void testMaxUncommittedMessages() throws PubSubException {
        List<PubSubMessage> messages = make(1);
        ExampleBufferingSubscriber subscriber = new ExampleBufferingSubscriber(1, messages);

        // Multiple receives without a commit.
        Assert.assertNotNull(subscriber.receive());
        Assert.assertNull(subscriber.receive());
    }

    @Test
    public void testRateLimit() throws PubSubException, InterruptedException {
        List<PubSubMessage> messages = make(20);
        ExampleBufferingSubscriber subscriber = new ExampleBufferingSubscriber(100, 5, 10L, messages);
        for (int i = 0; i < 5; i++) {
            Assert.assertNotNull(subscriber.receive());
        }
        Assert.assertNull(subscriber.receive());
        Assert.assertEquals(subscriber.getCallCount(), 5);

        // Sleep to reset interval
        Thread.sleep(15);

        for (int i = 0; i < 5; i++) {
            Assert.assertNotNull(subscriber.receive());
        }
        Assert.assertNull(subscriber.receive());
        Assert.assertEquals(subscriber.getCallCount(), 10);
    }

    @Test
    public void testNoMoreMessages() throws PubSubException {
        List<PubSubMessage> messages = make(5);
        ExampleBufferingSubscriber subscriber = new ExampleBufferingSubscriber(100, messages);
        for (int i = 0; i < 5; ++i) {
            Assert.assertNotNull(subscriber.receive());
            subscriber.commit(String.valueOf(i));
        }
        // Null test
        Assert.assertNull(subscriber.receive());
        // Makes getMessages return an empty list
        subscriber.close();
        // Empty test
        Assert.assertNull(subscriber.receive());

        subscriber.add(Collections.singletonList(new PubSubMessage("foo", "bar")));
        PubSubMessage actual = subscriber.receive();
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getId(), "foo");
        Assert.assertEquals(actual.getContent(), "bar");
        Assert.assertEquals(actual.getContentAsString(), "bar");

        Assert.assertEquals(subscriber.getCallCount(), 8);
    }

    @Test
    public void testCommitting() throws PubSubException {
        List<PubSubMessage> messages = make(5);
        ExampleBufferingSubscriber subscriber = new ExampleBufferingSubscriber(3, messages);

        Assert.assertNotNull(subscriber.receive());
        Assert.assertNotNull(subscriber.receive());
        Assert.assertNotNull(subscriber.receive());
        Assert.assertNull(subscriber.receive());

        subscriber.commit(String.valueOf(0));
        subscriber.commit(String.valueOf(1));
        subscriber.commit(String.valueOf(2));

        // (3, -1) is uncommited
        PubSubMessage actual = subscriber.receive();
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getId(), String.valueOf(3));
        Assert.assertNotNull(actual.getContent());
    }

    @Test
    public void testCommittingUnknownMessage() throws PubSubException {
        List<PubSubMessage> messages = make(2);
        ExampleBufferingSubscriber subscriber = new ExampleBufferingSubscriber(1, messages);

        Assert.assertNotNull(subscriber.receive());

        Assert.assertNull(subscriber.receive());

        // This doesn't exist
        subscriber.commit(String.valueOf(42));
        Assert.assertNull(subscriber.receive());

        subscriber.commit(String.valueOf(0));

        PubSubMessage actual = subscriber.receive();
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getId(), String.valueOf(1));
        Assert.assertNotNull(actual.getContent());
    }

    @Test
    public void testFailing() throws PubSubException {
        List<PubSubMessage> messages = make(5);
        ExampleBufferingSubscriber subscriber = new ExampleBufferingSubscriber(3, messages);

        Assert.assertNotNull(subscriber.receive());
        PubSubMessage actualFirstTime = subscriber.receive();
        Assert.assertNotNull(actualFirstTime);
        Assert.assertNotNull(subscriber.receive());

        Assert.assertNull(subscriber.receive());

        // Commit 0 and 2 and fail 1
        subscriber.commit(String.valueOf(0));
        subscriber.commit(String.valueOf(2));
        Assert.assertTrue(subscriber.uncommittedMessages.containsKey(String.valueOf(1)));
        subscriber.fail(String.valueOf(1));
        Assert.assertFalse(subscriber.uncommittedMessages.containsKey(String.valueOf(1)));

        PubSubMessage actualSecondTime = subscriber.receive();
        Assert.assertNotNull(actualSecondTime);
        Assert.assertEquals(actualFirstTime.getId(), String.valueOf(1));
        Assert.assertNotNull(actualFirstTime.getContent());

        Assert.assertEquals(actualFirstTime, actualSecondTime);
        Assert.assertEquals(actualFirstTime.getContent(), actualSecondTime.getContent());
    }

    @Test
    public void testFailingUnknownMessage() throws PubSubException {
        List<PubSubMessage> messages = make(2);
        ExampleBufferingSubscriber subscriber = new ExampleBufferingSubscriber(1, messages);

        Assert.assertNotNull(subscriber.receive());

        Assert.assertNull(subscriber.receive());

        subscriber.commit(String.valueOf(0));
        // This doesn't exist
        subscriber.fail(String.valueOf(42));
        Assert.assertFalse(subscriber.uncommittedMessages.containsKey(ImmutablePair.of(String.valueOf(42), -1)));

        PubSubMessage actual = subscriber.receive();
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getId(), String.valueOf(1));
        Assert.assertNotNull(actual.getContent());
    }
}
