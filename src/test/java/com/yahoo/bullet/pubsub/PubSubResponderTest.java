/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class PubSubResponderTest {
    private static class TestResponder implements PubSubResponder {
        private Map<String, PubSubMessage> store = new HashMap<>();

        @Override
        public void respond(String id, PubSubMessage message) {
            store.put(id, message);
        }

        @Override
        public void close() {
            PubSubResponder.super.close();
            store.clear();
        }
    }

    @Test
    public void testResponding() {
        PubSubResponder responder = new TestResponder();
        responder.respond("id1", null);
        responder.respond("id2", new PubSubMessage("id2", ""));

        TestResponder testResponder = (TestResponder) responder;
        Assert.assertNull(testResponder.store.get("id1"));
        Assert.assertEquals(testResponder.store.get("id2"), new PubSubMessage("id2", ""));
        responder.close();
        Assert.assertTrue(testResponder.store.isEmpty());
    }
}
