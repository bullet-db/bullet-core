/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class PubSubResponderTest {
    private static class TestResponder extends PubSubResponder {
        private Map<String, PubSubMessage> store = new HashMap<>();

        TestResponder() {
            super(null);
        }

        @Override
        public void respond(String id, PubSubMessage message) {
            store.put(id, message);
        }

        @Override
        public void close() {
            super.close();
            store.clear();
        }
    }

    @Test
    public void testResponding() {
        PubSubResponder responder = new TestResponder();
        responder.respond("id1", null);
        responder.respond("id2", new PubSubMessage("id2", new byte[0]));

        TestResponder testResponder = (TestResponder) responder;
        Assert.assertNull(testResponder.store.get("id1"));
        Assert.assertEquals(testResponder.store.get("id2"), new PubSubMessage("id2", new byte[0]));
        responder.close();
        Assert.assertTrue(testResponder.store.isEmpty());
    }
}
