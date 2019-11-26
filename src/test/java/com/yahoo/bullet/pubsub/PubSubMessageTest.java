/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.pubsub.Metadata.Signal;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;

public class PubSubMessageTest {
    private String getRandomString() {
        return UUID.randomUUID().toString();
    }

    @Test
    public void testNoMetadataCreation() {
        String messageID = getRandomString();
        String messageContent = getRandomString();

        PubSubMessage message = new PubSubMessage(messageID, messageContent);
        Assert.assertEquals(message.getId(), messageID);
        Assert.assertEquals(message.getContent(), messageContent);
        Assert.assertNull(message.getMetadata());
    }

    @Test
    public void testWithSequenceCreation() {
        String messageID = getRandomString();
        String messageContent = getRandomString();

        PubSubMessage message = new PubSubMessage(messageID, messageContent);
        Assert.assertEquals(message.getId(), messageID);
        Assert.assertEquals(message.getContent(), messageContent);
        Assert.assertNull(message.getMetadata());
    }

    @Test
    public void testWithOnlySignalCreation() {
        String messageID = getRandomString();

        PubSubMessage message = new PubSubMessage(messageID, Signal.KILL);
        Assert.assertEquals(message.getId(), messageID);
        Assert.assertNull(message.getContent());
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), Signal.KILL);
        Assert.assertNull(message.getMetadata().getContent());
    }

    @Test
    public void testWithSignalCreation() {
        String messageID = getRandomString();
        String messageContent = getRandomString();
        Signal signal = Signal.ACKNOWLEDGE;

        PubSubMessage message = new PubSubMessage(messageID, messageContent, signal);
        Assert.assertEquals(message.getId(), messageID);
        Assert.assertEquals(message.getContent(), messageContent);
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), signal);
        Assert.assertNull(message.getMetadata().getContent());
        Assert.assertNull(message.getMetadata().getContent());
    }

    @Test
    public void testWithMetadataCreation() {
        String messageID = getRandomString();
        String messageContent = getRandomString();
        String metadataContent = getRandomString();
        Signal signal = Signal.ACKNOWLEDGE;
        //Test creation without a sequence number.
        PubSubMessage message = new PubSubMessage(messageID, messageContent, new Metadata(signal, metadataContent));
        Assert.assertEquals(message.getId(), messageID);
        Assert.assertEquals(message.getContent(), messageContent);
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), signal);
        Assert.assertEquals(message.getMetadata().getContent().toString(), metadataContent);
    }

    @Test
    public void testWithMetadataAndSequenceCreation() {
        String messageID = getRandomString();
        String messageContent = getRandomString();
        String metadataContent = getRandomString();
        Signal signal = Signal.ACKNOWLEDGE;
        //Test creation with a sequence number.
        PubSubMessage message = new PubSubMessage(messageID, messageContent, new Metadata(signal, metadataContent));
        Assert.assertEquals(message.getId(), messageID);
        Assert.assertEquals(message.getContent(), messageContent);
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), signal);
        Assert.assertEquals(message.getMetadata().getContent().toString(), metadataContent);
    }

    @Test
    public void testHasContent() {
        String messageID = getRandomString();
        String messageContent = getRandomString();

        PubSubMessage message;
        message = new PubSubMessage(messageID, messageContent);
        Assert.assertTrue(message.hasContent());
        Assert.assertEquals(message.getContent(), messageContent);

        message = new PubSubMessage(messageID, (byte[]) null, Signal.COMPLETE);
        Assert.assertFalse(message.hasContent());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNoIDIllegalCreation() {
        new PubSubMessage(null, "");
    }

    @Test
    public void testEquals() {
        String messageID = getRandomString();
        String messageContent = getRandomString();
        Metadata randomMetadata = new Metadata(Signal.ACKNOWLEDGE, getRandomString());
        PubSubMessage message1 = new PubSubMessage(messageID, messageContent, randomMetadata);
        PubSubMessage message2 = new PubSubMessage(messageID, messageContent, new Metadata(Signal.ACKNOWLEDGE, getRandomString()));
        PubSubMessage message3 = new PubSubMessage(getRandomString(), messageContent, randomMetadata);

        Assert.assertEquals(message1, message2);
        Assert.assertNotEquals(message1, message3);
        Assert.assertFalse(message1.equals(null));
        Assert.assertFalse(message1.equals(new PubSubException("Dummy")));
    }

    @Test
    public void testHashCode() {
        String messageID = getRandomString();
        String messageContent = getRandomString();
        Metadata randomMetadata = new Metadata(Signal.ACKNOWLEDGE, getRandomString());
        PubSubMessage message1 = new PubSubMessage(messageID, messageContent, randomMetadata);
        PubSubMessage message2 = new PubSubMessage(messageID, messageContent, new Metadata(Signal.ACKNOWLEDGE, getRandomString()));
        Assert.assertEquals(message1.hashCode(), message2.hashCode());
    }

    @Test
    public void testHasMetadata() {
        String messageID = getRandomString();
        String messageContent = getRandomString();

        PubSubMessage message;
        message = new PubSubMessage(messageID, messageContent);
        Assert.assertFalse(message.hasMetadata());
        Assert.assertFalse(message.hasSignal());

        message = new PubSubMessage(messageID, (byte[]) null, Signal.COMPLETE);
        Assert.assertTrue(message.hasMetadata());
        Assert.assertTrue(message.hasSignal(Signal.COMPLETE));

        message = new PubSubMessage(messageID, (byte[]) null, new Metadata());
        Assert.assertTrue(message.hasMetadata());
        Assert.assertFalse(message.hasSignal());
    }

    @Test
    public void testToString() {
        String bar = "[98,97,114]";
        assertJSONEquals(new PubSubMessage("foo", "bar", new Metadata(Signal.FAIL, 42.0)).toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'FAIL', 'content': 42.0 } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", new Metadata(Signal.COMPLETE, new ArrayList<>())).toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'COMPLETE', 'content': [] } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", Signal.COMPLETE).toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'COMPLETE', 'content': null } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", Signal.ACKNOWLEDGE).toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'ACKNOWLEDGE', 'content': null } }");
        assertJSONEquals(new PubSubMessage("foo", "bar").toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': null }");
        assertJSONEquals(new PubSubMessage("foo", "bar").toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': null }");
        assertJSONEquals(new PubSubMessage().toString(),
                         "{ 'id': '', 'content': null, 'metadata': null }");
    }

    @Test
    public void testJSONConversion() {
        String bar = "[98,97,114]";
        assertJSONEquals(new PubSubMessage("foo", "bar", new Metadata(Signal.FAIL, 42.0)).asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'FAIL', 'content': 42.0 } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", new Metadata(Signal.COMPLETE, new ArrayList<>())).asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'COMPLETE', 'content': [] } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", Signal.COMPLETE).asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'COMPLETE', 'content': null } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", Signal.ACKNOWLEDGE).asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'ACKNOWLEDGE', 'content': null } }");
        assertJSONEquals(new PubSubMessage("foo", "bar").asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': null }");
        assertJSONEquals(new PubSubMessage("foo", "bar").asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': null }");
        assertJSONEquals(new PubSubMessage().asJSON(),
                         "{ 'id': '', 'content': null, 'metadata': null }");
    }

    @Test
    public void testRecreatingFromJSON() {
        String bar = "[98,97,114]";
        PubSubMessage actual = PubSubMessage.fromJSON("{ 'id': 'foo', 'content': " + bar + ", 'metadata': " +
                                                        "{ 'signal': 'FAIL', 'content': { 'type': null } } }");
        PubSubMessage expected = new PubSubMessage("foo", (String) null);
        Assert.assertEquals(actual, expected);
        Assert.assertEquals(actual.getContent(), "bar");

        Assert.assertTrue(actual.hasMetadata());
        Assert.assertTrue(actual.hasSignal());
        Assert.assertTrue(actual.hasSignal(Signal.FAIL));
        Assert.assertEquals(actual.getMetadata().getSignal(), Signal.FAIL);
        Assert.assertEquals(actual.getMetadata().getContent(), Collections.singletonMap("type", null));
    }

    @Test
    public void testRecreatingBadMessages() {
        PubSubMessage actual = PubSubMessage.fromJSON("{ }");

        Assert.assertEquals(actual.getId(), "");
        Assert.assertNull(actual.getMetadata());
        Assert.assertNull(actual.getContent());

        actual = PubSubMessage.fromJSON("{ 'metadata': { 'signal': 'ACKNOWLEDGE' } }");

        Assert.assertEquals(actual.getId(), "");
        Assert.assertTrue(actual.hasSignal());
        Assert.assertTrue(actual.hasSignal(Signal.ACKNOWLEDGE));
        Assert.assertTrue(actual.getMetadata().hasSignal(Signal.ACKNOWLEDGE));
        Assert.assertNull(actual.getMetadata().getContent());
        Assert.assertNull(actual.getContent());
    }

    @Test
    public void testSettingMetadata() {
        PubSubMessage message = new PubSubMessage("foo", Signal.KILL);
        Assert.assertEquals(message.getId(), "foo");
        Assert.assertNull(message.getContent());
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), Signal.KILL);
        Assert.assertNull(message.getMetadata().getContent());

        message.setMetadata(null);
        Assert.assertNull(message.getMetadata());

        message.setMetadata(new Metadata(Signal.ACKNOWLEDGE, "bar"));
        Assert.assertEquals(message.getMetadata().getSignal(), Signal.ACKNOWLEDGE);
        Assert.assertEquals(message.getMetadata().getContent().toString(), "bar");
    }
}
