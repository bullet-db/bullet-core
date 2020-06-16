/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.Metadata.Signal;
import com.yahoo.bullet.result.Meta;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;

public class PubSubMessageTest {
    private static final byte[] CONTENT = "bar".getBytes(PubSubMessage.CHARSET);

    private String getRandomString() {
        return UUID.randomUUID().toString();
    }

    private byte[] getRandomBytes() {
        return SerializerDeserializer.toBytes(UUID.randomUUID());
    }

    @Test
    public void testNoMetadataCreation() {
        String messageID = getRandomString();
        byte[] messageContent = getRandomBytes();

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
        Assert.assertNull(message.getContentAsString());
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), Signal.KILL);
        Assert.assertNull(message.getMetadata().getContent());
    }

    @Test
    public void testWithSignalCreation() {
        String messageID = getRandomString();
        byte[] messageContent = getRandomBytes();
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
        byte[] messageContent = getRandomBytes();
        String metadataContent = getRandomString();
        Signal signal = Signal.ACKNOWLEDGE;

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
        byte[] messageContent = getRandomBytes();

        PubSubMessage message;
        message = new PubSubMessage(messageID, messageContent);
        Assert.assertTrue(message.hasContent());
        Assert.assertEquals(message.getContent(), messageContent);

        message = new PubSubMessage(messageID, null, Signal.COMPLETE);
        Assert.assertFalse(message.hasContent());

        message = new PubSubMessage(messageID, (String) null, null);
        Assert.assertFalse(message.hasContent());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNoIDIllegalCreation() {
        new PubSubMessage(null, new byte[0]);
    }

    @Test
    public void testEquals() {
        String messageID = getRandomString();
        byte[] messageContent = getRandomBytes();
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
        byte[] messageContent = getRandomBytes();
        Metadata randomMetadata = new Metadata(Signal.ACKNOWLEDGE, getRandomString());
        PubSubMessage message1 = new PubSubMessage(messageID, messageContent, randomMetadata);
        PubSubMessage message2 = new PubSubMessage(messageID, messageContent, new Metadata(Signal.ACKNOWLEDGE, getRandomString()));
        Assert.assertEquals(message1.hashCode(), message2.hashCode());
    }

    @Test
    public void testHasMetadata() {
        String messageID = getRandomString();
        byte[] messageContent = getRandomBytes();

        PubSubMessage message;
        message = new PubSubMessage(messageID, messageContent);
        Assert.assertFalse(message.hasMetadata());
        Assert.assertFalse(message.hasSignal());

        message = new PubSubMessage(messageID, null, Signal.COMPLETE);
        Assert.assertTrue(message.hasMetadata());
        Assert.assertTrue(message.hasSignal(Signal.COMPLETE));

        message = new PubSubMessage(messageID, (byte[]) null, new Metadata());
        Assert.assertTrue(message.hasMetadata());
        Assert.assertFalse(message.hasSignal());
    }

    @Test
    public void testToString() {
        String bar = "[98,97,114]";
        Metadata metadata = new Metadata(Signal.FAIL, 42.0);
        assertJSONEquals(new PubSubMessage("foo", CONTENT, metadata).toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'FAIL', 'content': 42.0, 'created': " + metadata.getCreated() + " } }");
        metadata = new Metadata(Signal.COMPLETE, new ArrayList<>());
        assertJSONEquals(new PubSubMessage("foo", CONTENT, metadata).toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'COMPLETE', 'content': [], 'created': " + metadata.getCreated() + " } }");
        PubSubMessage pubSubMessage = new PubSubMessage("foo", CONTENT, Signal.COMPLETE);
        assertJSONEquals(pubSubMessage.toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'COMPLETE', 'content': null, 'created': " + pubSubMessage.getMetadata().getCreated() + " } }");
        pubSubMessage = new PubSubMessage("foo", CONTENT, Signal.ACKNOWLEDGE);
        assertJSONEquals(pubSubMessage.toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'ACKNOWLEDGE', 'content': null, 'created': " + pubSubMessage.getMetadata().getCreated() + " } }");
        assertJSONEquals(new PubSubMessage("foo", CONTENT).toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': null }");
        assertJSONEquals(new PubSubMessage("foo", CONTENT).toString(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': null }");
        assertJSONEquals(new PubSubMessage().toString(),
                         "{ 'id': '', 'content': null, 'metadata': null }");
    }

    @Test
    public void testJSONConversion() {
        String bar = "[98,97,114]";
        Metadata metadata = new Metadata(Signal.FAIL, 42.0);
        assertJSONEquals(new PubSubMessage("foo", CONTENT, metadata).asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'FAIL', 'content': 42.0, 'created': " + metadata.getCreated() + " } }");
        metadata = new Metadata(Signal.COMPLETE, new ArrayList<>());
        assertJSONEquals(new PubSubMessage("foo", CONTENT, metadata).asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'COMPLETE', 'content': [], 'created': " + metadata.getCreated() + " } }");
        PubSubMessage pubSubMessage = new PubSubMessage("foo", CONTENT, Signal.COMPLETE);
        assertJSONEquals(pubSubMessage.asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'COMPLETE', 'content': null, 'created': " + pubSubMessage.getMetadata().getCreated() + " } }");
        pubSubMessage = new PubSubMessage("foo", CONTENT, Signal.ACKNOWLEDGE);
        assertJSONEquals(pubSubMessage.asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': { 'signal': 'ACKNOWLEDGE', 'content': null, 'created': " + pubSubMessage.getMetadata().getCreated() + " } }");
        assertJSONEquals(new PubSubMessage("foo", CONTENT).asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': null }");
        assertJSONEquals(new PubSubMessage("foo", CONTENT).asJSON(),
                         "{ 'id': 'foo', 'content': " + bar + ", 'metadata': null }");
        assertJSONEquals(new PubSubMessage().asJSON(),
                         "{ 'id': '', 'content': null, 'metadata': null }");
    }

    @Test
    public void testRecreatingFromJSON() {
        String bar = "[98,97,114]";
        PubSubMessage actual = PubSubMessage.fromJSON("{ 'id': 'foo', 'content': " + bar + ", 'metadata': " +
                                                      "{ 'signal': 'FAIL', 'content': { 'type': null } } }");
        PubSubMessage expected = new PubSubMessage("foo", "bar", null);
        Assert.assertEquals(actual, expected);
        Assert.assertEquals(actual.getContent(), CONTENT);
        Assert.assertEquals(actual.getContentAsString(), "bar");

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
