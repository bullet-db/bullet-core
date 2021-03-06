/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.Metadata.Signal;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.Raw;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;

public class PubSubMessageTest {
    private static final String NULL = Base64.getEncoder().encodeToString(SerializerDeserializer.toBytes(null));
    private static final String BAR = Base64.getEncoder().encodeToString(SerializerDeserializer.toBytes("bar"));

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

        message = new PubSubMessage(messageID, (String) null);
        Assert.assertFalse(message.hasContent());
    }

    @Test
    public void testReadingDataAsDifferentTypes() {
        String string = getRandomString();
        byte[] bytes = getRandomBytes();
        Query query = new Query(new Projection(), null, new Raw(1), null, new Window(), Long.MAX_VALUE);

        PubSubMessage message;
        message = new PubSubMessage("foo", string);
        Assert.assertEquals(message.getContent(), string);
        Assert.assertEquals(message.getContentAsString(), string);

        message = new PubSubMessage("foo", bytes);
        Assert.assertEquals(message.getContent(), bytes);
        Assert.assertEquals(message.getContentAsByteArray(), bytes);

        message = new PubSubMessage("foo", query);
        Assert.assertSame(message.getContent(), query);
        Assert.assertSame(message.getContentAsQuery(), query);
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
        PubSubMessage messageA = new PubSubMessage(messageID, messageContent, randomMetadata);
        PubSubMessage messageB = new PubSubMessage(messageID, messageContent, new Metadata(Signal.ACKNOWLEDGE, getRandomString()));
        PubSubMessage messageC = new PubSubMessage(getRandomString(), messageContent, randomMetadata);

        Assert.assertEquals(messageA, messageB);
        Assert.assertNotEquals(messageA, messageC);
        Assert.assertFalse(messageA.equals(null));
        Assert.assertFalse(messageA.equals(new PubSubException("Dummy")));
    }

    @Test
    public void testHashCode() {
        String messageID = getRandomString();
        byte[] messageContent = getRandomBytes();
        Metadata randomMetadata = new Metadata(Signal.ACKNOWLEDGE, getRandomString());
        PubSubMessage messageA = new PubSubMessage(messageID, messageContent, randomMetadata);
        PubSubMessage messageB = new PubSubMessage(messageID, messageContent, new Metadata(Signal.ACKNOWLEDGE, getRandomString()));
        Assert.assertEquals(messageA.hashCode(), messageB.hashCode());
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

        message = new PubSubMessage(messageID, null, new Metadata());
        Assert.assertTrue(message.hasMetadata());
        Assert.assertFalse(message.hasSignal());
    }

    @Test
    public void testToString() {
        Metadata metadata = new Metadata(Signal.FAIL, 42.0);
        assertJSONEquals(new PubSubMessage("foo", "bar", metadata).toString(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': { 'signal': 'FAIL', 'content': 42.0, 'created': " + metadata.getCreated() + " } }");
        metadata = new Metadata(Signal.COMPLETE, new ArrayList<>());
        assertJSONEquals(new PubSubMessage("foo", "bar", metadata).toString(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': { 'signal': 'COMPLETE', 'content': [], 'created': " + metadata.getCreated() + " } }");
        PubSubMessage pubSubMessage = new PubSubMessage("foo", "bar", Signal.COMPLETE);
        assertJSONEquals(pubSubMessage.toString(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': { 'signal': 'COMPLETE', 'content': null, 'created': " + pubSubMessage.getMetadata().getCreated() + " } }");
        pubSubMessage = new PubSubMessage("foo", "bar", Signal.ACKNOWLEDGE);
        assertJSONEquals(pubSubMessage.toString(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': { 'signal': 'ACKNOWLEDGE', 'content': null, 'created': " + pubSubMessage.getMetadata().getCreated() + " } }");
        assertJSONEquals(new PubSubMessage("foo", "bar").toString(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': null }");
        assertJSONEquals(new PubSubMessage("foo", "bar").toString(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': null }");
        assertJSONEquals(new PubSubMessage().toString(),
                         "{ 'id': '', 'content': '" + NULL + "', 'metadata': null }");
    }

    @Test
    public void testJSONConversion() {
        Metadata metadata = new Metadata(Signal.FAIL, 42.0);
        assertJSONEquals(new PubSubMessage("foo", "bar", metadata).asJSON(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': { 'signal': 'FAIL', 'content': 42.0, 'created': " + metadata.getCreated() + " } }");
        metadata = new Metadata(Signal.COMPLETE, new ArrayList<>());
        assertJSONEquals(new PubSubMessage("foo", "bar", metadata).asJSON(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': { 'signal': 'COMPLETE', 'content': [], 'created': " + metadata.getCreated() + " } }");
        PubSubMessage pubSubMessage = new PubSubMessage("foo", "bar", Signal.COMPLETE);
        assertJSONEquals(pubSubMessage.asJSON(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': { 'signal': 'COMPLETE', 'content': null, 'created': " + pubSubMessage.getMetadata().getCreated() + " } }");
        pubSubMessage = new PubSubMessage("foo", "bar", Signal.ACKNOWLEDGE);
        assertJSONEquals(pubSubMessage.asJSON(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': { 'signal': 'ACKNOWLEDGE', 'content': null, 'created': " + pubSubMessage.getMetadata().getCreated() + " } }");
        assertJSONEquals(new PubSubMessage("foo", "bar").asJSON(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': null }");
        assertJSONEquals(new PubSubMessage("foo", "bar").asJSON(),
                         "{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': null }");
        assertJSONEquals(new PubSubMessage().asJSON(),
                         "{ 'id': '', 'content': '" + NULL + "', 'metadata': null }");
    }

    @Test
    public void testRecreatingFromJSON() {
        PubSubMessage actual = PubSubMessage.fromJSON("{ 'id': 'foo', 'content': '" + BAR + "', 'metadata': " +
                                                      "{ 'signal': 'FAIL', 'content': { 'type': null } } }");
        PubSubMessage expected = new PubSubMessage("foo", "bar");

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
    public void testSettingConent() {
        PubSubMessage message = new PubSubMessage("foo", "bar");
        Assert.assertEquals(message.getId(), "foo");
        Assert.assertEquals(message.getContent(), "bar");
        Assert.assertEquals(message.getContentAsString(), "bar");

        message.setContent(new byte[0]);
        Assert.assertEquals(message.getContent(), new byte[0]);
        Assert.assertEquals(message.getContentAsByteArray(), new byte[0]);

        message.setContent(new HashMap<>());
        Assert.assertEquals(message.getContent(), Collections.emptyMap());
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
