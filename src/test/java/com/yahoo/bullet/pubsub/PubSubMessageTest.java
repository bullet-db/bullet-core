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
        Assert.assertTrue(messageID.equals(message.getId()));
        Assert.assertEquals(message.getSequence(), -1);
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertNull(message.getMetadata());
    }

    @Test
    public void testWithSequenceCreation() {
        String messageID = getRandomString();
        String messageContent = getRandomString();

        PubSubMessage message = new PubSubMessage(messageID, messageContent, 0);
        Assert.assertTrue(messageID.equals(message.getId()));
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertEquals(message.getSequence(), 0);
        Assert.assertNull(message.getMetadata());
    }

    @Test
    public void testWithOnlySignalCreation() {
        String messageID = getRandomString();

        PubSubMessage message = new PubSubMessage(messageID, Signal.KILL);
        Assert.assertTrue(messageID.equals(message.getId()));
        Assert.assertNull(message.getContent());
        Assert.assertEquals(message.getSequence(), -1);
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), Signal.KILL);
        Assert.assertNull(message.getMetadata().getContent());
    }

    @Test
    public void testWithSignalCreation() {
        String messageID = getRandomString();
        String messageContent = getRandomString();
        Signal signal = Signal.ACKNOWLEDGE;

        PubSubMessage message = new PubSubMessage(messageID, messageContent, signal, 0);
        Assert.assertTrue(messageID.equals(message.getId()));
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertEquals(message.getSequence(), 0);
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
        Assert.assertTrue(messageID.equals(message.getId()));
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertEquals(message.getSequence(), -1);
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), signal);
        Assert.assertTrue(message.getMetadata().getContent().toString().equals(metadataContent));
    }

    @Test
    public void testWithMetadataAndSequenceCreation() {
        String messageID = getRandomString();
        String messageContent = getRandomString();
        String metadataContent = getRandomString();
        Signal signal = Signal.ACKNOWLEDGE;
        //Test creation with a sequence number.
        PubSubMessage message = new PubSubMessage(messageID, messageContent, new Metadata(signal, metadataContent), 0);
        Assert.assertTrue(messageID.equals(message.getId()));
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertEquals(message.getSequence(), 0);
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), signal);
        Assert.assertTrue(message.getMetadata().getContent().toString().equals(metadataContent));
    }

    @Test
    public void testHasContent() {
        String messageID = getRandomString();
        String messageContent = getRandomString();

        PubSubMessage message;
        message = new PubSubMessage(messageID, messageContent);
        Assert.assertTrue(message.hasContent());

        message = new PubSubMessage(messageID, null, Signal.COMPLETE);
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
        PubSubMessage message4 = new PubSubMessage(messageID, messageContent, randomMetadata, 2);

        Assert.assertEquals(message1, message2);
        Assert.assertNotEquals(message1, message3);
        Assert.assertNotEquals(message1, message4);
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

        message = new PubSubMessage(messageID, null, Signal.COMPLETE);
        Assert.assertTrue(message.hasMetadata());
        Assert.assertTrue(message.hasSignal(Signal.COMPLETE));

        message = new PubSubMessage(messageID, null, new Metadata());
        Assert.assertTrue(message.hasMetadata());
        Assert.assertFalse(message.hasSignal());
    }

    @Test
    public void testToString() {
        assertJSONEquals(new PubSubMessage("foo", "bar", new Metadata(Signal.FAIL, 42.0), 42).toString(),
                         "{ 'id': 'foo', 'sequence': 42, 'content': 'bar', 'metadata': { 'signal': 'FAIL', 'content': 42.0 } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", new Metadata(Signal.COMPLETE, new ArrayList<>())).toString(),
                         "{ 'id': 'foo', 'sequence': -1, 'content': 'bar', 'metadata': { 'signal': 'COMPLETE', 'content': [] } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", Signal.COMPLETE, 22).toString(),
                         "{ 'id': 'foo', 'sequence': 22, 'content': 'bar', 'metadata': { 'signal': 'COMPLETE', 'content': null } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", Signal.ACKNOWLEDGE).toString(),
                         "{ 'id': 'foo', 'sequence': -1, 'content': 'bar', 'metadata': { 'signal': 'ACKNOWLEDGE', 'content': null } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", 34).toString(),
                         "{ 'id': 'foo', 'sequence': 34, 'content': 'bar', 'metadata': null }");
        assertJSONEquals(new PubSubMessage("foo", "bar").toString(),
                         "{ 'id': 'foo', 'sequence': -1, 'content': 'bar', 'metadata': null }");
        assertJSONEquals(new PubSubMessage().toString(),
                         "{ 'id': '', 'sequence': -1, 'content': null, 'metadata': null }");
    }

    @Test
    public void testJSONConversion() {
        assertJSONEquals(new PubSubMessage("foo", "bar", new Metadata(Signal.FAIL, 42.0), 42).asJSON(),
                         "{ 'id': 'foo', 'sequence': 42, 'content': 'bar', 'metadata': { 'signal': 'FAIL', 'content': 42.0 } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", new Metadata(Signal.COMPLETE, new ArrayList<>())).asJSON(),
                         "{ 'id': 'foo', 'sequence': -1, 'content': 'bar', 'metadata': { 'signal': 'COMPLETE', 'content': [] } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", Signal.COMPLETE, 22).asJSON(),
                         "{ 'id': 'foo', 'sequence': 22, 'content': 'bar', 'metadata': { 'signal': 'COMPLETE', 'content': null } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", Signal.ACKNOWLEDGE).asJSON(),
                         "{ 'id': 'foo', 'sequence': -1, 'content': 'bar', 'metadata': { 'signal': 'ACKNOWLEDGE', 'content': null } }");
        assertJSONEquals(new PubSubMessage("foo", "bar", 34).asJSON(),
                         "{ 'id': 'foo', 'sequence': 34, 'content': 'bar', 'metadata': null }");
        assertJSONEquals(new PubSubMessage("foo", "bar").asJSON(),
                         "{ 'id': 'foo', 'sequence': -1, 'content': 'bar', 'metadata': null }");
        assertJSONEquals(new PubSubMessage().asJSON(),
                         "{ 'id': '', 'sequence': -1, 'content': null, 'metadata': null }");
    }

    @Test
    public void testRecreatingFromJSON() {
        PubSubMessage actual = PubSubMessage.fromJSON("{ 'id': 'foo', 'sequence': 42, 'content': 'bar', " +
                                                      "'metadata': { 'signal': 'FAIL', 'content': { 'type': null } } }");
        PubSubMessage expected = new PubSubMessage("foo", null, 42);
        Assert.assertEquals(actual, expected);

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
        Assert.assertEquals(actual.getSequence(), -1);
        Assert.assertNull(actual.getMetadata());
        Assert.assertNull(actual.getContent());

        actual = PubSubMessage.fromJSON("{ 'metadata': { 'signal': 'ACKNOWLEDGE' } }");

        Assert.assertEquals(actual.getId(), "");
        Assert.assertEquals(actual.getSequence(), -1);
        Assert.assertTrue(actual.hasSignal());
        Assert.assertTrue(actual.hasSignal(Signal.ACKNOWLEDGE));
        Assert.assertTrue(actual.getMetadata().hasSignal(Signal.ACKNOWLEDGE));
        Assert.assertNull(actual.getMetadata().getContent());
        Assert.assertNull(actual.getContent());
    }
}
