package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.pubsub.Metadata.Signal;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

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

        message = new PubSubMessage(messageID, null, Signal.COMPLETE);
        Assert.assertTrue(message.hasMetadata());

        message = new PubSubMessage(messageID, null, new Metadata());
        Assert.assertTrue(message.hasMetadata());
    }
}
