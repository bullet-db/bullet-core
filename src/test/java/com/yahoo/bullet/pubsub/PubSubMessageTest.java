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
        String messageId = getRandomString();
        String messageContent = getRandomString();

        PubSubMessage message = new PubSubMessage(messageId, messageContent);
        Assert.assertTrue(messageId.equals(message.getId()));
        Assert.assertEquals(message.getSequence(), -1);
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertNull(message.getMetadata());
    }

    @Test
    public void testWithSequenceCreation() {
        String messageId = getRandomString();
        String messageContent = getRandomString();

        PubSubMessage message = new PubSubMessage(messageId, messageContent, 0);
        Assert.assertTrue(messageId.equals(message.getId()));
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertEquals(message.getSequence(), 0);
        Assert.assertNull(message.getMetadata());
    }

    @Test
    public void testWithSignalCreation() {
        String messageId = getRandomString();
        String messageContent = getRandomString();
        Signal signal = Signal.ACKNOWLEDGE;

        PubSubMessage message = new PubSubMessage(messageId, messageContent, signal, 0);
        Assert.assertTrue(messageId.equals(message.getId()));
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertEquals(message.getSequence(), 0);
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), signal);
        Assert.assertNull(message.getMetadata().getContent());
        Assert.assertNull(message.getMetadata().getContent());
    }

    @Test
    public void testWithMetadataCreation() {
        String messageId = getRandomString();
        String messageContent = getRandomString();
        String metadataContent = getRandomString();
        Signal signal = Signal.ACKNOWLEDGE;
        //Test creation without a sequence number.
        PubSubMessage message = new PubSubMessage(messageId, messageContent, new Metadata(signal, metadataContent));
        Assert.assertTrue(messageId.equals(message.getId()));
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertEquals(message.getSequence(), -1);
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), signal);
        Assert.assertTrue(message.getMetadata().getContent().toString().equals(metadataContent));
    }

    @Test
    public void testWithMetadataAndSequenceCreation() {
        String messageId = getRandomString();
        String messageContent = getRandomString();
        String metadataContent = getRandomString();
        Signal signal = Signal.ACKNOWLEDGE;
        //Test creation with a sequence number.
        PubSubMessage message = new PubSubMessage(messageId, messageContent, new Metadata(signal, metadataContent), 0);
        Assert.assertTrue(messageId.equals(message.getId()));
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertEquals(message.getSequence(), 0);
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), signal);
        Assert.assertTrue(message.getMetadata().getContent().toString().equals(metadataContent));
    }

    @Test
    public void testHasContent() {
        String messageId = getRandomString();
        String messageContent = getRandomString();

        PubSubMessage message;
        message = new PubSubMessage(messageId, messageContent);
        Assert.assertTrue(message.hasContent());

        message = new PubSubMessage(messageId, null, Signal.COMPLETE);
        Assert.assertFalse(message.hasContent());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNoIdIllegalCreation() {
        new PubSubMessage(null, "");
    }
}
