package com.yahoo.bullet.pubsub;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;
import com.yahoo.bullet.pubsub.Metadata.Signal;

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
        Assert.assertEquals(message.getSequenceNumber(), -1);
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertNotNull(message.getMetadata());
        Assert.assertNull(message.getMetadata().getSignal());
        Assert.assertNull(message.getMetadata().getContent());
    }

    @Test
    public void testWithSignalCreation() {
        String messageId = getRandomString();
        String messageContent = getRandomString();
        Signal signal = Signal.ACKNOWLEDGE;

        PubSubMessage message = new PubSubMessage(messageId, messageContent, 0, signal);
        Assert.assertTrue(messageId.equals(message.getId()));
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertEquals(message.getSequenceNumber(), 0);
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
        //Test creation with a sequence number.
        PubSubMessage message = new PubSubMessage(messageId, messageContent, 0, new Metadata(signal, metadataContent));
        Assert.assertTrue(messageId.equals(message.getId()));
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertEquals(message.getSequenceNumber(), 0);
        Assert.assertNotNull(message.getMetadata());
        Assert.assertEquals(message.getMetadata().getSignal(), signal);
        Assert.assertTrue(message.getMetadata().getContent().toString().equals(metadataContent));
        //Test creation without a sequence number.
        message = new PubSubMessage(messageId, messageContent, new Metadata(signal, metadataContent));
        Assert.assertTrue(messageId.equals(message.getId()));
        Assert.assertTrue(messageContent.equals(message.getContent()));
        Assert.assertEquals(message.getSequenceNumber(), -1);
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

        message = new PubSubMessage(messageId, null);
        Assert.assertFalse(message.hasContent());
    }
}
