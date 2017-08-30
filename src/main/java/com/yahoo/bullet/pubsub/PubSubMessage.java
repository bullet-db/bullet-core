package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.pubsub.Metadata.Signal;
import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

/**
 * The class of messages that can be understood by the PubSub package. The id should be set to the query ID generated
 * by the web service for Bullet queries. The sequence identifies individual segments if a multi-part response is
 * emitted by Bullet.
 */
@Getter
public class PubSubMessage implements Serializable {
    private String id;
    private int sequence;
    private String content;
    private Metadata metadata;

    /**
     * Constructor for a message having only content.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     */
    public PubSubMessage(String id, String content) {
        this(id, content, (Metadata) null, -1);
    }

    /**
     * Constructor for a message having content and a sequence number.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param sequence The sequence number of the message.
     */
    public PubSubMessage(String id, String content, int sequence) {
        this(id, content, (Metadata) null, sequence);
    }

    /**
     * Constructor for a message having content and {@link Metadata}.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param metadata The Metadata associated with the message.
     */
    public PubSubMessage(String id, String content, Metadata metadata) {
        this(id, content, metadata, -1);
    }

    /**
     * Constructor for a message having content and a {@link Metadata.Signal}.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param signal The Metadata.Signal to be sent with the message.
     */
    public PubSubMessage(String id, String content, Signal signal) {
        this(id, content, signal, -1);
    }

    /**
     * Constructor for a message having content, a {@link Metadata.Signal} and a sequence number.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param signal The Signal to be sent with the message.
     * @param sequence The sequence number of the message.
     */
    public PubSubMessage(String id, String content, Signal signal, int sequence) {
        this(id, content, new Metadata(signal, null), sequence);
    }

    /**
     * Constructor for a message having content, {@link Metadata} and a sequence number.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param metadata The Metadata associated with the message.
     * @param sequence The sequence number of the message.
     */
    public PubSubMessage(String id, String content, Metadata metadata, int sequence) {
        this.id = Objects.requireNonNull(id, "ID cannot be null");
        this.content = content;
        this.metadata = metadata;
        this.sequence = sequence;
    }

    /**
     * Check if message has content.
     *
     * @return true if message has content.
     */
    public boolean hasContent() {
        return content != null;
    }

    /**
     * Check if message has {@link Metadata}.
     *
     * @return true if message has Metadata.
     */
    public boolean hasMetadata() {
        return metadata != null;
    }

    @Override
    public int hashCode() {
        return (id + sequence).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || other.getClass() != PubSubMessage.class) {
            return false;
        }
        PubSubMessage otherMessage = (PubSubMessage) other;
        return id.equals(otherMessage.getId()) && sequence == otherMessage.getSequence();
    }
}
