package com.yahoo.bullet.pubsub;

import java.io.Serializable;
import java.util.Objects;

import com.yahoo.bullet.pubsub.Metadata.Signal;
import lombok.Getter;

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
     * Constructor for a message that contains an id and content.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     */
    public PubSubMessage(String id, String content) {
        this(id, content, (Metadata) null, -1);
    }

    /**
     * Constructor for a message that contains an id, content and a sequence number.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param sequence The integer sequence number of the message.
     */
    public PubSubMessage(String id, String content, int sequence) {
        this(id, content, (Metadata) null, sequence);
    }

    /**
     * Constructor for a message that contains an id, {@link Metadata} and content.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param metadata The Metadata associated with the message.
     */
    public PubSubMessage(String id, String content, Metadata metadata) {
        this(id, content, metadata, -1);
    }

    /**
     * Constructor for a message that contains an id, {@link Metadata.Signal} and may contain content.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param signal The Signal to be sent with the message.
     */
    public PubSubMessage(String id, String content, Signal signal) {
        this(id, content, signal, -1);
    }

    /**
     * Constructor for a message that contains an id, {@link Metadata.Signal}, a sequence number and may contain content.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param signal The Signal to be sent with the message.
     * @param sequence The integer sequence number of the message.
     */
    public PubSubMessage(String id, String content, Signal signal, int sequence) {
        this(id, content, new Metadata(signal, null), sequence);
    }

    /**
     * Constructor for a message that contains an id, {@link Metadata}, a sequence number and content.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param metadata The Metadata associated with the message.
     * @param sequence The integer sequence number of the message.
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
}
