package com.yahoo.bullet.pubsub;

import java.io.Serializable;
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
     * Constructor for a message having only content.
     *
     * @param id is the ID associated with the message.
     * @param content is the content of the message.
     */
    public PubSubMessage(String id, String content) {
        this(id, content, -1, new Metadata());
    }

    /**
     * Constructor for a message having content and a sequence number.
     *
     * @param id is the ID associated with the message.
     * @param content is the content of the message.
     * @param sequence is the sequence number of the message.
     */
    public PubSubMessage(String id, String content, int sequence) {
        this(id, content, sequence, new Metadata());
    }

    /**
     * Constructor for a PubSubMessage having content and Metadata.
     *
     * @param id is the ID associated with the message.
     * @param content is the content of the message.
     * @param metadata is the {@link Metadata} associated with the message.
     */
    public PubSubMessage(String id, String content, Metadata metadata) {
        this(id, content, -1, metadata);
    }

    /**
     * Constructor for a message having content, a {@link Metadata.Signal} and a sequence number.
     *
     * @param id is the ID associated with the message.
     * @param content is the content of the message.
     * @param sequence is the sequence number of the message.
     * @param signal is the Metadata.Signal of the message.
     */
    public PubSubMessage(String id, String content, int sequence, Signal signal) {
        this(id, content, sequence, new Metadata(signal, null));
    }

    /**
     * Constructor for a PubSubMessage having content, Metadata and a sequence number.
     *
     * @param id is the ID associated with the message.
     * @param content is the content of the message.
     * @param sequence is the sequence number associated with the message.
     * @param metadata is the {@link Metadata} associated with the message.
     */
    public PubSubMessage(String id, String content, int sequence, Metadata metadata) {
        this.id = id;
        this.content = content;
        this.sequence = sequence;
        this.metadata = metadata;
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
