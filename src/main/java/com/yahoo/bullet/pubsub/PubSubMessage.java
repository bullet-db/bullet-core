package com.yahoo.bullet.pubsub;

import java.io.Serializable;
import com.yahoo.bullet.pubsub.Metadata.Signal;
import lombok.Getter;

@Getter
public class PubSubMessage implements Serializable {
    private String id;
    private String content;
    private Metadata metadata;
    private long sequenceNumber;

    /**
     * Constructor for a message having only content.
     *
     * @param id is the query ID associated with the message.
     * @param content is the content of the message.
     */
    public PubSubMessage(String id, String content) {
        this.id = id;
        this.content = content;
        this.metadata = new Metadata();
        this.sequenceNumber = -1;
    }

    /**
     * Constructor for a message having content and a sequence number.
     *
     * @param id is the query ID associated with the message.
     * @param content is the content of the message.
     * @param sequenceNumber is the sequence number of the message.
     */
    public PubSubMessage(String id, String content, long sequenceNumber) {
        this.id = id;
        this.content = content;
        this.metadata = new Metadata();
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * Constructor for a PubSubMessage having content and Metadata.
     *
     * @param id is the query ID associated with the message.
     * @param content is the content of the message.
     * @param metadata is the {@link Metadata} associated with the message.
     */
    public PubSubMessage(String id, String content, Metadata metadata) {
        this(id, content);
        this.metadata = metadata;
    }

    /**
     * Constructor for a message having content, a {@link Metadata.Signal} and a sequence number.
     *
     * @param id is the query ID associated with the message.
     * @param content is the content of the message.
     * @param sequenceNumber is the sequence number of the message.
     * @param signal is the Metadata.Signal of the message.
     */
    public PubSubMessage(String id, String content, long sequenceNumber, Signal signal) {
        this(id, content, sequenceNumber);
        this.metadata.setSignal(signal);
    }

    /**
     * Constructor for a PubSubMessage having content, Metadata and a sequence number.
     *
     * @param id is the query ID associated with the message.
     * @param content is the content of the message.
     * @param sequenceNumber is the sequence number associated with the message.
     * @param metadata is the {@link Metadata} associated with the message.
     */
    public PubSubMessage(String id, String content, long sequenceNumber, Metadata metadata) {
        this(id, content, sequenceNumber);
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
