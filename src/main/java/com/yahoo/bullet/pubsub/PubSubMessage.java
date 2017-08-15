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
     * Constructor for a message containing content and not supporting sequence numbers.
     *
     */
    public PubSubMessage(String id, String content) {
        this.id = id;
        this.content = content;
        this.metadata = new Metadata();
        this.sequenceNumber = -1;
    }

    /**
     * Constructor for a message containing content and supporting sequence numbers.
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
     * Constructor for a message containing content and {@link Metadata.Signal}.
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
     * Constructor for a PubSubMessage containing Metadata and content.
     *
     * @param id is the query ID associated with the message.
     * @param content is the content of the message.
     * @param sequenceNumber is the sequence number associated with the message.
     * @param metadata is the {@link Metadata} associated with the message.
     */
    public PubSubMessage(String id, String content, long sequenceNumber, Metadata metadata) {
        this.id = id;
        this.content = content;
        this.sequenceNumber = sequenceNumber;
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
