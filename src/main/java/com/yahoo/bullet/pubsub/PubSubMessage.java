package com.yahoo.bullet.pubsub;

import java.io.Serializable;
import com.yahoo.bullet.pubsub.Metadata.Signal;
import lombok.Getter;

@Getter
public class PubSubMessage implements Serializable {
    private String id;
    private String content;
    private Metadata metadata;

    /**
     * Constructor for a message containing only content.
     *
     * @param id is the ID associated with the message.
     * @param content is the content of the message.
     */
    public PubSubMessage(String id, String content) {
        this.id = id;
        this.content = content;
        this.metadata = new Metadata();
    }

    /**
     * Constructor for a message containing content and {@link Metadata.Signal}.
     *
     * @param id is the ID associated with the message.
     * @param content is the content of the message.
     * @param signal is the Metadata.Signal of the message.
     */
    public PubSubMessage(String id, String content, Signal signal) {
        this.id = id;
        this.content = content;
        this.metadata = new Metadata(signal, null);
    }

    /**
     * Constructor for a PubSubMessage containing Metadata and content.
     *
     * @param id is the ID associated with the message.
     * @param content is the content of the message.
     * @param metadata is the {@link Metadata} associated with the message.
     */
    public PubSubMessage(String id, String content, Metadata metadata) {
        this.id = id;
        this.content = content;
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
