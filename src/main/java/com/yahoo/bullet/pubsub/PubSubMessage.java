/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.pubsub.Metadata.Signal;
import com.yahoo.bullet.result.JSONFormatter;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * The class of messages that can be understood by the PubSub package. The id should be set to the query ID generated
 * by the web service for Bullet queries.
 */
@Getter
public class PubSubMessage implements Serializable, JSONFormatter {
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final long serialVersionUID = -5068189058170874687L;

    private String id;
    private byte[] content;
    @Setter
    private Metadata metadata;

    /**
     * Constructor for a message having no information. Used internally. Not recommended for use.
     */
    public PubSubMessage() {
        this("", (byte[]) null);
    }

    /**
     * Constructor for a message having only content.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     */
    public PubSubMessage(String id, byte[] content) {
        this(id, content, (Metadata) null);
    }

    /**
     * Constructor for a message having only a {@link Metadata.Signal}.
     *
     * @param id The ID associated with the message.
     * @param signal The signal only for the Metadata.
     */
    public PubSubMessage(String id, Signal signal) {
        this(id, null, signal);
    }

    /**
     * Constructor for a message having content and a {@link Metadata.Signal}.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param signal The Signal to be sent with the message.
     */
    public PubSubMessage(String id, byte[] content, Signal signal) {
        this(id, content, new Metadata(signal, null));
    }

    /**
     * Constructor for a message having content as a String and {@link Metadata}.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message as a String.
     * @param metadata The Metadata associated with the message.
     */
    public PubSubMessage(String id, String content, Metadata metadata) {
        this(id, content == null ? null : content.getBytes(CHARSET), metadata);
    }

    /**
     * Constructor for a message having content and {@link Metadata}.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param metadata The Metadata associated with the message.
     */
    public PubSubMessage(String id, byte[] content, Metadata metadata) {
        this.id = Objects.requireNonNull(id, "ID cannot be null");
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

    /**
     * Check if message has {@link Metadata}.
     *
     * @return true if message has Metadata.
     */
    public boolean hasMetadata() {
        return metadata != null;
    }

    /**
     * Check if message has a given {@link Signal}.
     *
     * @param signal The signal to check for.
     * @return true if message has the given signal.
     */
    public boolean hasSignal(Signal signal) {
        return hasMetadata() && metadata.hasSignal(signal);
    }

    /**
     * Check if the message has a {@link Signal}.
     *
     * @return true if message has a signal.
     */
    public boolean hasSignal() {
        return hasMetadata() && metadata.hasSignal();
    }

    /**
     * Returns the content stored in the message as a String. You should use this to read the String back from the
     * message if you provided it originally to the message as a String.
     *
     * @return The content stored as a String using the {@link PubSubMessage#CHARSET}.
     */
    public String getContentAsString() {
        return content == null ? null : new String(content, CHARSET);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || other.getClass() != PubSubMessage.class) {
            return false;
        }
        PubSubMessage otherMessage = (PubSubMessage) other;
        return id.equals(otherMessage.getId());
    }

    @Override
    public String toString() {
        return asJSON();
    }

    @Override
    public String asJSON() {
        return JSONFormatter.asJSON(this);
    }

    /**
     * Converts a json representation back to an instance.
     *
     * @param json The string representation of the JSON.
     * @return An instance of this class.
     */
    public static PubSubMessage fromJSON(String json) {
        return JSONFormatter.fromJSON(json, PubSubMessage.class);
    }
}
