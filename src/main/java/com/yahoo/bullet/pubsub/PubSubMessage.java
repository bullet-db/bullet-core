/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.google.gson.Gson;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.Metadata.Signal;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.result.JSONFormatter;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * The class of messages that can be understood by the PubSub package. The id should be set to the query ID generated
 * by the web service for Bullet queries.
 */
@Getter
public class PubSubMessage implements Serializable, JSONFormatter {
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final long serialVersionUID = 5096747716667851530L;

    private String id;
    // Serializable enforced through the constructors, and getter. Is Object so GSON can reify an instance.
    private Object content;
    @Setter
    private Metadata metadata;

    /**
     * Constructor for a message having no information. Used internally. Not recommended for use.
     */
    public PubSubMessage() {
        this("", (byte[]) null);
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
     * Constructor for a message having only content.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     */
    public PubSubMessage(String id, Serializable content) {
        this(id, content, (Metadata) null);
    }

    /**
     * Constructor for a message having content and a {@link Metadata.Signal}.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param signal The Signal to be sent with the message.
     */
    public PubSubMessage(String id, Serializable content, Signal signal) {
        this(id, content, new Metadata(signal, null));
    }

    /**
     * Constructor for a message having content and {@link Metadata}.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @param metadata The Metadata associated with the message.
     */
    public PubSubMessage(String id, Serializable content, Metadata metadata) {
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
     * Returns the {@link Serializable} content stored in the message.
     *
     * @return The content stored.
     */
    public Serializable getContent() {
        return (Serializable) content;
    }

    /**
     * Returns the content stored in the message as a byte[]. You should use this to read the byte[] back from the
     * message if you provided it originally to the message as a byte[].
     *
     * @return The content stored as a byte[].
     */
    public byte[] getContentAsByteArray() {
        return (byte[]) content;
    }

    /**
     * Returns the content stored in the message as a String. You should use this to read the String back from the
     * message if you provided it originally to the message as a String.
     *
     * @return The content stored as a String.
     */
    public String getContentAsString() {
        return (String) content;
    }

    /**
     * Returns the content stored in the message as a {@link Query}. You should use this to read the {@link Query} back
     * if you originally provided to the message as a {@link Serializable}.
     *
     * @return The content stored as a {@link Query}.
     */
    public Query getContentAsQuery() {
        return (Query) content;
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
        String data = Base64.getEncoder().encodeToString(SerializerDeserializer.toBytes((Serializable) content));
        PubSubMessage message = new PubSubMessage(id, data, metadata);
        return JSONFormatter.asJSON(message);
    }

    /**
     * Converts a json representation back to an instance. Is the inverse of {@link #asJSON()}.
     *
     * @param json The string representation of the JSON.
     * @return An instance of this class.
     */
    public static PubSubMessage fromJSON(String json) {
        return fromJSON(json, GSON);
    }

    /**
     * Converts a json representation back to an instance using a specific {@link Gson} converter.
     * Is the inverse of {@link #asJSON()}.
     *
     * @param json The string representation of the JSON.
     * @param gson The {@link Gson} converter to use.
     * @return An instance of this class.
     */
    public static PubSubMessage fromJSON(String json, Gson gson) {
        return fromJSON(gson.fromJson(json, PubSubMessage.class));
    }

    private static PubSubMessage fromJSON(PubSubMessage message) {
        if (message == null || message.getContent() == null) {
            return message;
        }
        message.content = SerializerDeserializer.fromBytes(Base64.getDecoder().decode(message.getContentAsString()));
        return message;
    }
}
