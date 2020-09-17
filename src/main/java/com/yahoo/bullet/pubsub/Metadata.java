/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class Metadata implements Serializable {
    public enum Signal {
        ACKNOWLEDGE,
        COMPLETE,
        FAIL,
        KILL,
        REPLAY,
        CUSTOM
    }

    private static final long serialVersionUID = 7478596915692253699L;
    @Getter @Setter
    private Signal signal;
    // Serializable enforced through the constructor, getter, and setter. Is Object so GSON can reify an instance.
    private Object content;
    @Getter @Setter
    private long created;

    /**
     * Default constructor that creates an empty instance of metadata.
     */
    public Metadata() {
        created = System.currentTimeMillis();
    }

    /**
     * Allows you to create an instance with a {@link com.yahoo.bullet.pubsub.Metadata.Signal} and a
     * {@link Serializable} object.
     *
     * @param signal The signal to set.
     * @param object The object that is the metadata.
     */
    public Metadata(Signal signal, Serializable object) {
        this();
        this.signal = signal;
        this.content = object;
    }

    /**
     * Returns a copy of the current metadata. Subclasses should override this method.
     *
     * @return A copy of this {@link Metadata}.
     */
    public Metadata copy() {
        return new Metadata(signal, (Serializable) content);
    }

    /**
     * Set a serializable content for this metadata.
     *
     * @param content The content for this metadata.
     */
    public void setContent(Serializable content) {
        this.content = content;
    }

    /**
     * Returns the {@link Serializable} content in this metadata.
     *
     * @return The serializable content or null.
     */
    public Serializable getContent() {
        return (Serializable) content;

    }

    /**
     * Check if Metadata has content.
     *
     * @return true if Metadata has content.
     */
    public boolean hasContent() {
        return content != null;
    }

    /**
     * Check if Metadata has signal.
     *
     * @return true if message has {@link Metadata#signal}
     */
    public boolean hasSignal() {
        return signal != null;
    }

    /**
     * Check if Metadata has the given signal.
     *
     * @param signal The signal to check against.
     * @return true if message has {@link Metadata#signal}
     */
    public boolean hasSignal(Signal signal) {
        return hasSignal() && this.signal == signal;
    }
}
