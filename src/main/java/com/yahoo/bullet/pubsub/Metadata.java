/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@NoArgsConstructor
public class Metadata implements Serializable {
    public enum Signal {
        ACKNOWLEDGE,
        COMPLETE,
        FAIL,
        KILL,
        REPLAY,
        CUSTOM
    }

    private static final long serialVersionUID = 4234800234857923112L;

    @Getter @Setter
    private Signal signal;

    // This is a Serializable object enforced through the constructor, getter and setter. Storing it as an Object so
    // GSON can reify an instance.
    private Object content;

    /**
     * Allows you to create an instance with a {@link com.yahoo.bullet.pubsub.Metadata.Signal} and a
     * {@link Serializable} object.
     *
     * @param signal The signal to set.
     * @param object The object that is the metadata.
     */
    public Metadata(Signal signal, Serializable object) {
        this.signal = signal;
        this.content = object;
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
