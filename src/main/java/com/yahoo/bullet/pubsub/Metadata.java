/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter @Setter @AllArgsConstructor @NoArgsConstructor
public class Metadata implements Serializable {
    public enum Signal {
        ACKNOWLEDGE,
        COMPLETE
    }
    private Signal signal;
    private Serializable content;

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
}
