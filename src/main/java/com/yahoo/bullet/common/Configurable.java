/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.common;

public interface Configurable {
    /**
     * Takes a {@link BulletConfig} containing configuration and applies it to itself.
     *
     * @param configuration The configuration containing the settings.
     */
    default void configure(BulletConfig configuration) {
        // Do nothing
    }
}
