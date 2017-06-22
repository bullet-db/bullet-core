/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import java.util.Map;

public interface Configurable {
    /**
     * Takes a map containing configuration and applies it to itself.
     *
     * @param configuration A Map of configuration key values.
     */
    default void configure(Map configuration) {
        // Do nothing
    }
}
