/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.common.Initializable;

public interface Scheme extends Initializable {
    /**
     * Returns true if this window is closed.
     *
     * @return A boolean denoting whether this window is currently closed.
     */
    boolean isClosed();

    /**
     * Returns true if this window is accepting more data.
     *
     * @return A boolean denoting this window can accept more data.
     */
    default boolean isOpen() {
        return !isClosed();
    }

    long windowCount();

    void runOnClose(Runnable runnable);
}
