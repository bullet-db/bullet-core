/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import java.util.Arrays;

public interface Closable {
    /**
     * Returns true if this is currently closed.
     *
     * @return A boolean denoting whether this object is current closed.
     */
    boolean isClosed();

    /**
     * Returns true if any of the provided {@link Closable} objects are closed.
     *
     * @param objects A non-null array of non-null closable objects.
     * @return A boolean denoting whether any of the objects were closed.gT
     */
    static boolean areAnyClosed(Closable... objects) {
        return Arrays.stream(objects).anyMatch(Closable::isClosed);
    }
}
