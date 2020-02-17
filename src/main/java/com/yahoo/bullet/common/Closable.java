/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.common;

public interface Closable {
    /**
     * Returns true if this is currently closed.
     *
     * @return A boolean denoting whether this object is current closed.
     */
    boolean isClosed();
}
