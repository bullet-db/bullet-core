/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.querying.aggregations.MockStrategy;
import lombok.Setter;

public class ClosableStrategy extends MockStrategy {
    @Setter
    private boolean closed = false;

    @Override
    public boolean isClosed() {
        super.isClosed();
        return closed;
    }
}

