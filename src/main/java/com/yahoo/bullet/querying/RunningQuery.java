/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.query.Query;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A wrapper for a running query.
 */
@Getter @RequiredArgsConstructor
public class RunningQuery {
    private final String id;
    private final Query query;
    private long startTime;

    @Override
    public String toString() {
        return query.toString();
    }

    /**
     * Returns true if this running query has timed out. In other words, it returns whether this has been running
     * longer than the query duration.
     *
     * @return A boolean denoting whether this query has timed out.
     */
    public boolean isTimedOut() {
        // Never add to query.getDuration since it can be infinite (Long.MAX_VALUE)
        return System.currentTimeMillis() - startTime >= query.getDuration();
    }

    /**
     * Exposed for package only. Starts the query.
     */
    void start() {
        startTime = System.currentTimeMillis();
    }
}
