/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.query.Query;
import lombok.Getter;

/**
 * A wrapper for a running query.
 */
@Getter
public class RunningQuery {
    private final String id;
    private final Query query;
    private final String queryString;
    private final long startTime;

    /**
     * Constructor that takes an id, query, and metadata that contains the query string and start time.
     *
     * @param id The query id.
     * @param query The query object.
     * @param metadata The metadata associated with the given query.
     */
    public RunningQuery(String id, Query query, Metadata metadata) {
        this.id = id;
        this.query = query;
        this.queryString = (String) metadata.getContent();
        this.startTime = metadata.getCreated();
    }

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
        // Never add to query.getDuration() since it can be infinite (Long.MAX_VALUE)
        return System.currentTimeMillis() - startTime >= query.getDuration();
    }
}
