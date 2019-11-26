/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.parsing.Parser;
import com.yahoo.bullet.parsing.Query;
import lombok.Getter;

import java.util.List;
import java.util.Optional;

/**
 * A wrapper for a running query.
 */
public class RunningQuery implements Initializable {
    @Getter
    private final String id;
    @Getter
    private final Query query;

    @Getter
    private long startTime;
    private String queryString;

    /**
     * Creates an instance of a Query object from the given String version of the query and an ID. It does also not
     * initialize it.
     *
     * @param id The String query ID.
     * @param queryString The String version of the query.
     * @param config The configuration to use for the query.
     * @throws com.google.gson.JsonParseException if there were issues parsing the query.
     *
     */
    public RunningQuery(String id, String queryString, BulletConfig config) {
        this(id, Parser.parse(queryString, config));
        this.queryString = queryString;
    }

    /**
     * Creates an instance of this from the given String ID for a query and a configured {@link Query}. It does also not
     * initialize it.
     *
     * @param id The non-null String query ID.
     * @param query The non-null configured query.
     */
    RunningQuery(String id, Query query) {
        this.id = id;
        this.query = query;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        start();
        return query.initialize();
    }

    @Override
    public String toString() {
        return queryString != null ? queryString : query.toString();
    }

    /**
     * Returns true if this running query has timed out. In other words, it returns whether this has been running
     * longer than the query duraton.
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
