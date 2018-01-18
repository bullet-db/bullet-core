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
 * A wrapper for a query.
 */
@Getter
public class RunningQuery implements Initializable {
    private final String id;
    private final long startTime;
    private final Query query;
    private String queryString;

    /**
     * Creates an instance of a Query object from the given String version of the query and an ID. It does also not
     * initialize it.
     *
     * @param id The String query ID.
     * @param queryString The String version of the query.
     * @param config The configuration to use for the query.
     * @throws com.google.gson.JsonParseException if there were issues parsing the query.
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
    public RunningQuery(String id, Query query) {
        this.id = id;
        this.query = query;
        startTime = System.currentTimeMillis();
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        return query.initialize();
    }

    @Override
    public String toString() {
        String body = queryString != null ? queryString : query.toString();
        return String.format("%s : %s", id, body);
    }
}
