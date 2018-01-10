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

    /**
     * Creates an instance of a Query object from the given String version of the query and an id. It does also not
     * initialize it.
     *
     * @param id The String query id.
     * @param queryString The String version of the query.
     * @param config The configuration to use for the query.
     * @throws com.google.gson.JsonParseException if there were issues parsing the query.
     */
    public RunningQuery(String id, String queryString, BulletConfig config) {
        this.id = id;
        query = Parser.parse(queryString, config);
        startTime = System.currentTimeMillis();
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        return query.initialize();
    }
}
