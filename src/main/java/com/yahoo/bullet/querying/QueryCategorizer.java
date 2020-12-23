/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.record.BulletRecord;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * This categorizes running queries into whether they are done, closed, have exceeded the rate limits or have new data.
 * Running queries are provided as a {@link Map} of String query IDs to non-null, valid, initialized {@link Querier}
 * objects.
 * <p>
 * Use {@link #categorize(Map)} and {@link #categorize(BulletRecord, Map)}for categorizing queries. The latter
 * categorizes after making the Querier instances {@link Querier#consume(BulletRecord)}.
 */
@Getter @Slf4j
public class QueryCategorizer {
    private Map<String, Querier> rateLimited = new HashMap<>();
    private Map<String, Querier> closed = new HashMap<>();
    private Map<String, Querier> done = new HashMap<>();
    private Map<String, Querier> hasData = new HashMap<>();

    /**
     * Categorize the given {@link Map} of query IDs to {@link Querier} instances.
     *
     * @param queries The queries to categorize.
     * @return This object for chaining.
     */
    public QueryCategorizer categorize(Map<String, Querier> queries) {
        queries.entrySet().forEach(this::classify);
        return this;
    }

    /**
     * Categorize the given {@link Map} of query IDs to {@link Querier} instances after consuming the given record.
     *
     * @param record The {@link BulletRecord} to consume first.
     * @param queries The queries to categorize.
     * @return This object for chaining.
     */
    public QueryCategorizer categorize(BulletRecord record, Map<String, Querier> queries) {
        for (Map.Entry<String, Querier> query : queries.entrySet()) {
            query.getValue().consume(record);
            classify(query);
        }
        return this;
    }

    private void classify(Map.Entry<String, Querier> query) {
        String id = query.getKey();
        Querier querier = query.getValue();
        if (querier.isDone()) {
            done.put(id, querier);
        } else if (querier.isExceedingRateLimit()) {
            rateLimited.put(id, querier);
        } else if (querier.isClosed()) {
            closed.put(id, querier);
        } else if (querier.hasNewData()) {
            hasData.put(id, querier);
        }
    }
}
