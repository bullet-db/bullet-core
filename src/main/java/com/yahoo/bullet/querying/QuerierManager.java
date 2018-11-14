/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.record.BulletRecord;

import java.util.Map;

/**
 * It also uses the {@link QueryCategorizer} to categorize your queries for you if you use the
 * {@link #categorize()} (for categorizing all queries) or {@link #categorize(BulletRecord)} (for categorizing all
 * partitioned queries for a record).
 */
public class QuerierManager extends QueryManager<Querier> {
    /**
     * {@inheritDoc}.
     *
     * @param config {@inheritDoc}.
     */
    public QuerierManager(BulletConfig config) {
        super(config);
    }

    /**
     * Categorizes all the queries in the manager regardless of partitioning, using a {@link QueryCategorizer}.
     *
     * @return The {@link QueryCategorizer} instance with all the categorized queries in the manager.
     */
    public QueryCategorizer categorize() {
        return categorize(queries);
    }

    /**
     * Categorizes only the queries for the {@link BulletRecord} after partitioning using the {@link QueryCategorizer}.
     *
     * @param record The {@link BulletRecord} to consume for the partitioned queries.
     * @return The {@link QueryCategorizer} instance with the categorized queries in the manager after partitioning.
     */
    public QueryCategorizer categorize(BulletRecord record) {
        return categorize(record, partition(record));
    }

    private QueryCategorizer categorize(Map<String, Querier> queries) {
        return new QueryCategorizer().categorize(queries);
    }

    private QueryCategorizer categorize(BulletRecord record, Map<String, Querier> queries) {
        return new QueryCategorizer().categorize(record, queries);
    }
}
