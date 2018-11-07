/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.querying.partitioning.Partitioner;
import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class QueryManager {
    private Map<String, Set<String>> partitioning;
    private Map<String, Querier> queries;
    private Partitioner partitioner;

    private static class NoPartitioner implements Partitioner {
        public static final List<String> EMPTY_KEYS = Collections.singletonList("");
        @Override
        public List<String> getKeys(Query query) {
            return EMPTY_KEYS;
        }

        @Override
        public List<String> getKeys(BulletRecord record) {
            return EMPTY_KEYS;
        }
    }

    /**
     * The constructor that takes a non-null {@link BulletConfig} instance that contains partitioning settings.
     *
     * @param config The non-null config.
     */
    public QueryManager(BulletConfig config) {
        boolean enable = config.getAs(BulletConfig.QUERY_PARTITIONER_ENABLE, Boolean.class);
        if (enable) {
            partitioner = config.loadConfiguredClass(BulletConfig.QUERY_PARTITIONER_CLASS_NAME);
            log.info("Partitioning for queries is enabled. Using %s", partitioner.getClass().getName());
        } else {
            partitioner = new NoPartitioner();
        }
        partitioning = new HashMap<>();
        queries = new HashMap<>();
    }

    /**
     * Adds a query instance to
     * @param id
     * @param querier
     */
    public void addQuery(String id, Querier querier) {
        Query query = querier.getRunningQuery().getQuery();
        List<String> keys = partitioner.getKeys(query);
        for (String key : keys) {
            Set<String> partition = partitioning.getOrDefault(key, new HashSet<>());
            partition.add(id);
            partitioning.put(key, partition);
        }
        queries.put(id, querier);
    }

    public Querier removeQuery(String id) {
        Querier querier = queries.get(id);
        if (querier != null) {
            Query query = querier.getRunningQuery().getQuery();
            List<String> keys = partitioner.getKeys(query);
            for (String key : keys) {
                partitioning.get(key).remove(id);
            }
        }
        return querier;
    }

    public Querier getQuery(String id) {
        return queries.get(id);
    }

    public Map<String, Querier> getQueries(BulletRecord record) {
        List<String> keys = partitioner.getKeys(record);
        Map<String, Querier> queriers = new HashMap<>();
        for (String key : keys) {
            Set<String> queryIDs = partitioning.get(key);
            if (queryIDs != null) {
                queryIDs.forEach(id -> queriers.put(id, queries.get(id)));
            }
        }
        log.debug("Retrieved %d/%d queries for record: %s", queriers.size(), queries.size(), record);
        return queriers;
    }
}
