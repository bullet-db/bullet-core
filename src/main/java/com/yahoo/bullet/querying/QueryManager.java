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
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @param <S> Any wrapper object around a {@link Query} that extends {@link QueryWrapper}.
 *
 * This class wraps the concept of the {@link Partitioner} and allows you to control using your configured (or not)
 * partitioner, what queries (wrapped in the given {@link QueryWrapper}) are seen for each {@link BulletRecord}.  You
 * can get the queries relevant to your record (after applying any partitioner) using {@link #partition(BulletRecord)}.
 * You can use the {@link #addQuery(String, QueryWrapper)} to add a query to the manager and the
 * {@link #removeAndGetQuery(String)} and {@link #removeQueries(Set)} methods to remove a query from the manager.
 */
@Slf4j
public class QueryManager<S extends QueryWrapper> {
    protected Map<String, Set<String>> partitioning;
    protected Map<String, S> queries;
    private Partitioner partitioner;
    private long queriesSeen = 0;
    private long expectedQueriesSeen = 0;

    public static final int QUANTILE_STEP = 10;

    public enum PartitionStat {
        QUERY_COUNT, PARTITION_COUNT, ACTUAL_QUERIES_SEEN, EXPECTED_QUERIES_SEEN,
        STDDEV_PARTITION_SIZE, LARGEST_PARTITION, SMALLEST_PARTITION, DISTRIBUTION_PARTITION_SIZE
    }

    // Exposed for testing.
    static class Partition implements Comparable<Partition> {
        private final String name;
        private final int count;
        static final String DELIMITER = " -> ";

        private Partition(Map.Entry<String, Set<String>> partition) {
            name = partition.getKey();
            count = partition.getValue().size();
        }

        @Override
        public int compareTo(Partition o) {
            return count - o.count;
        }

        @Override
        public String toString() {
            return name + DELIMITER + count;
        }
    }

    private static class NoPartitioner implements Partitioner {
        private static final Set<String> EMPTY_KEYS = Collections.singleton("");

        @Override
        public Set<String> getKeys(Query query) {
            return EMPTY_KEYS;
        }

        @Override
        public Set<String> getKeys(BulletRecord record) {
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
     * Adds a {@link QueryWrapper} to the manager.
     *
     * @param id The query ID.
     * @param query A {@link QueryWrapper} instance.
     */
    public void addQuery(String id, S query) {
        Query wrappedQuery = query.getQuery();
        Set<String> keys = partitioner.getKeys(wrappedQuery);
        for (String key : keys) {
            Set<String> partition = partitioning.getOrDefault(key, new HashSet<>());
            partition.add(id);
            partitioning.put(key, partition);
            log.debug("Added query: %s to partition: %s", id, key);
        }
        queries.put(id, query);
    }

    /**
     * Removes and returns a {@link QueryWrapper} from the manager. The manager does not have any information
     * pertaining to the query any longer.
     *
     * @param id The query ID to remove.
     * @return The removed {@link QueryWrapper} instance.
     */
    public S removeAndGetQuery(String id) {
        S query = queries.remove(id);
        if (query != null) {
            Query wrappedQuery = query.getQuery();
            Set<String> keys = partitioner.getKeys(wrappedQuery);
            for (String key : keys) {
                partitioning.get(key).remove(id);
                log.debug("Removed query: %s from partition: %s", id, key);
            }
        }
        return query;
    }

    /**
     * Removes and returns the {@link List} of {@link QueryWrapper} instances for the given non-null query IDs. The
     * manager does not have any information pertaining to these queries after.
     *
     * @param ids The non-null {@link Set} of query IDs to remove.
     * @return The removed {@link List} of {@link QueryWrapper} instances.
     */
    public List<S> removeAndGetQueries(Set<String> ids) {
        return ids.stream().map(this::removeAndGetQuery).filter(Objects::nonNull).collect(Collectors.toList());
    }

    /**
     * Removes all the queries for the given non-null {@link Set} of query IDs from the manager completely. Use
     * {@link #removeAndGetQueries(Set)} to get these queries.
     *
     * @param ids The non-null {@link Set} of query IDs to remove.
     */
    public void removeQueries(Set<String> ids) {
        ids.forEach(this::removeAndGetQuery);
    }

    /**
     * Retrieves a query stored in the manager or null, if not found.
     *
     * @param id The ID of the query.
     * @return The {@link QueryWrapper} instance or null, if not present.
     */
    public S getQuery(String id) {
        return queries.get(id);
    }

    /**
     * Takes a {@link BulletRecord} instance and returns the matching queries (according to the {@link Partitioner})
     * for it as as {@link Map} of query IDs to the {@link QueryWrapper} instances.
     *
     * @param record The non-null {@link BulletRecord} instance.
     * @return The non-null {@link Map} of matching queries for the record.
     */
    public Map<String, S> partition(BulletRecord record) {
        Set<String> keys = partitioner.getKeys(record);
        Map<String, S> queriesToSee = new HashMap<>();
        for (String key : keys) {
            Set<String> queryIDs = partitioning.getOrDefault(key, Collections.emptySet());
            queryIDs.forEach(id -> queriesToSee.put(id, queries.get(id)));
        }
        int queriesSeen = queriesToSee.size();
        int allQueries = queries.size();
        this.queriesSeen += queriesSeen;
        expectedQueriesSeen += allQueries;
        log.trace("Retrieved %d/%d queries for record: %s", queriesSeen, allQueries, record);
        return queriesToSee;
    }

    /**
     * Gets some statistics about the current state of partitioning and queries in this manager.
     *
     * @return A {@link Map} of {@link PartitionStat} to their values for the current state of the manager.
     */
    public Map<PartitionStat, Object> getStats() {
        Map<PartitionStat, Object> stats = new HashMap<>();
        List<Partition> sorted = partitioning.entrySet().stream().map(Partition::new).sorted().collect(Collectors.toList());
        int size = sorted.size();
        stats.put(PartitionStat.QUERY_COUNT, queries.size());
        stats.put(PartitionStat.PARTITION_COUNT, size);
        stats.put(PartitionStat.ACTUAL_QUERIES_SEEN, queriesSeen);
        stats.put(PartitionStat.EXPECTED_QUERIES_SEEN, expectedQueriesSeen);
        if (size > 0) {
            stats.put(PartitionStat.LARGEST_PARTITION, sorted.get(size - 1).toString());
            stats.put(PartitionStat.SMALLEST_PARTITION, sorted.get(0).toString());
            double[] sizes = sorted.stream().mapToDouble(p -> (double) p.count).toArray();
            stats.put(PartitionStat.STDDEV_PARTITION_SIZE, new StandardDeviation().evaluate(sizes));
            stats.put(PartitionStat.DISTRIBUTION_PARTITION_SIZE, getDistributions(sorted));
        }
        return stats;
    }

    private List<String> getDistributions(List<Partition> sorted) {
        int size = sorted.size();
        int step = size <= QUANTILE_STEP ? 1 : size / QUANTILE_STEP;
        List<Partition> quantiles = new ArrayList<>();
        for (int i = 0; i < size; i += step) {
            quantiles.add(sorted.get(i));
        }
        return quantiles.stream().map(Partition::toString).collect(Collectors.toList());
    }
}
