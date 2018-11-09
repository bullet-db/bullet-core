/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.querying.partitioning.SimpleEqualityPartitioner;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.yahoo.bullet.parsing.Clause.Operation.EQUALS;
import static com.yahoo.bullet.parsing.FilterUtils.makeClause;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class QueryManagerTest {
    private static Querier getQuerier(Query query) {
        Querier querier = QueryCategorizerTest.makeQuerier(false, false, false);
        RunningQuery runningQuery = mock(RunningQuery.class);
        when(runningQuery.getQuery()).thenReturn(query);
        when(querier.getRunningQuery()).thenReturn(runningQuery);
        return querier;
    }

    @SafeVarargs
    private static Query getQuery(Pair<String, String>... equalities) {
        Query query = new Query();
        if (equalities != null) {
            List<Clause> clauses = Arrays.stream(equalities)
                                         .map(e -> makeClause(e.getKey(), singletonList(e.getValue()), EQUALS))
                                         .collect(Collectors.toList());
            query.setFilters(clauses);
        }
        query.setAggregation(new Aggregation());
        query.configure(new BulletConfig());
        query.initialize();
        return query;
    }

    private static BulletConfig getEqualityPartitionerConfig(String... fields) {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.QUERY_PARTITIONER_ENABLE, true);
        config.set(BulletConfig.QUERY_PARTITIONER_CLASS_NAME, BulletConfig.DEFAULT_QUERY_PARTITIONER_CLASS_NAME);
        config.set(BulletConfig.EQUALITY_PARTITIONER_DELIMITER, "-");
        config.set(BulletConfig.EQUALITY_PARTITIONER_FIELDS, asList(fields));
        return config.validate();
    }

    private static void addQuerier(QueryManager manager, int i, int j, Map<String, Querier> queriers) {
        String value = String.valueOf(i);
        String id = String.valueOf((i * 100) + j);
        Query query = getQuery(ImmutablePair.of("A", value));
        Querier querier = getQuerier(query);
        queriers.put(id, querier);
        manager.addQuery(id, querier);
    }

    private static String makePartition(String rawName, int count) {
        return rawName + SimpleEqualityPartitioner.DISAMBIGUATOR + QueryManager.Partition.DELIMITER + String.valueOf(count);

    }

    @Test
    public void testAddingAndRemovingQueries() {
        QueryManager manager = new QueryManager(getEqualityPartitionerConfig("A", "B"));
        Query queryA = getQuery(ImmutablePair.of("A", "foo"));
        Query queryB = getQuery(ImmutablePair.of("A", "foo"), ImmutablePair.of("B", "bar"));
        Querier querierA = getQuerier(queryA);
        Querier querierB = getQuerier(queryB);
        Querier querierC = getQuerier(queryA);
        Querier querierD = getQuerier(queryA);
        Querier querierE = getQuerier(queryB);

        manager.addQuery("idA", querierA);
        manager.addQuery("idB", querierB);
        manager.addQuery("idC", querierC);
        manager.addQuery("idD", querierD);
        manager.addQuery("idE", querierE);
        Assert.assertSame(manager.getQuery("idA"), querierA);
        Assert.assertSame(manager.getQuery("idC"), querierC);
        Assert.assertSame(manager.getQuery("idD"), querierD);
        Assert.assertSame(manager.getQuery("idB"), querierB);
        Assert.assertSame(manager.getQuery("idE"), querierE);

        Assert.assertNull(manager.removeAndGetQuery("fake"));
        Assert.assertSame(manager.removeAndGetQuery("idC"), querierC);
        List<Querier> removed = manager.removeAndGetQueries(new LinkedHashSet<>(asList("idE", "idB", "idC")));
        Assert.assertEquals(removed.size(), 2);
        Assert.assertSame(removed.get(0), querierE);
        Assert.assertSame(removed.get(1), querierB);

        Assert.assertNotNull(manager.getQuery("idA"));
        Assert.assertNotNull(manager.getQuery("idD"));
        Assert.assertNull(manager.getQuery("idB"));
        Assert.assertNull(manager.getQuery("idC"));
        Assert.assertNull(manager.getQuery("idE"));

        manager.removeQueries(new HashSet<>(asList("idD", "idA")));
        Assert.assertNull(manager.getQuery("idA"));
        Assert.assertNull(manager.getQuery("idD"));
    }

    @Test
    public void testNoPartitioning() {
        QueryManager manager = new QueryManager(new BulletConfig());
        Query queryA = getQuery(ImmutablePair.of("A", "foo"));
        Query queryB = getQuery(ImmutablePair.of("A", "foo"), ImmutablePair.of("B", "bar"));
        Query queryC = getQuery();
        Querier querierA = getQuerier(queryA);
        Querier querierB = getQuerier(queryB);
        Querier querierC = getQuerier(queryC);
        manager.addQuery("idA", querierA);
        manager.addQuery("idB", querierB);
        manager.addQuery("idC", querierC);

        BulletRecord recordA = RecordBox.get().add("A", "foo").getRecord();
        BulletRecord recordB = RecordBox.get().add("A", "foo").add("B", "bar").getRecord();
        BulletRecord recordC = RecordBox.get().getRecord();

        Map<String, Querier> partitionA = manager.partition(recordA);
        Map<String, Querier> partitionB = manager.partition(recordB);
        Map<String, Querier> partitionC = manager.partition(recordC);

        Assert.assertEquals(partitionA.size(), 3);
        Assert.assertSame(partitionA.get("idA"), querierA);
        Assert.assertSame(partitionA.get("idB"), querierB);
        Assert.assertSame(partitionA.get("idC"), querierC);
        Assert.assertEquals(partitionB.size(), 3);
        Assert.assertSame(partitionB.get("idA"), querierA);
        Assert.assertSame(partitionB.get("idB"), querierB);
        Assert.assertSame(partitionB.get("idC"), querierC);
        Assert.assertEquals(partitionC.size(), 3);
        Assert.assertSame(partitionC.get("idA"), querierA);
        Assert.assertSame(partitionC.get("idB"), querierB);
        Assert.assertSame(partitionC.get("idC"), querierC);
    }

    @Test
    public void testPartitioning() {
        QueryManager manager = new QueryManager(getEqualityPartitionerConfig("A", "B"));
        Query queryA = getQuery(ImmutablePair.of("A", "foo"));
        Query queryB = getQuery(ImmutablePair.of("A", "foo"), ImmutablePair.of("B", "bar"));
        Query queryC = getQuery();
        Querier querierA = getQuerier(queryA);
        Querier querierB = getQuerier(queryB);
        Querier querierC = getQuerier(queryC);
        manager.addQuery("idA", querierA);
        manager.addQuery("idB", querierB);
        manager.addQuery("idC", querierC);

        BulletRecord recordA = RecordBox.get().add("A", "foo").getRecord();
        BulletRecord recordB = RecordBox.get().add("A", "foo").add("B", "bar").getRecord();
        BulletRecord recordC = RecordBox.get().getRecord();

        Map<String, Querier> partitionA = manager.partition(recordA);
        Map<String, Querier> partitionB = manager.partition(recordB);
        Map<String, Querier> partitionC = manager.partition(recordC);

        Assert.assertEquals(partitionA.size(), 2);
        Assert.assertSame(partitionA.get("idA"), querierA);
        Assert.assertSame(partitionA.get("idC"), querierC);
        Assert.assertEquals(partitionB.size(), 3);
        Assert.assertSame(partitionB.get("idA"), querierA);
        Assert.assertSame(partitionB.get("idB"), querierB);
        Assert.assertSame(partitionB.get("idC"), querierC);
        Assert.assertEquals(partitionC.size(), 1);
        Assert.assertSame(partitionC.get("idC"), querierC);
    }

    @Test
    public void testCategorizingAll() {
        QueryManager manager = new QueryManager(new BulletConfig());
        Query queryA = getQuery(ImmutablePair.of("A", "foo"));
        Query queryB = getQuery(ImmutablePair.of("A", "foo"), ImmutablePair.of("B", "bar"));
        Querier querierA = getQuerier(queryA);
        Querier querierB = getQuerier(queryB);
        manager.addQuery("idA", querierA);
        manager.addQuery("idB", querierB);

        QueryCategorizer categorizer = manager.categorize();
        Assert.assertEquals(categorizer.getDone().size(), 0);
        Assert.assertEquals(categorizer.getClosed().size(), 0);
        Assert.assertEquals(categorizer.getRateLimited().size(), 0);
        verify(querierA, times(1)).isDone();
        verify(querierA, times(1)).isClosed();
        verify(querierA, times(1)).isExceedingRateLimit();
        verify(querierB, times(1)).isDone();
        verify(querierB, times(1)).isClosed();
        verify(querierB, times(1)).isExceedingRateLimit();
    }

    @Test
    public void testCategorizingNoPartitioning() {
        QueryManager manager = new QueryManager(new BulletConfig());
        Query queryA = getQuery(ImmutablePair.of("A", "foo"));
        Query queryB = getQuery(ImmutablePair.of("A", "foo"), ImmutablePair.of("B", "bar"));
        Querier querierA = getQuerier(queryA);
        Querier querierB = getQuerier(queryB);
        manager.addQuery("idA", querierA);
        manager.addQuery("idB", querierB);

        BulletRecord recordA = RecordBox.get().add("A", "foo").add("B", "bar").getRecord();
        BulletRecord recordB = RecordBox.get().add("A", "foo").getRecord();

        QueryCategorizer categorizer = manager.categorize(recordA);
        Assert.assertEquals(categorizer.getDone().size(), 0);
        Assert.assertEquals(categorizer.getClosed().size(), 0);
        Assert.assertEquals(categorizer.getRateLimited().size(), 0);
        verify(querierA, times(1)).consume(recordA);
        verify(querierB, times(1)).consume(recordA);

        categorizer = manager.categorize(recordB);
        Assert.assertEquals(categorizer.getDone().size(), 0);
        Assert.assertEquals(categorizer.getClosed().size(), 0);
        Assert.assertEquals(categorizer.getRateLimited().size(), 0);
        verify(querierA, times(1)).consume(recordB);
        verify(querierB, times(1)).consume(recordB);
    }

    @Test
    public void testCategorizingPartitioning() {
        QueryManager manager = new QueryManager(getEqualityPartitionerConfig("A", "B"));
        Query queryA = getQuery(ImmutablePair.of("A", "foo"));
        Query queryB = getQuery(ImmutablePair.of("A", "foo"), ImmutablePair.of("B", "bar"));
        Querier querierA = getQuerier(queryA);
        Querier querierB = getQuerier(queryB);
        manager.addQuery("idA", querierA);
        manager.addQuery("idB", querierB);

        BulletRecord recordA = RecordBox.get().add("A", "foo").add("B", "bar").getRecord();
        BulletRecord recordB = RecordBox.get().add("A", "foo").getRecord();

        QueryCategorizer categorizer = manager.categorize(recordA);
        Assert.assertEquals(categorizer.getDone().size(), 0);
        Assert.assertEquals(categorizer.getClosed().size(), 0);
        Assert.assertEquals(categorizer.getRateLimited().size(), 0);
        verify(querierA, times(1)).consume(recordA);
        verify(querierB, times(1)).consume(recordA);

        categorizer = manager.categorize(recordB);
        Assert.assertEquals(categorizer.getDone().size(), 0);
        Assert.assertEquals(categorizer.getClosed().size(), 0);
        Assert.assertEquals(categorizer.getRateLimited().size(), 0);
        verify(querierA, times(1)).consume(recordB);
        verify(querierB, never()).consume(recordB);
    }

    @Test
    public void testSmallStatistics() {
        QueryManager manager = new QueryManager(getEqualityPartitionerConfig("A"));
        Map<String, Querier> queries = new HashMap<>();

        // Adds i partitions from 1 to 5 with i copies of the query that looks for A == i
        for (int i = 1; i <= 5; ++i) {
            for (int j = 1; j <= i; ++j) {
                addQuerier(manager, i, j, queries);
            }
        }
        Assert.assertEquals(queries.size(), 15);
        Map<QueryManager.PartitionStat, Object> stats = manager.getStats();
        Assert.assertEquals(stats.get(QueryManager.PartitionStat.QUERIES), 15);
        Assert.assertEquals(stats.get(QueryManager.PartitionStat.COUNT), 5);
        Assert.assertEquals(stats.get(QueryManager.PartitionStat.LARGEST), makePartition("5", 5));
        Assert.assertEquals(stats.get(QueryManager.PartitionStat.SMALLEST), makePartition("1", 1));
        Assert.assertNotNull(stats.get(QueryManager.PartitionStat.STDDEV));

        List<String> distribution = (List<String>) stats.get(QueryManager.PartitionStat.DISTRIBUTION);

        Assert.assertEquals(distribution.size(), 5);
        Assert.assertEquals(distribution.get(0), makePartition(String.valueOf("1"), 1));
        Assert.assertEquals(distribution.get(1), makePartition(String.valueOf("2"), 2));
        Assert.assertEquals(distribution.get(2), makePartition(String.valueOf("3"), 3));
        Assert.assertEquals(distribution.get(3), makePartition(String.valueOf("4"), 4));
        Assert.assertEquals(distribution.get(4), makePartition(String.valueOf("5"), 5));
    }

    @Test
    public void testLargeStatistics() {
        QueryManager manager = new QueryManager(getEqualityPartitionerConfig("A"));
        Map<String, Querier> queries = new HashMap<>();

        // Make max > QUANTILE_STEP
        final int max = 20;
        // Adds i partitions from 1 to max with i copies of the query that looks for A == i
        for (int i = 1; i <= max; ++i) {
            for (int j = 1; j <= i; ++j) {
                addQuerier(manager, i, j, queries);
            }
        }
        int queryCount = (max * (max + 1)) / 2;
        Assert.assertEquals(queries.size(), queryCount);
        Map<QueryManager.PartitionStat, Object> stats = manager.getStats();
        Assert.assertEquals(stats.get(QueryManager.PartitionStat.QUERIES), queryCount);
        Assert.assertEquals(stats.get(QueryManager.PartitionStat.COUNT), max);
        Assert.assertEquals(stats.get(QueryManager.PartitionStat.LARGEST), makePartition(String.valueOf(max), max));
        Assert.assertEquals(stats.get(QueryManager.PartitionStat.SMALLEST), makePartition("1", 1));
        Assert.assertNotNull(stats.get(QueryManager.PartitionStat.STDDEV));

        List<String> distribution = (List<String>) stats.get(QueryManager.PartitionStat.DISTRIBUTION);

        int step = max / QueryManager.QUANTILE_STEP;
        // If max > QUANTILE_STEP, distribution has QUANTILE_STEP points
        Assert.assertEquals(distribution.size(), QueryManager.QUANTILE_STEP);
        for (int i = 0; i < QueryManager.QUANTILE_STEP; ++i) {
            // Each i represents the 1+(i * step)th partition. Adding one because partitions were generated from 1 not 0.
            int partitionNumber = (i * step) + 1;
            Assert.assertEquals(distribution.get(i), makePartition(String.valueOf(partitionNumber), partitionNumber));
        }
    }
}
