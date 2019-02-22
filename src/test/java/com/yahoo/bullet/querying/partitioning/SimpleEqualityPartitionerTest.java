/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.partitioning;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.FilterClause;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.StringFilterClause;
import com.yahoo.bullet.parsing.Value;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.typesystem.Type;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static com.yahoo.bullet.parsing.FilterUtils.makeClause;
import static com.yahoo.bullet.parsing.FilterUtils.makeObjectClause;
import static com.yahoo.bullet.parsing.FilterUtils.makeStringClause;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public class SimpleEqualityPartitionerTest {
    private BulletConfig config;

    @BeforeClass
    public void setup() {
        config = new BulletConfig();
        config.set(BulletConfig.QUERY_PARTITIONER_ENABLE, true);
        config.set(BulletConfig.QUERY_PARTITIONER_CLASS_NAME, BulletConfig.DEFAULT_QUERY_PARTITIONER_CLASS_NAME);
        config.set(BulletConfig.EQUALITY_PARTITIONER_DELIMITER, "-");
    }

    private SimpleEqualityPartitioner createPartitioner(String... fields) {
        config.set(BulletConfig.EQUALITY_PARTITIONER_FIELDS, asList(fields));
        config.validate();
        return new SimpleEqualityPartitioner(config);
    }

    private Query createQuery(Clause... filters) {
        Query query = new Query();
        if (filters != null) {
            //query.setFilters(asList(filters));
        }
        query.setAggregation(new Aggregation());
        query.configure(config);
        query.initialize();
        return query;
    }

    @Test
    public void testDefaultPartitioningQueryWithNoFilters() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Assert.assertEquals(partitioner.getKeys(createQuery()), singleton("null-null"));
    }

    @Test
    public void testDefaultPartitioningQueryWithNoLogicalFilters() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Assert.assertEquals(partitioner.getKeys(createQuery(makeClause(Clause.Operation.AND))), singleton("null-null"));
    }

    @Test
    public void testDefaultPartitioningQueryWithUnrelatedFilters() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(makeClause("C", singletonList("bar"), Clause.Operation.EQUALS),
                                  makeClause("D", singletonList("baz"), Clause.Operation.EQUALS));
        Assert.assertEquals(partitioner.getKeys(query), singleton("null-null"));
    }

    @Test
    public void testDefaultPartitioningQueryWithNonEqualityFilters() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(makeClause("A", singletonList("bar"), Clause.Operation.REGEX_LIKE),
                                  makeClause("B", singletonList("baz"), Clause.Operation.CONTAINS_KEY));
        Assert.assertEquals(partitioner.getKeys(query), singleton("null-null"));
    }

    @Test
    public void testDefaultPartitioningQueryWithOR() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(makeClause(Clause.Operation.OR,
                                             makeClause("A", singletonList("bar"), Clause.Operation.EQUALS),
                                             makeClause("B", singletonList("baz"), Clause.Operation.EQUALS)));
        Assert.assertEquals(partitioner.getKeys(query), singleton("null-null"));
    }

    @Test
    public void testDefaultPartitioningQueryWithNOT() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(makeClause(Clause.Operation.NOT, makeClause("A", singletonList("bar"), Clause.Operation.EQUALS)));
        Assert.assertEquals(partitioner.getKeys(query), singleton("null-null"));
    }

    @Test
    public void testDefaultPartitioningForQueryWithMissingValues() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A");
        FilterClause clause = new StringFilterClause();
        clause.setField("A");
        clause.setValues(null);
        clause.setOperation(Clause.Operation.EQUALS);
        Query query = createQuery(clause);
        Assert.assertEquals(partitioner.getKeys(query), singleton("null"));
    }

    @Test
    public void testDefaultPartitioningForQueryWithMultipleValues() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(makeClause(Clause.Operation.AND,
                                             makeClause("A", asList("foo", "bar"), Clause.Operation.EQUALS),
                                             makeClause("B", singletonList("baz"), Clause.Operation.EQUALS)));
        Assert.assertEquals(partitioner.getKeys(query), singleton("null-null"));
    }

    @Test
    public void testDefaultPartitioningForQueryWithRepeatedFilters() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(makeClause(Clause.Operation.AND,
                                             makeClause(Clause.Operation.AND,
                                                        makeClause("A", singletonList("quux"), Clause.Operation.EQUALS),
                                                        makeClause("B", singletonList("norf"), Clause.Operation.EQUALS)),
                                             makeClause(Clause.Operation.AND,
                                                        makeClause("B", singletonList("qux"), Clause.Operation.EQUALS),
                                                        makeClause("A", singletonList("bar"), Clause.Operation.EQUALS))));
        Assert.assertEquals(partitioner.getKeys(query), singletonList("null-null"));
    }

    @Test
    public void testPartitioningForQueryWithMissingFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(makeClause(Clause.Operation.AND, makeClause("A", singletonList("bar"), Clause.Operation.EQUALS)));
        Assert.assertEquals(partitioner.getKeys(query), singleton("bar.-null"));
    }

    @Test
    public void testPartitioningForQueryWithNullCheckedFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        // Has an ObjectFilterClause with type forced to STRING (so a == "null" as opposed to is not null)
        Query query = createQuery(makeClause(Clause.Operation.AND,
                                             makeObjectClause("A", singletonList(new Value(Value.Kind.VALUE, Type.NULL_EXPRESSION, Type.STRING)), Clause.Operation.EQUALS),
                                             makeStringClause("B", null, Clause.Operation.EQUALS)));

        Assert.assertEquals(partitioner.getKeys(query), singleton("null.-null"));
    }

    @Test
    public void testPartitioningForQueryWithAllFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B", "C");
        Query query = createQuery(makeClause(Clause.Operation.AND,
                                             makeClause("C", singletonList("qux"), Clause.Operation.EQUALS),
                                             makeClause("B", singletonList("baz"), Clause.Operation.EQUALS),
                                             makeClause("A", singletonList("bar"), Clause.Operation.EQUALS)));
        Assert.assertEquals(partitioner.getKeys(query), singleton("bar.-baz.-qux."));
    }

    @Test
    public void testPartitioningForQueryWithNestedFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B", "C", "D.e");
        Query query = createQuery(makeClause(Clause.Operation.AND,
                                             makeClause(Clause.Operation.AND,
                                                        makeClause("B", singletonList("quux"), Clause.Operation.EQUALS),
                                                        makeClause("D.e", singletonList("norf"), Clause.Operation.EQUALS)),
                                             makeClause(Clause.Operation.AND,
                                                        makeClause("C", singletonList("qux"), Clause.Operation.EQUALS),
                                                        makeClause("A", singletonList("bar"), Clause.Operation.EQUALS))));
        Assert.assertEquals(partitioner.getKeys(query), singleton("bar.-quux.-qux.-norf."));
    }

    @Test
    public void testPartitioningForRecordWithMissingFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        BulletRecord record = RecordBox.get().add("A", "foo").getRecord();
        Set<String> expected = new HashSet<>(asList("null-null", "foo.-null"));
        Set<String> actual = partitioner.getKeys(record);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testPartitioningForRecordWithNulledFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        BulletRecord record = RecordBox.get().add("A", "null").getRecord();
        Set<String> expected = new HashSet<>(asList("null.-null", "null-null"));
        Set<String> actual = partitioner.getKeys(record);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testPartitioningForRecordWithMultiplePresentAndMissingFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B", "C", "D");
        BulletRecord record = RecordBox.get().add("B", "foo").add("D", "baz").getRecord();
        Set<String> expected = new HashSet<>(asList("null-foo.-null-null", "null-null-null-null",
                                                    "null-null-null-baz.", "null-foo.-null-baz."));
        Set<String> actual = partitioner.getKeys(record);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testPartitioningForRecordWithAllFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B", "C.d");
        BulletRecord record = RecordBox.get().add("A", "foo").add("B", "bar").addMap("C", ImmutablePair.of("d", "baz")).getRecord();
        Set<String> expected = new HashSet<>(asList("foo.-bar.-baz.",
                                                    "foo.-bar.-null", "foo.-null-baz.", "null-bar.-baz.",
                                                    "foo.-null-null", "null-null-baz.", "null-bar.-null",
                                                    "null-null-null"));
        Set<String> actual = partitioner.getKeys(record);
        Assert.assertEquals(actual, expected);
    }
}
