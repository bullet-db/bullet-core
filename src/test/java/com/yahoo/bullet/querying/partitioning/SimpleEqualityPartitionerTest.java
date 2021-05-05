/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.partitioning;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.aggregations.Raw;
import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.ListExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

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

    private Query createQuery() {
        Query query = new Query(new Projection(), null, new Raw(null), null, new Window(), null);
        query.configure(config);
        return query;
    }

    private Query createQuery(Expression filter) {
        Query query = new Query(new Projection(), filter, new Raw(null), null, new Window(), null);
        query.configure(config);
        return query;
    }

    private Query createQuery(Expression... filters) {
        Expression filter = filters[0];
        for (int i = 1; i < filters.length; i++) {
            filter = new BinaryExpression(filter, filters[i], Operation.AND);
        }
        return createQuery(filter);
    }

    @Test
    public void testDefaultPartitioningQueryWithNoFilters() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Assert.assertEquals(partitioner.getKeys(createQuery()), singleton("*-*"));
    }

    @Test
    public void testDefaultPartitioningQueryWithNoEqualityFilters() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(new FieldExpression("abc"));
        Assert.assertEquals(partitioner.getKeys(query), singleton("*-*"));
    }

    @Test
    public void testDefaultPartitioningQueryWithNullEqualityFilters() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(new BinaryExpression(new FieldExpression("A"), new ValueExpression(null), Operation.EQUALS),
                                  new BinaryExpression(new FieldExpression("B"), new ValueExpression(null), Operation.EQUALS));
        Assert.assertEquals(partitioner.getKeys(query), singleton("null-null"));
    }

    @Test
    public void testDefaultPartitioningQueryWithUnrelatedFilters() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(new BinaryExpression(new FieldExpression("C"), new ValueExpression("bar"), Operation.EQUALS),
                                  new BinaryExpression(new FieldExpression("D"), new ValueExpression("baz"), Operation.EQUALS));
        Assert.assertEquals(partitioner.getKeys(query), singleton("*-*"));
    }

    @Test
    public void testDefaultPartitioningQueryWithNonEqualityFilters() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(new BinaryExpression(new FieldExpression("A"), new ValueExpression("bar"), Operation.NOT_EQUALS),
                                  new BinaryExpression(new FieldExpression("B"), new ValueExpression("baz"), Operation.NOT_EQUALS));
        Assert.assertEquals(partitioner.getKeys(query), singleton("*-*"));
    }

    @Test
    public void testDefaultPartitioningQueryWithOR() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(new BinaryExpression(new BinaryExpression(new FieldExpression("A"), new ValueExpression("bar"), Operation.EQUALS),
                                                       new BinaryExpression(new FieldExpression("B"), new ValueExpression("baz"), Operation.EQUALS),
                                                       Operation.OR));
        Assert.assertEquals(partitioner.getKeys(query), singleton("*-*"));
    }

    @Test
    public void testDefaultPartitioningQueryWithNOT() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(new UnaryExpression(new BinaryExpression(new FieldExpression("A"), new ValueExpression("bar"), Operation.EQUALS),
                                                      Operation.NOT));
        Assert.assertEquals(partitioner.getKeys(query), singleton("*-*"));
    }

    @Test
    public void testDefaultPartitioningForQueryWithMultipleValues() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(new BinaryExpression(new FieldExpression("A"), new ValueExpression("foo"), Operation.EQUALS),
                                  new BinaryExpression(new FieldExpression("A"), new ValueExpression("bar"), Operation.EQUALS),
                                  new BinaryExpression(new FieldExpression("B"), new ValueExpression("baz"), Operation.EQUALS));
        Assert.assertEquals(partitioner.getKeys(query), singleton("*-*"));
    }

    @Test
    public void testDefaultPartitioningForQueryWthImproperBinaryEquals() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A");
        Query query = createQuery(new BinaryExpression(new FieldExpression("A"), new FieldExpression("B"), Operation.EQUALS),
                                  new BinaryExpression(new FieldExpression("A"), new FieldExpression("B"), Operation.EQUALS),
                                  new BinaryExpression(new ValueExpression("foo"), new ValueExpression("bar"), Operation.EQUALS),
                                  new BinaryExpression(new ListExpression(emptyList()), new ListExpression(emptyList()), Operation.EQUALS));
        Assert.assertEquals(partitioner.getKeys(query), singleton("*"));
    }

    @Test
    public void testPartitioningForQueryWithMissingFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(new BinaryExpression(new FieldExpression("A"), new ValueExpression("bar"), Operation.EQUALS));
        Assert.assertEquals(partitioner.getKeys(query), singleton("bar.-*"));
    }

    @Test
    public void testPartitioningForQueryWithNullCheckedFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        Query query = createQuery(new BinaryExpression(new FieldExpression("A"), new ValueExpression("null"), Operation.EQUALS),
                                  new BinaryExpression(new FieldExpression("B"), new ValueExpression(null), Operation.EQUALS));

        Assert.assertEquals(partitioner.getKeys(query), singleton("null.-null"));
    }

    @Test
    public void testPartitioningForQueryWithAllFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B", "C");
        Query query = createQuery(new BinaryExpression(new FieldExpression("A"), new ValueExpression("bar"), Operation.EQUALS),
                                  new BinaryExpression(new FieldExpression("B"), new ValueExpression("baz"), Operation.EQUALS),
                                  new BinaryExpression(new FieldExpression("C"), new ValueExpression("qux"), Operation.EQUALS));
        Assert.assertEquals(partitioner.getKeys(query), singleton("bar.-baz.-qux."));
    }

    @Test
    public void testPartitioningForQueryWithAllFieldsOperandsFlipped() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B", "C");
        Query query = createQuery(new BinaryExpression(new ValueExpression("bar"), new FieldExpression("A"), Operation.EQUALS),
                                  new BinaryExpression(new ValueExpression("baz"), new FieldExpression("B"), Operation.EQUALS),
                                  new BinaryExpression(new ValueExpression("qux"), new FieldExpression("C"), Operation.EQUALS));
        Assert.assertEquals(partitioner.getKeys(query), singleton("bar.-baz.-qux."));
    }

    @Test
    public void testPartitioningForQueryWithNestedFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B", "C", "D.e");
        Query query = createQuery(new BinaryExpression(new FieldExpression("A"), new ValueExpression("bar"), Operation.EQUALS),
                                  new BinaryExpression(new FieldExpression("B"), new ValueExpression("quux"), Operation.EQUALS),
                                  new BinaryExpression(new FieldExpression("C"), new ValueExpression("qux"), Operation.EQUALS),
                                  new BinaryExpression(new FieldExpression("D", "e"), new ValueExpression("norf"), Operation.EQUALS));
        Assert.assertEquals(partitioner.getKeys(query), singleton("bar.-quux.-qux.-norf."));
    }

    @Test
    public void testNoPartitioningForQueryWithExpressionFields() {
        FieldExpression fieldExpression = new FieldExpression("A", new ValueExpression("b"));
        SimpleEqualityPartitioner partitioner = createPartitioner(fieldExpression.getName());
        Query query = createQuery(new BinaryExpression(fieldExpression, new ValueExpression("bar"), Operation.EQUALS));
        Assert.assertEquals(partitioner.getKeys(query), singleton("*"));
    }

    @Test
    public void testPartitioningForRecordWithMissingFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        BulletRecord record = RecordBox.get().add("A", "foo").getRecord();
        Set<String> expected = new HashSet<>(asList("foo.-null", "foo.-*", "*-null", "*-*"));
        Set<String> actual = partitioner.getKeys(record);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testPartitioningForRecordWithNulledFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B");
        BulletRecord record = RecordBox.get().add("A", "null").getRecord();
        Set<String> expected = new HashSet<>(asList("null.-null", "null.-*", "*-null", "*-*"));
        Set<String> actual = partitioner.getKeys(record);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testPartitioningForRecordWithMultiplePresentAndMissingFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B", "C", "D");
        BulletRecord record = RecordBox.get().add("B", "foo").add("D", "baz").getRecord();
        Set<String> expected = new HashSet<>(asList("null-foo.-null-baz.", "null-foo.-null-*", "null-foo.-*-baz.", "null-foo.-*-*",
                                                    "null-*-null-baz.", "null-*-null-*", "null-*-*-baz.", "null-*-*-*",
                                                    "*-foo.-null-baz.", "*-foo.-null-*", "*-foo.-*-baz.", "*-foo.-*-*",
                                                    "*-*-null-baz.", "*-*-null-*", "*-*-*-baz.", "*-*-*-*"));
        Set<String> actual = partitioner.getKeys(record);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testPartitioningForRecordWithAllFields() {
        SimpleEqualityPartitioner partitioner = createPartitioner("A", "B", "C.d");
        BulletRecord record = RecordBox.get().add("A", "foo").add("B", "bar").addMap("C", ImmutablePair.of("d", "baz")).getRecord();
        Set<String> expected = new HashSet<>(asList("foo.-bar.-baz.",
                                                    "foo.-bar.-*", "foo.-*-baz.", "*-bar.-baz.",
                                                    "foo.-*-*", "*-*-baz.", "*-bar.-*",
                                                    "*-*-*"));
        Set<String> actual = partitioner.getKeys(record);
        Assert.assertEquals(actual, expected);
    }
}
