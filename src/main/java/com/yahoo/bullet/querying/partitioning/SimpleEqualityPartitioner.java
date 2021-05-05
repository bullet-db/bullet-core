/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.partitioning;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This partitioner uses a list of fields to partition. If fields A and B are used to partition, this partitioner
 * tries to make sure that queries with equality filters on A and/or B are partitioned appropriately and makes sure
 * that records with values for A and/or B end up seeing a subset of queries that have ANDed, equality filters for those
 * values.
 *
 * This simple partitioner is only to be used for partitioning queries with ANDed equality filters with one value for
 * each filter. The latter criteria can be relaxed in the future but for now, it is not supported. It will default
 * partition the query if:
 *
 * 1) ORs or NOTs are detected in the filter
 * 2) If a field being equality filtered on is present with multiple values in filter
 * 3) If the query has no filters
 *
 * Ex: A == foo AND B.c == bar AND D == null, using the fields [A, B.c, D] will make sure that records will values
 * of foo, bar and null for those fields, will be seen only by those queries that are have those filters (or subsets
 * of it), including queries with no filters on these fields (and queries with no filters at all).
 *
 * The {@link #getKeys(Query)} returns a set of size 1. This means the queries need not be duplicated after
 * partitioning. However, {@link #getKeys(BulletRecord)} will return a set of keys representing the queries that this
 * record needs to presented to. The size of this can be up to 2^(# of fields), where each of the keys is the join
 * of all combinations of null and the actual value in the record for each field in field order with the delimiter.
 *
 * Ex: If fields are [A, B.c, D] and a record has these values: [A: foo, B.c: bar, D: baz, ...], the keys will be the
 * the concatenation of the following items in each tuple using the configured delimiter (not necessarily in this order):
 *
 * [foo, bar, baz]
 * [foo, null, baz]
 * [foo, bar, null]
 * [foo, null, null]
 * [null, bar, baz]
 * [null, bar, null]
 * [null, null, baz]
 * [null, null, null]
 *
 * Using these keys and presenting the record to all the queries with the same key will ensure that the record is
 * seen by exactly only the queries that need to see it.
 */
public class SimpleEqualityPartitioner implements Partitioner {
    /*
    NULL represents the null value (as opposed to the string "null"). ANY represents all values and is a wildcard used
    when a field doesn't have a filter, i.e. the field's value does not matter.
    */
    private static final String ANY = "*";
    private static final String NULL = "null";
    private static final int LOWEST_BIT_MASK = 1;
    private static final int ZERO = 0;
    // This appends this char to all non-null values to disambiguate them if they actually had NO_FIELD as their values
    public static final char DISAMBIGUATOR = '.';

    private List<String> fields;
    private Set<String> fieldSet;
    private String delimiter;
    private final Set<String> defaultKeys;

    /**
     * Constructor that takes a {@link BulletConfig} instance with definitions for the various settings this needs.
     * Delimiter: {@link BulletConfig#EQUALITY_PARTITIONER_DELIMITER} and
     * Fields to partition on: {@link BulletConfig#EQUALITY_PARTITIONER_FIELDS}
     *
     * @param config The non-null config containing settings for this class.
     */
    public SimpleEqualityPartitioner(BulletConfig config) {
        delimiter = config.getAs(BulletConfig.EQUALITY_PARTITIONER_DELIMITER, String.class);
        fields = (List<String>) config.getAs(BulletConfig.EQUALITY_PARTITIONER_FIELDS, List.class);
        fieldSet = new HashSet<>(fields);
        String defaultKey = Collections.nCopies(fields.size(), ANY).stream().collect(Collectors.joining(delimiter));
        defaultKeys = Collections.singleton(defaultKey);
    }

    /**
     * {@inheritDoc}
     *
     * This partitioner ensures that queries are not stored in duplicate by returning only key for a query (the list
     * that is returned is of size 1).
     *
     * @param query {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override
    public Set<String> getKeys(Query query) {
        Objects.requireNonNull(query);

        Expression filter = query.getFilter();

        // If no filter, default partition
        if (filter == null) {
            return defaultKeys;
        }

        // Map each field to the values that it is checked for equality against
        Map<String, Set<Serializable>> equalityClauses = new HashMap<>();
        mapFieldsToValues(filter, equalityClauses);

        // If not exactly one equality per field, default partition
        if (equalityClauses.values().stream().anyMatch(set -> set.size() != 1)) {
            return defaultKeys;
        }

        // Generate key in fields order and pad with NO_FIELD if no mapping present
        String key = fields.stream().map(equalityClauses::get).map(this::getFilterValue).collect(Collectors.joining(delimiter));

        // For the SimpleEqualityPartitioner, the query is mapped to exactly one key only.
        return Collections.singleton(key);
    }

    @Override
    public Set<String> getKeys(BulletRecord record) {
        Map<String, String> values = getFieldValues(record);
        /*
         * Generate a truth table for all possible combinations of the fields when using the field value or not using
         * an integer to represent a binary of fields.size() chars where each one represents to include or not include
         * the field. Note, fields that are not present are NULL-mapped. When not included, they are ANY-mapped.
         */
        return IntStream.range(0, 1 << fields.size()).mapToObj(i -> binaryToKey(i, values)).collect(Collectors.toSet());
    }

    private void mapFieldsToValues(Expression expression, Map<String, Set<Serializable>> mapping) {
        if (!(expression instanceof BinaryExpression)) {
            return;
        }
        BinaryExpression binary = (BinaryExpression) expression;
        if (binary.getOp() == Operation.AND) {
            mapFieldsToValues(binary.getLeft(), mapping);
            mapFieldsToValues(binary.getRight(), mapping);
        } else if (binary.getOp() == Operation.EQUALS) {
            if (binary.getLeft() instanceof FieldExpression && binary.getRight() instanceof ValueExpression) {
                addFieldToMapping((FieldExpression) binary.getLeft(), (ValueExpression) binary.getRight(), mapping);
            } else if (binary.getRight() instanceof FieldExpression && binary.getLeft() instanceof ValueExpression) {
                addFieldToMapping((FieldExpression) binary.getRight(), (ValueExpression) binary.getLeft(), mapping);
            }
        }
    }

    private void addFieldToMapping(FieldExpression fieldExpression, ValueExpression valueExpression, Map<String, Set<Serializable>> mapping) {
        if (fieldExpression.getKey() instanceof Expression || fieldExpression.getSubKey() instanceof Expression) {
            return;
        }
        String field = fieldExpression.getName();
        if (fieldSet.contains(field)) {
            Serializable value = valueExpression.getValue();
            mapping.computeIfAbsent(field, s -> new HashSet<>()).add(value);
        }
    }

    private String getFilterValue(Set<Serializable> values) {
        if (values == null) {
            return ANY;
        }
        Serializable value = values.iterator().next();
        if (value == null) {
            return NULL;
        }
        return makeKeyEntry(value.toString());
    }

    private Map<String, String> getFieldValues(BulletRecord record) {
        Map<String, String> fieldValues = new HashMap<>();
        for (String field : fields) {
            TypedObject value = record.typedExtract(field);
            fieldValues.put(field, value.isNull() ? NULL : makeKeyEntry(value.getValue().toString()));
        }
        return fieldValues;
    }

    private String binaryToKey(int number, Map<String, String> values) {
        // If binary is 011 and fields is [A, B.c, D], the key is [values[A], values[B.c], ANY].join(delimiter)
        return IntStream.range(0, fields.size()).mapToObj(i -> getValueForIndex(number, i, values))
                                                .collect(Collectors.joining(delimiter));
    }

    private String getValueForIndex(int number, int index, Map<String, String> values) {
        boolean shouldPick = ((number >> index) & LOWEST_BIT_MASK) != ZERO;
        return shouldPick ? values.get(fields.get(index)) : ANY;
    }

    private String makeKeyEntry(String value) {
        return value + DISAMBIGUATOR;
    }
}
