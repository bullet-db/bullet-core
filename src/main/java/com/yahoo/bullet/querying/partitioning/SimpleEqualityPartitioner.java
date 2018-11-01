/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.partitioning;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.FilterClause;
import com.yahoo.bullet.parsing.LogicalClause;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
 * The {@link #getKeys(Query)} returns a list of size 1. This means the queries need not be duplicated after
 * partitioning. However, {@link #getKeys(BulletRecord)} will return a list of keys representing the queries that this
 * record needs to presented to. The size of this can be up to 2^(# of fields), where each of the keys is the list
 * of all combinations of null and the actual value in the record for each field.
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
    public static final String NO_FIELD = Type.NULL_EXPRESSION;

    private LinkedHashSet<String> fields;
    private String delimiter;
    private final List<String> defaultKeys;

    /**
     * Constructor that takes a {@link BulletConfig} instance with definitions for the various settings this needs.
     * Delimiter: {@link BulletConfig#EQUALITY_PARTITIONER_DELIMITER} and
     * Fields to partition on: {@link BulletConfig#EQUALITY_PARTITIONER_FIELDS}
     *
     * @param config The non-null config containing settings for this class.
     */
    public SimpleEqualityPartitioner(BulletConfig config) {
        delimiter = config.getAs(BulletConfig.EQUALITY_PARTITIONER_DELIMITER, String.class);
        List<String> fieldsList = (List<String>) config.getAs(BulletConfig.EQUALITY_PARTITIONER_FIELDS, List.class);
        fields = new LinkedHashSet<>(fieldsList);
        String defaultKey = Collections.nCopies(fields.size(), NO_FIELD).stream().collect(Collectors.joining(delimiter));
        defaultKeys = Collections.singletonList(defaultKey);
    }

    /**
     * {@inheritDoc}
     *
     * This partitioner ensures that queries are not stored in duplicate by returning only key for a query (the list
     * that is returned is of size 1).
     *
     * @param query The query to partition.
     * @return The {@link List} containing the one key for this query.
     */
    @Override
    public List<String> getKeys(Query query) {
        Objects.requireNonNull(query);
        List<Clause> filters = query.getFilters();
        // If no filters or has non ANDs, default partition
        if (filters == null || filters.isEmpty() || hasNonANDLogicals(filters)) {
            return defaultKeys;
        }

        // For each unique field, get all the FilterClauses that define an operation on it.
        Map<String, List<FilterClause>> fieldFilters = new HashMap<>();
        filters.forEach(c -> this.mapEqualityFilters(c, fieldFilters));

        // If not one equality filter per field and not one value per filter, default partition
        if (fieldFilters.values().stream().anyMatch(this::hasInvalidFilterClauses)) {
            return defaultKeys;
        }

        // Generate key in fields order and pad with NO_FIELD if no mapping present
        String key = fields.stream().map(fieldFilters::get).map(this::getFilterValue).collect(Collectors.joining(delimiter));
        // For the SimpleEqualityPartitioner, the query is mapped to exactly one key only.
        return Collections.singletonList(key);
    }

    @Override
    public List<String> getKeys(BulletRecord record) {
        return generateKeyCombinations(getFieldValues(record));
    }

    private static boolean hasNonANDLogicals(List<Clause> filters) {
        return filters.stream().anyMatch(SimpleEqualityPartitioner::hasNonANDLogicals);
    }

    private static boolean hasNonANDLogicals(Clause clause) {
        if (clause instanceof FilterClause) {
            return false;
        }
        return clause.getOperation() != Clause.Operation.AND || hasNonANDLogicals(((LogicalClause) clause).getClauses());
    }

    private void mapEqualityFilters(Clause clause, Map<String, List<FilterClause>> mapping) {
        if (clause instanceof FilterClause) {
            mapEqualityFilter((FilterClause) clause, mapping);
        }
        List<Clause> clauses = ((LogicalClause) clause).getClauses();
        if (clauses != null) {
            clauses.forEach(c -> this.mapEqualityFilters(c, mapping));
        }
    }

    private void mapEqualityFilter(FilterClause clause, Map<String, List<FilterClause>> mapping) {
        String field = clause.getField();
        if (clause.getOperation() != Clause.Operation.EQUALS || !fields.contains(field)) {
            return;
        }
        List<FilterClause> list = mapping.getOrDefault(field, new ArrayList<>());
        list.add(clause);
        mapping.put(field, list);
    }

    private boolean hasInvalidFilterClauses(List<FilterClause> filters) {
        if (filters == null || filters.size() != 1)  {
            return false;
        }
        FilterClause filter = filters.get(0);
        List values = filter.getValues();
        return values != null && values.size() == 1;
    }

    private String getFilterValue(List<FilterClause> singletonFilters) {
        if (singletonFilters == null) {
            return NO_FIELD;
        }
        // Otherwise, it's a list of size 1 with a singular value, which has been already validated
        FilterClause filter = singletonFilters.get(0);
        Object value = filter.getValues().get(0);
        return filter.getValue(value);
    }

    private Map<String, String> getFieldValues(BulletRecord record) {
        Map<String, String> fieldValues = new HashMap<>();
        for (String field : fields) {
            Object value = record.get(field);
            fieldValues.put(field, value == null ? NO_FIELD : value.toString());
        }
        return fieldValues;
    }

    private List<String> generateKeyCombinations(Map<String, String> values) {
        Set<String> keys = new HashSet<>();
        return new ArrayList<>(keys);
    }
}
