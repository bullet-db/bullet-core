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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This partitioner uses a list of fields to partition. If fields A and B are used to partition, this partitioner
 * tries to make sure that queries with equality filters on A and/or B are partitioned appropriately and makes sure
 * that records with values for A and/or B end up seeing a subset of queries that have equality filters for those
 * values.
 *
 * If the fields being equality filtered on in the query are present with multiple values, only the last of them
 * will be used. This simple partitioner is meant to be used for partitioning queries with binary, ANDed equality
 * filters. If you OR the same filter on a field
 *
 * Ex: A == foo AND B.c == bar AND D == null, using the fields [A, B.c, D] will make sure that records will values
 * of foo, bar and null for those fields, will be seen only by those queries that are have those filters (and others).
 * It will also be seen by those queries that have no filters for any of these fields.
 */
public class SimpleEqualityPartitioner implements Partitioner {
    public static final String DEFAULT_PARTITION = "";
    public static final String NO_FIELD = "";

    private LinkedHashSet<String> fields;
    private String delimiter;

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
    }

    @Override
    public String getKey(Query query) {
        Objects.requireNonNull(query);
        List<Clause> filters = query.getFilters();
        // If not one equality filter per field, we have to default partition.
        if (filters == null || filters.isEmpty()) {
            return DEFAULT_PARTITION;
        }

        Map<String, List<FilterClause>> fieldFilters = new HashMap<>();
        filters.forEach(c -> this.mapEqualityFilters(c, fieldFilters));

        // If not one equality filter per field, we have to default partition.
        if (fieldFilters.values().stream().anyMatch(this::validateFilter)) {
            return DEFAULT_PARTITION;
        }
        // Generate Key in fields order and pad with NO_FIELD if no mapping present
        return fields.stream().map(fieldFilters::get).map(this::getFilterValue).collect(Collectors.joining(delimiter));
    }

    @Override
    public String getKey(BulletRecord record) {
        return null;
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

    private String getFilterValue(List<FilterClause> filters) {
        if (filters == null) {
            return NO_FIELD;
        }
        // Otherwise, it's a list of size 1 with a singular value
        FilterClause filter = filters.get(0);
        Object value = filter.getValues().get(0);
        return filter.getValue(value);

    }

    private boolean validateFilter(List<FilterClause> filters) {
        if (filters == null || filters.size() != 1)  {
            return false;
        }
        FilterClause filter = filters.get(0);
        List values = filter.getValues();
        return values != null && values.size() == 1;
    }
}
