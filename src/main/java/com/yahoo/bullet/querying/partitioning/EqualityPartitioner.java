/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.partitioning;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.FilterClause;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.record.BulletRecord;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This partitioner uses a list of fields to partition. If fields A and B are used to partition, this partitioner
 * tries to make sure that queries with equality filters on A and/or B are partitioned appropriately and makes sure
 * that records with values for A and/or B end up seeing a subset of queries that have equality filters for those
 * values.
 *
 * If the fields being equality filtered on in the query are present with multiple values, only the first of them
 * will be used. This simple partitioner is meant to be used for queries with binary, ANDed equality filters.
 *
 * Ex: A == foo AND B.c == bar AND D == null, using the fields [A, B.c, D] will make sure that records will values
 * of foo, bar and null for those fields, will be seen only by those queries that are have those filters (and others).
 * It will also be seen by those queries that have no filters for any of these fields.
 */
public class EqualityPartitioner implements Partitioner {
    public static final String NO_FILTER = "";

    private List<String> fields;
    private String delimiter;

    /**
     * Constructor that takes a {@link BulletConfig} instance with definitions for the various settings this needs.
     * Delimiter: {@link BulletConfig#EQUALITY_PARTITIONER_DELIMITER} and
     * Fields to partition on: {@link BulletConfig#EQUALITY_PARTITIONER_FIELDS}
     *
     * @param config The non-null config containing settings for this class.
     */
    public EqualityPartitioner(BulletConfig config) {
        delimiter = config.getAs(BulletConfig.EQUALITY_PARTITIONER_DELIMITER, String.class);
        fields = (List<String>) config.getAs(BulletConfig.EQUALITY_PARTITIONER_FIELDS, List.class);
    }

    @Override
    public String getKey(Query query) {
        Objects.requireNonNull(query);
        List<Clause> filters = query.getFilters();
        if (filters == null || filters.isEmpty()) {
            return NO_FILTER;
        }
        return null;
    }

    @Override
    public String getKey(BulletRecord record) {
        return null;
    }

    private static Stream<FilterClause> getEqualityFilters(Clause clause) {
        return null;
    }
}
