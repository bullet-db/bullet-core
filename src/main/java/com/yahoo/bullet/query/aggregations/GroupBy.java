/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.querying.aggregations.TupleSketchingStrategy;
import com.yahoo.bullet.querying.aggregations.Strategy;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroupBy extends Aggregation {
    private static final long serialVersionUID = -6974294259446732772L;
    private static final BulletException GROUP_BY_REQUIRES_FIELDS =
            new BulletException("GROUP BY requires at least one field.", "Please group by at least one field.");
    private static final BulletException COUNT_FIELD_INVALID_OPERATION =
            new BulletException("COUNT_FIELD is not a valid operation.", "Please remove this operation.");

    private final Map<String, String> fields;
    @Getter
    private final Set<GroupOperation> operations;

    /**
     * Constructor that creates a GROUP aggregation with a specified max size and fields.
     *
     * @param size The max size of the GROUP aggregation. Can be null.
     * @param fields The non-null and non-empty fields to group by.
     * @param operations The non-null set of group operations. Can be empty.
     */
    public GroupBy(Integer size, Map<String, String> fields, Set<GroupOperation> operations) {
        super(size, AggregationType.GROUP);
        Utilities.requireNonNull(fields);
        Utilities.requireNonNull(operations);
        if (fields.isEmpty()) {
            throw GROUP_BY_REQUIRES_FIELDS;
        }
        if (operations.stream().anyMatch(operation -> operation.getType() == GroupOperation.GroupOperationType.COUNT_FIELD)) {
            throw COUNT_FIELD_INVALID_OPERATION;
        }
        this.fields = fields;
        this.operations = operations;
    }

    @Override
    public Strategy getStrategy(BulletConfig config) {
        return new TupleSketchingStrategy(this, config);
    }

    @Override
    public List<String> getFields() {
        return new ArrayList<>(fields.keySet());
    }

    /**
     * Gets a map from field names to aliases.
     *
     * @return A {@link Map} from field names to aliases.
     */
    public Map<String, String> getFieldsToNames() {
        return fields;
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", fields: " + fields + ", operations: " + operations + "}";
    }
}
