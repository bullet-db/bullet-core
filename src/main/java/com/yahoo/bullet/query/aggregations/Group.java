/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.querying.aggregations.GroupStrategy;
import com.yahoo.bullet.querying.aggregations.TupleSketchingStrategy;
import com.yahoo.bullet.querying.aggregations.Strategy;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Group extends Aggregation {
    private static final long serialVersionUID = -6974294259446732772L;
    private static final BulletException COUNT_FIELD_INVALID_OPERATION =
            new BulletException("COUNT_FIELD is not a valid operation.", "Please remove this operation.");

    private Map<String, String> fields;
    private Set<GroupOperation> operations;

    /**
     * Constructor that creates a GROUP aggregation with a specified max size.
     *
     * @param size The max size of the GROUP aggregation. Can be null.
     */
    public Group(Integer size) {
        super(size, AggregationType.GROUP);
    }

    @Override
    public Strategy getStrategy(BulletConfig config) {
        return hasFields() ? new TupleSketchingStrategy(this, config) : new GroupStrategy(this, config);
    }

    @Override
    public List<String> getFields() {
        return hasFields() ? new ArrayList<>(fields.keySet()) : Collections.emptyList();
    }

    /**
     * Gets a map from field names to aliases.
     *
     * @return A {@link Map} from field names to aliases. If there are no fields, returns an empty map.
     */
    public Map<String, String> getFieldsToNames() {
        return fields != null ? fields : Collections.emptyMap();
    }

    private boolean hasFields() {
        return fields != null && !fields.isEmpty();
    }

    /**
     * Sets the map of field names to aliases for this aggregation.
     *
     * @param fields The map of field names to aliases to set. Must not be null.
     */
    public void setFields(Map<String, String> fields) {
        this.fields = Utilities.requireNonNull(fields);
    }

    /**
     * Gets the set of {@link GroupOperation} in this aggregation.
     *
     * @return The set of {@link GroupOperation} in this aggregation. If there are no operations, returns an empty set.
     */
    public Set<GroupOperation> getOperations() {
        return operations != null ? operations : Collections.emptySet();
    }

    /**
     * Adds a {@link GroupOperation} to this aggregation.
     *
     * @param type The type of {@link GroupOperation} to add.
     * @param field The field of the {@link GroupOperation} to add.
     * @param name The name of the {@link GroupOperation} to add.
     */
    public void addGroupOperation(GroupOperation.GroupOperationType type, String field, String name) {
        addGroupOperation(new GroupOperation(type, field, name));
    }

    /**
     * Adds a {@link GroupOperation} to this aggregation.
     *
     * @param groupOperation The {@link GroupOperation} to add.
     */
    public void addGroupOperation(GroupOperation groupOperation) {
        if (groupOperation.getType() == GroupOperation.GroupOperationType.COUNT_FIELD) {
            throw COUNT_FIELD_INVALID_OPERATION;
        }
        if (operations == null) {
            operations = new HashSet<>();
        }
        operations.add(groupOperation);
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", fields: " + fields + ", operations: " + operations + "}";
    }
}
