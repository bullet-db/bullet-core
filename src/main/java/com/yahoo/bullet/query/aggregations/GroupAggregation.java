/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.aggregations.GroupAll;
import com.yahoo.bullet.aggregations.GroupBy;
import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroupAggregation extends Aggregation {
    private static final long serialVersionUID = -6974294259446732772L;

    private Map<String, String> fields;
    private Set<GroupOperation> operations;

    /**
     * Constructor that creates a GROUP aggregation with a specified max size.
     *
     * @param size The max size of the GROUP aggregation. Can be null.
     */
    public GroupAggregation(Integer size) {
        super(size, Type.GROUP);
    }

    @Override
    public Strategy getStrategy(BulletConfig config) {
        return hasFields() ? new GroupBy(this, config) : new GroupAll(this, config);
    }

    @Override
    public List<String> getFields() {
        return hasFields() ? new ArrayList<>(fields.keySet()) : Collections.emptyList();
    }

    public Map<String, String> getFieldsToNames() {
        return fields != null ? fields : Collections.emptyMap();
    }

    private boolean hasFields() {
        return fields != null && !fields.isEmpty();
    }

    public void setFields(Map<String, String> fields) {
        this.fields = Utilities.requireNonNull(fields);
    }

    public Set<GroupOperation> getOperations() {
        return operations != null ? operations : Collections.emptySet();
    }

    public void addGroupOperation(GroupOperation.GroupOperationType type, String field, String name) {
        addGroupOperation(new GroupOperation(type, field, name));
    }

    public void addGroupOperation(GroupOperation groupOperation) {
        if (groupOperation.getType() == GroupOperation.GroupOperationType.COUNT_FIELD) {
            throw new BulletException("COUNT_FIELD is not a valid operation.", "Please remove this operation.");
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
