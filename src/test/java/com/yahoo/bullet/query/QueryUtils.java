/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.aggregations.CountDistinct;
import com.yahoo.bullet.query.aggregations.Distribution;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.aggregations.GroupAll;
import com.yahoo.bullet.query.aggregations.GroupBy;
import com.yahoo.bullet.query.aggregations.LinearDistribution;
import com.yahoo.bullet.query.aggregations.Raw;
import com.yahoo.bullet.query.aggregations.TopK;
import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.ListExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryUtils {
    public static Query makeGroupAllFilterQuery(Expression filter, List<GroupOperation> operations) {
        Query query = new Query(new Projection(), filter, new GroupAll(new HashSet<>(operations)), null, new Window(), null);
        query.configure(new BulletConfig());
        return query;
    }

    public static Query makeGroupAllFieldFilterQuery(String field, List<Serializable> values, Operation op, List<GroupOperation> operations) {
        Expression filter = new BinaryExpression(new FieldExpression(field),
                                                 new ListExpression(values.stream().map(ValueExpression::new).collect(Collectors.toList())),
                                                 op);
        return makeGroupAllFilterQuery(filter, operations);
    }

    public static Query makeGroupByFilterQuery(Expression filter, int size, Map<String, String> fields, List<GroupOperation> operations) {
        Query query = new Query(new Projection(), filter, new GroupBy(size, fields, new HashSet<>(operations)), null, new Window(), null);
        query.configure(new BulletConfig());
        return query;
    }

    public static Query makeProjectionFilterQuery(Expression filter, List<Field> fields) {
        Query query = new Query(new Projection(fields, false), filter, new Raw(1), null, new Window(), null);
        query.configure(new BulletConfig());
        return query;
    }

    public static Query makeProjectionQuery(List<Field> fields) {
        return makeProjectionFilterQuery(null, fields);
    }

    public static Query makeSimpleAggregationFilterQuery(Expression filter, Integer size, Window.Unit emit, Integer emitValue,
                                                         Window.Unit include, Integer includeValue) {
        Window window = WindowUtils.makeWindow(emit, emitValue, include, includeValue);
        Query query = new Query(new Projection(), filter, new Raw(size), null, window, null);
        query.configure(new BulletConfig());
        return query;
    }

    public static Query makeSimpleAggregationFieldFilterQuery(Serializable value, Integer size, Window.Unit emit, Integer emitValue,
                                                              Window.Unit include, Integer includeValue) {
        Expression filter = new BinaryExpression(new FieldExpression("field"), new ValueExpression(value), Operation.EQUALS);
        return makeSimpleAggregationFilterQuery(filter, size, emit, emitValue, include, includeValue);
    }

    public static Query makeSimpleAggregationQuery(Integer size, Window.Unit emit, Integer emitValue, Window.Unit include,
                                                   Integer includeValue) {
        return makeSimpleAggregationFilterQuery(null, size, emit, emitValue, include, includeValue);
    }

    public static Query makeFilterQuery(Expression filter, Integer size) {
        Query query = new Query(new Projection(), filter, new Raw(size), null, new Window(), null);
        query.configure(new BulletConfig());
        return query;
    }

    public static Query makeFilterQuery(Expression filter) {
        return makeFilterQuery(filter, 1);
    }

    public static Query makeFilterQuery(String field, List<Serializable> values, Operation op) {
        Expression filter = new BinaryExpression(new FieldExpression(field),
                                                 new ListExpression(values.stream().map(ValueExpression::new).collect(Collectors.toList())),
                                                 op);
        return makeFilterQuery(filter);
    }

    public static Query makeFieldFilterQuery(Serializable value, Integer size) {
        return makeFilterQuery(new BinaryExpression(new FieldExpression("field"), new ValueExpression(value), Operation.EQUALS), size);
    }

    public static Query makeFieldFilterQuery(Serializable value) {
        return makeFieldFilterQuery(value, 1);
    }

    public static Query makeRawQuery(Integer size) {
        Query query = new Query(new Projection(), null, new Raw(size), null, new Window(), null);
        query.configure(new BulletConfig());
        return query;
    }

    public static Query makeCountDistinctQuery(List<String> fields, String name) {
        CountDistinct countDistinct = new CountDistinct(fields, name);
        Query query = new Query(new Projection(), null, countDistinct, null, new Window(), null);
        query.configure(new BulletConfig());
        return query;
    }

    public static Query makeDistributionQuery(Integer size, DistributionType type, String field, int numberOfPoints) {
        Distribution distribution = new LinearDistribution(field, type, size, numberOfPoints);
        Query query = new Query(new Projection(), null, distribution, null, new Window(), null);
        query.configure(new BulletConfig());
        return query;
    }

    public static Query makeTopKQuery(Integer size, Long threshold, String name, Map<String, String> fields) {
        TopK topK = new TopK(fields, size, threshold, name);
        Query query = new Query(new Projection(), null, topK, null, new Window(), null);
        query.configure(new BulletConfig());
        return query;
    }
}
