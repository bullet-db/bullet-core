/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.AggregationOperations.AggregationType;
import com.yahoo.bullet.aggregations.Distribution.DistributionType;
import com.yahoo.bullet.querying.FilterOperations.FilterType;
import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.querying.Querier;
import com.yahoo.bullet.typesystem.Type;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**.
 * This class deliberately doesn't use GSON to make JSON in order to avoid any GSON specific changes or
 * misconfiguration (new fields added etc.) in the main code and tries to emulate what the parser will get from an
 * external client making an actual call.
 */
public class QueryUtils {
    @SafeVarargs
    public static String makeGroupFilterQuery(String field, List<String> values, FilterType operation,
                                              AggregationType aggregation, Integer size,
                                              List<GroupOperation> operations, Pair<String, String>... fields) {
        return "{" +
                "'filters' : [" + makeFilter(field, values, operation) + "], " +
                "'aggregation' : " + makeGroupAggregation(size, aggregation, operations, fields) +
                "}";
    }

    @SafeVarargs
    public static String makeGroupFilterQuery(List<Clause> clauses, FilterType operation,
                                              AggregationType aggregation, Integer size,
                                              List<GroupOperation> operations, Pair<String, String>... fields) {
        return "{" +
                "'filters' : [" + makeFilter(clauses, operation) + "], " +
                "'aggregation' : " + makeGroupAggregation(size, aggregation, operations, fields) +
                "}";
    }

    @SafeVarargs
    public static String makeRawFullQuery(String field, List<String> values, FilterType operation,
                                          AggregationType aggregation, Integer size,
                                          Pair<String, String>... projections) {
        return "{" +
               "'filters' : [" + makeFilter(field, values, operation) + "], " +
               "'projection' : " + makeProjections(projections) + ", " +
               "'aggregation' : " + makeSimpleAggregation(size, aggregation) +
               "}";
    }

    @SafeVarargs
    public static String makeRawFullQuery(List<Clause> clauses, FilterType operation,
                                          AggregationType aggregation, Integer size,
                                          Pair<String, String>... projections) {
        return "{" +
               "'filters' : [" + makeFilter(clauses, operation) + "], " +
               "'projection' : " + makeProjections(projections) + ", " +
               "'aggregation' : " + makeSimpleAggregation(size, aggregation) +
               "}";
    }

    @SafeVarargs
    public static String makeProjectionFilterQuery(String field, List<String> values, FilterType operation,
                                                   Pair<String, String>... projections) {
        return "{" +
               "'filters' : [" + makeFilter(field, values, operation) + "], " +
               "'projection': " + makeProjections(projections) +
               "}";
    }

    @SafeVarargs
    public static String makeProjectionFilterQuery(List<Clause> clauses, FilterType operation,
                                                   Pair<String, String>... projections) {
        return "{" +
               "'filters' : [" + makeFilter(clauses, operation) + "], " +
               "'projection': " + makeProjections(projections) +
               "}";
    }

    public static String makeSimpleAggregationFilterQuery(String field, List<String> values, FilterType operation,
                                                          AggregationType aggregation, Integer size) {
        return "{" +
               "'filters' : [" + makeFilter(field, values, operation) + "], " +
               "'aggregation' : " + makeSimpleAggregation(size, aggregation) +
               "}";
    }

    public static String makeSimpleAggregationFilterQuery(List<Clause> clauses, FilterType operation,
                                                          AggregationType aggregation, Integer size) {
        return "{" +
               "'filters' : [" + makeFilter(clauses, operation) + "], " +
               "'aggregation' : " + makeSimpleAggregation(size, aggregation) +
               "}";
    }

    public static String makeFilterQuery(String field, List<String> values, FilterType operation) {
        return "{'filters' : [" + makeFilter(field, values, operation) + "]}";
    }

    public static String makeFilterQuery(List<Clause> values, FilterType operation) {
        return "{'filters' : [" + makeFilter(values, operation) + "]}";
    }

    public static String makeFilterQuery(FilterType operation, Clause... values) {
        return "{'filters': [" + makeFilter(operation, values) + "]}";
    }

    public static String makeFieldFilterQuery(String value) {
        return makeFilterQuery("field", Collections.singletonList(value), FilterType.EQUALS);
    }

    @SafeVarargs
    public static String makeProjectionQuery(Pair<String, String>... projections) {
        return "{'projection' : " + makeProjections(projections) + "}";
    }

    public static String makeAggregationQuery(AggregationType operation, Integer size) {
        return "{'aggregation' : " + makeSimpleAggregation(size, operation) + "}";
    }

    @SafeVarargs
    public static String makeAggregationQuery(AggregationType operation, Integer size, Map<String, String> attributes,
                                              Pair<String, String>... fields) {
        return "{'aggregation' : " + makeStringAttributesAggregation(size, operation, attributes, fields) + "}";
    }

    public static String makeAggregationQuery(AggregationType operation, Integer size, DistributionType type,
                                              String field, List<Double> points, Double start, Double end,
                                              Double increment, Integer numberOfPoints) {
        return "{'aggregation' : " + makeDistributionAggregation(size, operation, type, field, points, start, end,
                                                                 increment, numberOfPoints) + "}";
    }

    @SafeVarargs
    public static String makeAggregationQuery(AggregationType operation, Integer size, Long threshold,
                                              String newName, Pair<String, String>... fields) {
        return "{'aggregation' : " + makeTopKAggregation(size, operation, threshold, newName, fields) + "}";
    }

    public static String makeFilter(String field, List<String> values, FilterType operation) {
        return "{" +
                "'field' : " + makeString(field) + ", " +
                "'operation' : " + makeString(getOperationFor(operation)) + ", " +
                "'values' : ['" + values.stream().reduce((a, b) -> a + "' , '" + b).orElse("") + "']" +
                "}";
    }

    public static String makeFilter(List<Clause> values, FilterType operation) {
        return "{" +
               "'operation' : " + makeString(getOperationFor(operation)) + ", " +
               "'clauses' : [" + values.stream().map(QueryUtils::toString).reduce((a, b) -> a + " , " + b).orElse("") + "]" +
               "}";
    }

    public static String makeFilter(FilterType operation, Clause... values) {
        return makeFilter(values == null ? Collections.emptyList() : Arrays.asList(values), operation);
    }

    @SafeVarargs
    public static String makeProjections(Pair<String, String>... pairs) {
        return "{'fields' : " + makeMap(pairs) + "}";
    }

    public static String makeSimpleAggregation(Integer size, AggregationType operation) {
        return "{'type' : '" + getOperationFor(operation) + "', 'size' : " + size + "}";
    }

    @SafeVarargs
    public static String makeGroupAggregation(Integer size, AggregationType operation, List<GroupOperation> operations,
                                              Pair<String, String>... fields) {
        return "{" +
                 "'type' : '" + getOperationFor(operation) + "', " +
                 "'fields' : " + makeGroupFields(fields) + ", " +
                 "'attributes' : {" +
                    "'operations' : [" +
                        operations.stream().map(QueryUtils::makeGroupOperation).reduce((a, b) -> a + " , " + b).orElse("") +
                    "]" +
                 "}, " +
                 "'size' : " + size +
               "}";
    }

    public static String makeDistributionAggregation(Integer size, AggregationType operation, DistributionType type,
                                                     String field, List<Double> points, Double start, Double end,
                                                     Double increment, Integer numberOfPoints) {
        return "{" +
                 "'type' : '" + getOperationFor(operation) + "', " +
                 "'fields' : " + makeGroupFields(Pair.of(field, field)) + ", " +
                 "'attributes' : " + makeMap(type, points, start, end, increment, numberOfPoints) + ", " +
                 "'size' : " + size +
               "}";
    }

    @SafeVarargs
    public static String makeTopKAggregation(Integer size, AggregationType operation, Long threshold,
                                             String newName, Pair<String, String>... fields) {
        return "{" +
                "'type' : '" + getOperationFor(operation) + "', " +
                "'fields' : " + makeGroupFields(fields) + ", " +
                "'attributes' : " + makeMap(threshold, newName) + ", " +
                "'size' : " + size +
                "}";
    }

    @SafeVarargs
    public static String makeStringAttributesAggregation(Integer size, AggregationType operation,
                                                         Map<String, String> attributes,
                                                         Pair<String, String>... fields) {
        return "{" +
                 "'type' : '" + getOperationFor(operation) + "', " +
                 "'fields' : " + makeGroupFields(fields) + ", " +
                 "'size' : " + size + ", " +
                 "'attributes' : " + makeMap(attributes) +
                "}";
    }

    @SafeVarargs
    public static String makeGroupFields(Pair<String, String>... fields) {
        if (fields == null) {
            return "null";
        }
        return makeMap(fields);

    }

    public static String makeGroupOperation(GroupOperation operation) {
        return "{" +
                 "'type' : " + makeString(operation.getType().getName()) + ", " +
                 "'field' : " + makeString(operation.getField()) + ", " +
                 "'newName' : " + makeString(operation.getNewName()) +
                "}";
    }

    public static String makeString(String field) {
        return field != null ? "'" + field + "'" : "null";

    }

    public static String makeMap(DistributionType type, List<Double> points, Double start, Double end, Double increment,
                                 Integer numberOfPoints) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("'" + Distribution.TYPE + "' : '").append(type.getName()).append("', ");

        if (points != null) {
            builder.append("'" + Distribution.POINTS + "' : [")
                   .append(points.stream().map(Object::toString).collect(Collectors.joining(",")))
                   .append("]");
        } else if (numberOfPoints != null) {
            builder.append("'" + Distribution.NUMBER_OF_POINTS + "' : ").append(numberOfPoints);
        } else {
            builder.append("'" + Distribution.RANGE_START + "' : ").append(start).append(", ");
            builder.append("'" + Distribution.RANGE_END + "' : ").append(end).append(", ");
            builder.append("'" + Distribution.RANGE_INCREMENT + "' : ").append(increment);
        }
        builder.append("}");
        return builder.toString();
    }

    public static String makeMap(Long threshold, String newName) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        if (threshold != null) {
            builder.append("'" + TopK.THRESHOLD_FIELD + "' : ").append(threshold);
        }
        if (threshold != null && newName != null) {
            builder.append(", ");
        }
        if (newName != null) {
            builder.append("'" + TopK.NEW_NAME_FIELD + "' : ").append(newName);
        }
        builder.append("}");
        return builder.toString();
    }

    @SafeVarargs
    public static String makeMap(Pair<String, String>... pairs) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");

        String delimiter = "";
        for (Pair<String, String> pair : pairs) {
            builder.append(delimiter)
                   .append("'").append(pair.getKey()).append("'")
                   .append(" : '").append(pair.getValue()).append("'");
            delimiter = ", ";
        }
        builder.append("}");
        return builder.toString();
    }

    public static String makeMap(Map<String, String> map) {
        if (map == null) {
            return "null";
        }
        return makeMap(map.entrySet().toArray(new Pair[0]));
    }

    public static Clause makeClause(FilterType operation, Clause... clauses) {
        LogicalClause clause = new LogicalClause();
        clause.setOperation(operation);
        if (clauses != null) {
            clause.setClauses(Arrays.asList(clauses));
        }
        clause.initialize();
        return clause;
    }

    public static Clause makeClause(String field, List<String> values, FilterType operation) {
        FilterClause clause = new FilterClause();
        clause.setField(field);
        clause.setValues(values == null ? Collections.singletonList(Type.NULL_EXPRESSION) : values);
        clause.setOperation(operation);
        clause.initialize();
        return clause;
    }

    // Again, not implementing toString in Clause to not tie the construction of the JSON to the src.
    public static String toString(Clause clause) {
        StringBuilder builder = new StringBuilder();
        if (clause instanceof FilterClause) {
            FilterClause filterClause = (FilterClause) clause;
            builder.append(makeFilter(filterClause.getField(), filterClause.getValues(), filterClause.getOperation()));
        } else if (clause instanceof LogicalClause) {
            LogicalClause logicalClause = (LogicalClause) clause;
            builder.append(makeFilter(logicalClause.getClauses(), logicalClause.getOperation()));
        }
        return builder.toString();
    }

    public static String getOperationFor(FilterType operation) {
        switch (operation) {
            case EQUALS:
                return "==";
            case NOT_EQUALS:
                return "!=";
            case GREATER_THAN:
                return ">";
            case LESS_THAN:
                return "<";
            case GREATER_EQUALS:
                return ">=";
            case LESS_EQUALS:
                return "<=";
            case AND:
                return "AND";
            case OR:
                return "OR";
            case NOT:
                return "NOT";
            default:
                return "";
        }
    }

    public static String getOperationFor(AggregationType operation) {
        switch (operation) {
            case TOP_K:
                return "TOP K";
            case RAW:
                return "RAW";
            case GROUP:
                return "GROUP";
            case COUNT_DISTINCT:
                return "COUNT DISTINCT";
            case DISTRIBUTION:
                return "DISTRIBUTION";
            default:
                return "";
        }
    }
}
