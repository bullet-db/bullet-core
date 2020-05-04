/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query;

/**.
 * This class deliberately doesn't use GSON to make JSON in order to avoid any GSON specific changes or
 * misconfiguration (new fields added etc.) in the main code and tries to emulate what the parser will get from an
 * externally submitted query.
 */
public class QueryUtils {
    /*
    @SafeVarargs
    public static String makeRawWindowQuery(String field, List<String> values, Clause.Operation operation,
                                            Aggregation.Type aggregation, Integer size,  Window.Unit emit,
                                            Integer emitValue, Window.Unit include, Integer includeValue,
                                            Pair<String, String>... projections) {
        return makeRawWindowQuery(field, values, operation, aggregation, size, null,
                                  emit, emitValue, include, includeValue, projections);
    }

    @SafeVarargs
    public static String makeRawWindowQuery(String field, List<String> values, Clause.Operation operation,
                                            Aggregation.Type aggregation, Integer size,
                                            List<String> postAggregations, Window.Unit emit,
                                            Integer emitValue, Window.Unit include, Integer includeValue,
                                            Pair<String, String>... projections) {
        return "{" +
                "'filters' : [" + makeStringFilter(field, values, operation) + "], " +
                "'projection' : " + makeProjections(projections) + ", " +
                "'aggregation' : " + makeSimpleAggregation(size, aggregation) + ", " +
                "'postAggregations' : " + makePostAggregations(postAggregations)  + ", " +
                "'window' : " + makeWindow(emit, emitValue, include, includeValue) +
                "}";
    }

    @SafeVarargs
    public static String makeGroupFilterQuery(String field, List<String> values, Clause.Operation operation,
                                              Aggregation.Type aggregation, Integer size,
                                              List<GroupOperation> operations, Pair<String, String>... fields) {
        return makeGroupFilterQuery(field, values, operation, aggregation, size, null, operations, fields);
    }

    @SafeVarargs
    public static String makeGroupFilterQuery(String field, List<String> values, Clause.Operation operation,
                                              Aggregation.Type aggregation, Integer size, List<String> postAggregations,
                                              List<GroupOperation> operations, Pair<String, String>... fields) {
        return "{" +
                 "'filters' : [" + makeStringFilter(field, values, operation) + "], " +
                 "'aggregation' : " + makeGroupAggregation(size, aggregation, operations, fields) + ", " +
                 "'postAggregations' : " + makePostAggregations(postAggregations)  +
               "}";
    }

    @SafeVarargs
    public static String makeGroupFilterQuery(List<Clause> clauses, Clause.Operation operation,
                                              Aggregation.Type aggregation, Integer size,
                                              List<GroupOperation> operations, Pair<String, String>... fields) {
        return makeGroupFilterQuery(clauses, operation, aggregation, size, null, operations, fields);
    }

    @SafeVarargs
    public static String makeGroupFilterQuery(List<Clause> clauses, Clause.Operation operation,
                                              Aggregation.Type aggregation, Integer size, List<String> postAggregations,
                                              List<GroupOperation> operations, Pair<String, String>... fields) {
        return "{" +
                "'filters' : [" + makeFilter(clauses, operation) + "], " +
                "'aggregation' : " + makeGroupAggregation(size, aggregation, operations, fields) + ", " +
                "'postAggregations' : " + makePostAggregations(postAggregations)  +
                "}";
    }

    @SafeVarargs
    public static String makeRawFullQuery(String field, List<String> values, Clause.Operation operation,
                                          Aggregation.Type aggregation, Integer size,
                                          Pair<String, String>... projections) {
        return makeRawFullQuery(field, values, operation, aggregation, size, null, projections);
    }

    @SafeVarargs
    public static String makeRawFullQuery(String field, List<String> values, Clause.Operation operation,
                                          Aggregation.Type aggregation, Integer size, List<String> postAggregations,
                                          Pair<String, String>... projections) {
        return "{" +
                "'filters' : [" + makeStringFilter(field, values, operation) + "], " +
                "'projection' : " + makeProjections(projections) + ", " +
                "'aggregation' : " + makeSimpleAggregation(size, aggregation) + ", " +
                "'postAggregations' : " + makePostAggregations(postAggregations)  +
                "}";
    }

    @SafeVarargs
    public static String makeRawFullQuery(List<Clause> clauses, Clause.Operation operation,
                                          Aggregation.Type aggregation, Integer size,
                                          Pair<String, String>... projections) {
        return makeRawFullQuery(clauses, operation, aggregation, size, null, projections);
    }

    @SafeVarargs
    public static String makeRawFullQuery(List<Clause> clauses, Clause.Operation operation,
                                          Aggregation.Type aggregation, Integer size, List<String> postAggregations,
                                          Pair<String, String>... projections) {
        return "{" +
                "'filters' : [" + makeFilter(clauses, operation) + "], " +
                "'projection' : " + makeProjections(projections) + ", " +
                "'aggregation' : " + makeSimpleAggregation(size, aggregation) + ", " +
                "'postAggregations' : " + makePostAggregations(postAggregations)  +
                "}";
    }

    @SafeVarargs
    public static String makeProjectionFilterQuery(String field, List<String> values, Clause.Operation operation,
                                                   Pair<String, String>... projections) {
        return makeProjectionFilterQuery(field, values, operation, null, projections);
    }

    @SafeVarargs
    public static String makeProjectionFilterQuery(String field, List<String> values, Clause.Operation operation,
                                                   List<String> postAggregations, Pair<String, String>... projections) {
        return "{" +
                "'filters' : [" + makeStringFilter(field, values, operation) + "], " +
                "'projection': " + makeProjections(projections) + ", " +
                "'postAggregations' : " + makePostAggregations(postAggregations)  +
                "}";
    }

    @SafeVarargs
    public static String makeProjectionFilterQuery(List<Clause> clauses, Clause.Operation operation,
                                                   Pair<String, String>... projections) {
        return makeProjectionFilterQuery(clauses, operation, null, projections);
    }

    @SafeVarargs
    public static String makeProjectionFilterQuery(List<Clause> clauses, Clause.Operation operation,
                                                   List<String> postAggregations, Pair<String, String>... projections) {
        return "{" +
                "'filters' : [" + makeFilter(clauses, operation) + "], " +
                "'projection': " + makeProjections(projections) + ", " +
                "'postAggregations' : " + makePostAggregations(postAggregations)  +
                "}";
    }

    public static String makeSimpleAggregationFilterQuery(String field, List<String> values, Clause.Operation operation,
                                                          Aggregation.Type aggregation, Integer size, Window.Unit emit,
                                                          Integer emitValue, Window.Unit include, Integer includeValue) {
        return makeSimpleAggregationFilterQuery(field, values, operation, aggregation, size, null,
                                                emit, emitValue, include, includeValue);
    }

    public static String makeSimpleAggregationFilterQuery(String field, List<String> values, Clause.Operation operation,
                                                          Aggregation.Type aggregation, Integer size, List<String> postAggregations,
                                                          Window.Unit emit, Integer emitValue, Window.Unit include, Integer includeValue) {
        return "{" +
                "'filters' : [" + makeStringFilter(field, values, operation) + "], " +
                "'aggregation' : " + makeSimpleAggregation(size, aggregation) + ", " +
                "'postAggregations' : " + makePostAggregations(postAggregations)  + ", " +
                "'window' : " + makeWindow(emit, emitValue, include, includeValue) +
                "}";
    }

    public static String makeSimpleAggregationFilterQuery(List<Clause> clauses, Clause.Operation operation,
                                                          Aggregation.Type aggregation, Integer size, Window.Unit emit,
                                                          Integer emitValue, Window.Unit include, Integer includeValue) {
        return makeSimpleAggregationFilterQuery(clauses, operation, aggregation, size, null,
                                                emit, emitValue, include, includeValue);
    }

    public static String makeSimpleAggregationFilterQuery(List<Clause> clauses, Clause.Operation operation,
                                                          Aggregation.Type aggregation, Integer size, List<String> postAggregations,
                                                          Window.Unit emit, Integer emitValue, Window.Unit include, Integer includeValue) {
        return "{" +
                "'filters' : [" + makeFilter(clauses, operation) + "], " +
                "'aggregation' : " + makeSimpleAggregation(size, aggregation) + ", " +
                "'postAggregations' : " + makePostAggregations(postAggregations)  + ", " +
                "'window' : " + makeWindow(emit, emitValue, include, includeValue) +
                "}";
    }

    public static String makeSimpleAggregationFilterQuery(String field, List<String> values, Clause.Operation operation,
                                                          Aggregation.Type aggregation, Integer size) {
        return makeSimpleAggregationFilterQuery(field, values, operation, aggregation, size, null);
    }

    public static String makeSimpleAggregationFilterQuery(String field, List<String> values, Clause.Operation operation,
                                                          Aggregation.Type aggregation, Integer size, List<String> postAggregations) {
        return "{" +
                "'filters' : [" + makeStringFilter(field, values, operation) + "], " +
                "'aggregation' : " + makeSimpleAggregation(size, aggregation) + ", " +
                "'postAggregations' : " + makePostAggregations(postAggregations)  +
                "}";
    }

    public static String makeSimpleAggregationFilterQuery(List<Clause> clauses, Clause.Operation operation,
                                                          Aggregation.Type aggregation, Integer size) {
        return makeSimpleAggregationFilterQuery(clauses, operation, aggregation, size, null);
    }

    public static String makeSimpleAggregationFilterQuery(List<Clause> clauses, Clause.Operation operation,
                                                          Aggregation.Type aggregation, Integer size, List<String> postAggregations) {
        return "{" +
                "'filters' : [" + makeFilter(clauses, operation) + "], " +
                "'aggregation' : " + makeSimpleAggregation(size, aggregation) + ", " +
                "'postAggregations' : " + makePostAggregations(postAggregations)  +
                "}";
    }

    public static String makeFilterQuery(String field, List<String> values, Clause.Operation operation) {
        return "{'filters' : [" + makeStringFilter(field, values, operation) + "]}";
    }

    public static String makeFilterQuery(List<Clause> values, Clause.Operation operation) {
        return "{'filters' : [" + makeFilter(values, operation) + "]}";
    }

    public static String makeFilterQuery(Clause.Operation operation, Clause... values) {
        return "{'filters': [" + makeFilter(operation, values) + "]}";
    }

    public static String makeFieldFilterQuery(String value) {
        return makeFilterQuery("field", Collections.singletonList(value), Clause.Operation.EQUALS);
    }

    @SafeVarargs
    public static String makeProjectionQuery(Pair<String, String>... projections) {
        return "{'projection' : " + makeProjections(projections) + "}";
    }

    public static String makeAggregationQuery(Aggregation.Type operation, Integer size, Window.Unit emit,
                                              Integer emitValue, Window.Unit include, Integer includeValue) {
        return "{" +
                 "'aggregation' : " + makeSimpleAggregation(size, operation) + ", " +
                 "'window' : " + makeWindow(emit, emitValue, include, includeValue) +
               "}";
    }

    public static String makeAggregationQuery(Aggregation.Type operation, Integer size) {
        return "{'aggregation' : " + makeSimpleAggregation(size, operation) + "}";
    }

    @SafeVarargs
    public static String makeAggregationQuery(Aggregation.Type operation, Integer size, Map<String, String> attributes,
                                              Pair<String, String>... fields) {
        return "{'aggregation' : " + makeStringAttributesAggregation(size, operation, attributes, fields) + "}";
    }

    public static String makeAggregationQuery(Aggregation.Type operation, Integer size, Distribution.Type type,
                                              String field, List<Double> points, Double start, Double end,
                                              Double increment, Integer numberOfPoints) {
        return "{'aggregation' : " + makeDistributionAggregation(size, operation, type, field, points, start, end,
                                                                 increment, numberOfPoints) + "}";
    }

    @SafeVarargs
    public static String makeAggregationQuery(Aggregation.Type operation, Integer size, Long threshold,
                                              String newName, Pair<String, String>... fields) {
        return "{'aggregation' : " + makeTopKAggregation(size, operation, threshold, newName, fields) + "}";
    }

    public static String makePostAggregationsQuery(String... postAggregations) {
        return "{'postAggregations': " + makePostAggregations(Arrays.asList(postAggregations)) + "}";
    }

    public static String makeStringFilter(String field, List<String> values, Clause.Operation operation) {
        return "{" +
                 "'field' : " + makeString(field) + ", " +
                 "'operation' : " + makeString(getOperationFor(operation)) + ", " +
                 "'values' : ['" + values.stream().reduce((a, b) -> a + "' , '" + b).orElse("") + "']" +
               "}";
    }

    public static String makeObjectFilter(String field, List<Value> values, Clause.Operation operation) {
        return "{" +
                 "'field' : " + makeString(field) + ", " +
                 "'operation' : " + makeString(getOperationFor(operation)) + ", " +
                 "'values' : [" + values.stream().map(v -> makeValue(v)).reduce((a, b) -> a + " , " + b).orElse("") + "]" +
               "}";
    }

    public static String makeFilter(List<Clause> values, Clause.Operation operation) {
        return "{" +
                 "'operation' : " + makeString(getOperationFor(operation)) + ", " +
                 "'clauses' : [" + values.stream().map(QueryUtils::toString).reduce((a, b) -> a + " , " + b).orElse("") + "]" +
               "}";
    }

    public static String makeFilter(Clause.Operation operation, Clause... values) {
        return makeFilter(values == null ? Collections.emptyList() : Arrays.asList(values), operation);
    }

    @SafeVarargs
    public static String makeProjections(Pair<String, String>... pairs) {
        return "{'fields' : " + makeMap(pairs) + "}";
    }

    public static String makeSimpleAggregation(Integer size, Aggregation.Type operation) {
        return "{'type' : '" + getTypeFor(operation) + "', 'size' : " + size + "}";
    }

    @SafeVarargs
    public static String makeGroupAggregation(Integer size, Aggregation.Type operation, List<GroupOperation> operations,
                                              Pair<String, String>... fields) {
        return "{" +
                 "'type' : '" + getTypeFor(operation) + "', " +
                 "'fields' : " + makeGroupFields(fields) + ", " +
                   "'attributes' : {" +
                      "'operations' : [" +
                          operations.stream().map(QueryUtils::makeGroupOperation).reduce((a, b) -> a + " , " + b).orElse("") +
                      "]" +
                 "}, " +
                 "'size' : " + size +
               "}";
    }

    public static String makeDistributionAggregation(Integer size, Aggregation.Type operation, Distribution.Type type,
                                                     String field, List<Double> points, Double start, Double end,
                                                     Double increment, Integer numberOfPoints) {
        return "{" +
                 "'type' : '" + getTypeFor(operation) + "', " +
                 "'fields' : " + makeGroupFields(Pair.of(field, field)) + ", " +
                 "'attributes' : " + makeMap(type, points, start, end, increment, numberOfPoints) + ", " +
                 "'size' : " + size +
               "}";
    }

    @SafeVarargs
    public static String makeTopKAggregation(Integer size, Aggregation.Type operation, Long threshold,
                                             String newName, Pair<String, String>... fields) {
        return "{" +
                 "'type' : '" + getTypeFor(operation) + "', " +
                 "'fields' : " + makeGroupFields(fields) + ", " +
                 "'attributes' : " + makeMap(threshold, newName) + ", " +
                 "'size' : " + size +
               "}";
    }

    @SafeVarargs
    public static String makeStringAttributesAggregation(Integer size, Aggregation.Type operation,
                                                         Map<String, String> attributes,
                                                         Pair<String, String>... fields) {
        return "{" +
                 "'type' : '" + getTypeFor(operation) + "', " +
                 "'fields' : " + makeGroupFields(fields) + ", " +
                 "'size' : " + size + ", " +
                 "'attributes' : " + makeMap(attributes) +
               "}";
    }

    public static String makePostAggregations(List<String> postAggregations) {
        return postAggregations == null ? "null" : "[" + postAggregations.stream().reduce((a, b) -> a + ", " + b).orElse("") + "]";
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

    public static String makeOrderBy(OrderBy.SortItem... sortItems) {
        return "{" +
                 "'type': 'ORDERBY', " +
                 "'fields': [" + Arrays.stream(sortItems).map(QueryUtils::makeSortItem).reduce((a, b) -> a + " , " + b).orElse("") + "]" +
               "}";
    }

    public static String makeComputation(Expression expression, String newName) {
        return "{" +
                 "'type': 'COMPUTATION', " +
                 "'expression': " + makeExpression(expression) + ", " +
                 "'newName': '" + newName + "'" +
               "}";
    }

    public static String makeString(String field) {
        return field != null ? "'" + field + "'" : "null";

    }

    public static String makeMap(Distribution.Type type, List<Double> points, Double start, Double end, Double increment,
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

    // Again, not implementing toString in Clause to not tie the construction of the JSON to the src.
    public static String toString(Clause clause) {
        StringBuilder builder = new StringBuilder();
        if (clause instanceof StringFilterClause) {
            StringFilterClause filterClause = (StringFilterClause) clause;
            builder.append(makeStringFilter(filterClause.getField(), filterClause.getValues(), filterClause.getOperation()));
        } else if (clause instanceof ObjectFilterClause) {
            ObjectFilterClause filterClause = (ObjectFilterClause) clause;
            builder.append(makeObjectFilter(filterClause.getField(), filterClause.getValues(), filterClause.getOperation()));
        } else if (clause instanceof LogicalClause) {
            LogicalClause logicalClause = (LogicalClause) clause;
            builder.append(makeFilter(logicalClause.getClauses(), logicalClause.getOperation()));
        }
        return builder.toString();
    }

    private static String makeWindow(Window.Unit emit, Integer emitValue, Window.Unit include, Integer includeValue) {
        return "{" +
                 "'emit' : " + makeWindowPart(emit, Window.EMIT_EVERY_FIELD, emitValue) + ", " +
                 "'include' : " + makeWindowPart(include, Window.INCLUDE_FIRST_FIELD, includeValue) +
               "}";
    }

    private static String makeWindowPart(Window.Unit unit, String key, Integer value) {
        String window = "{'type' : '" + getUnitFor(unit) + "'";
        if (unit != Window.Unit.ALL) {
            window += ", '" + key  + "' : " + value;
        }
        return window + "}";
    }

    private static String makeValue(Value value) {
        return "{" +
                 "'kind' : '" + value.getKind().name() + "', " +
                 "'value' : '" + value.getValue() + "', " +
                 "'type' : '" + value.getType() + "' " +
               "}";
    }

    private static String makeSortItem(OrderBy.SortItem sortItem) {
        return "{" +
                 "'field': '" + sortItem.getField() + "', " +
                 "'direction': '" + sortItem.getDirection() + "'" +
               "}";
    }

    private static String makeExpression(Expression expression) {
        if (expression instanceof LeafExpression) {
            LeafExpression leafExpression = (LeafExpression) expression;
            return "{" +
                     "'value': " + makeValue(leafExpression.getValue()) +
                   "}";
        } else {
            BinaryExpression binaryExpression = (BinaryExpression) expression;
            return "{" +
                     "'operation': '" + getExpressionOperationFor(binaryExpression.getOperation())  + "', " +
                     "'left': " + makeExpression(binaryExpression.getLeft()) + ", " +
                     "'right': " + makeExpression(binaryExpression.getRight()) + ", " +
                     "'type': '" + binaryExpression.getType() + "'" +
                   "}";
        }
    }

    private static String getExpressionOperationFor(Expression.Operation operation) {
        try {
            return operation.getClass().getField(operation.name()).getAnnotation(SerializedName.class).value();
        } catch (Exception e) {
            return "";
        }
    }

    public static String getOperationFor(Clause.Operation operation) {
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

    public static String getTypeFor(Aggregation.Type type) {
        switch (type) {
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

    public static String getUnitFor(Window.Unit unit) {
        switch (unit) {
            case RECORD:
                return "RECORD";
            case TIME:
                return "TIME";
            case ALL:
                return "ALL";
            default:
                return "";
        }
    }
    */
}
