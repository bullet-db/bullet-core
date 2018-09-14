/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.yahoo.bullet.common.BulletConfig;

public class Parser {
    private static final FieldTypeAdapterFactory<Clause> CLAUSE_FACTORY =
            FieldTypeAdapterFactory.of(Clause.class)
                                   .registerSubType(ObjectFilterClause.class, Parser::isObjectFilterClause)
                                   .registerSubType(StringFilterClause.class, Parser::isStringFilterClause)
                                   .registerSubType(LogicalClause.class, Parser::isLogicalClause);
    private static final FieldTypeAdapterFactory<PostAggregation> POST_AGGREGATION_FACTORY =
            FieldTypeAdapterFactory.of(PostAggregation.class)
                                   .registerSubType(OrderBy.class, Parser::isOrderBy)
                                   .registerSubType(Computation.class, Parser::isComputation);
    private static final FieldTypeAdapterFactory<Expression> EXPRESSION_FACTORY =
            FieldTypeAdapterFactory.of(Expression.class)
                                   .registerSubType(LeafExpression.class, Parser::isLeafExpression)
                                   .registerSubType(CastExpression.class, Parser::isCastExpression)
                                   .registerSubType(BinaryExpression.class, Parser::isBinaryExpression);
    private static final Gson GSON = new GsonBuilder().registerTypeAdapterFactory(CLAUSE_FACTORY)
                                                      .registerTypeAdapterFactory(POST_AGGREGATION_FACTORY)
                                                      .registerTypeAdapterFactory(EXPRESSION_FACTORY)
                                                      .excludeFieldsWithoutExposeAnnotation()
                                                      .create();

    private static Boolean isFilterClause(JsonObject jsonObject) {
        JsonElement jsonElement = jsonObject.get(Clause.OPERATION_FIELD);
        return jsonElement != null && Clause.Operation.RELATIONALS.contains(jsonElement.getAsString());
    }

    private static Boolean isStringFilterClause(JsonObject jsonObject) {
        if (!isFilterClause(jsonObject)) {
            return false;
        }
        JsonArray values = (JsonArray) jsonObject.get(FilterClause.VALUES_FIELD);
        return values != null && values.size() != 0 && values.get(0).isJsonPrimitive();
    }

    private static Boolean isObjectFilterClause(JsonObject jsonObject) {
        if (!isFilterClause(jsonObject)) {
            return false;
        }
        JsonArray values = (JsonArray) jsonObject.get(FilterClause.VALUES_FIELD);
        return values != null && values.size() != 0 && values.get(0).isJsonObject();
    }

    private static Boolean isLogicalClause(JsonObject jsonObject) {
        JsonElement jsonElement = jsonObject.get(Clause.OPERATION_FIELD);
        return jsonElement != null && Clause.Operation.LOGICALS.contains(jsonElement.getAsString());
    }

    private static Boolean isOrderBy(JsonObject jsonObject) {
        JsonElement jsonElement = jsonObject.get(PostAggregation.TYPE_FIELD);
        return jsonElement != null && jsonElement.getAsString().equals("ORDERBY");
    }

    private static Boolean isComputation(JsonObject jsonObject) {
        JsonElement jsonElement = jsonObject.get(PostAggregation.TYPE_FIELD);
        return jsonElement != null && jsonElement.getAsString().equals("COMPUTATION");
    }

    private static Boolean isBinaryExpression(JsonObject jsonObject) {
        JsonElement jsonElement = jsonObject.get(Expression.OPERATION_FIELD);
        return jsonElement != null && Expression.Operation.BINARY_OPERATION.contains(jsonElement.getAsString());
    }

    private static Boolean isCastExpression(JsonObject jsonObject) {
        JsonElement jsonElement = jsonObject.get(Expression.OPERATION_FIELD);
        return jsonElement != null && jsonElement.getAsString().equals("CAST");
    }

    private static Boolean isLeafExpression(JsonObject jsonObject) {
        return !jsonObject.has(Expression.OPERATION_FIELD);
    }

    /**
     * Parses a Query out of the query string.
     *
     * @param queryString The String version of the query.
     * @param config Additional configuration for the query.
     *
     * @return The parsed, configured Query.
     * @throws com.google.gson.JsonParseException if there was an issue parsing the query.
     */
    public static Query parse(String queryString, BulletConfig config) {
        Query query = GSON.fromJson(queryString, Query.class);
        query.configure(config);
        return query;
    }
}

