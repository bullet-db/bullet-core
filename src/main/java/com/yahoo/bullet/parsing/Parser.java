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
import com.yahoo.bullet.parsing.expressions.BinaryExpression;
import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.parsing.expressions.ListExpression;
import com.yahoo.bullet.parsing.expressions.NullExpression;
import com.yahoo.bullet.parsing.expressions.UnaryExpression;
import com.yahoo.bullet.parsing.expressions.ValueExpression;

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
                                   .registerSubType(NullExpression.class, Parser::isLazyNull)
                                   .registerSubType(ValueExpression.class, Parser::isLazyValue)
                                   .registerSubType(FieldExpression.class, Parser::isLazyField)
                                   .registerSubType(UnaryExpression.class, Parser::isLazyUnary)
                                   .registerSubType(BinaryExpression.class, Parser::isLazyBinary)
                                   .registerSubType(ListExpression.class, Parser::isLazyList);
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
        return jsonElement != null && jsonElement.getAsString().equals(PostAggregation.ORDER_BY_SERIALIZED_NAME);
    }

    private static Boolean isComputation(JsonObject jsonObject) {
        JsonElement jsonElement = jsonObject.get(PostAggregation.TYPE_FIELD);
        return jsonElement != null && jsonElement.getAsString().equals(PostAggregation.COMPUTATION_SERIALIZED_NAME);
    }

    private static Boolean isLazyNull(JsonObject jsonObject) {
        return jsonObject.size() == 0;
    }

    private static Boolean isLazyValue(JsonObject jsonObject) {
        return jsonObject.size() == 2 &&
               jsonObject.has("value") &&
               jsonObject.has("type");
    }

    private static Boolean isLazyField(JsonObject jsonObject) {
        return (jsonObject.size() == 1 || (jsonObject.size() == 2 && jsonObject.has("type"))) &&
                jsonObject.has("field");
    }

    private static Boolean isLazyUnary(JsonObject jsonObject) {
        return (jsonObject.size() == 2 || (jsonObject.size() == 3 && jsonObject.has("type"))) &&
                jsonObject.has("operand") &&
                jsonObject.has("op");
    }

    private static Boolean isLazyBinary(JsonObject jsonObject) {
        return (jsonObject.size() == 3 || (jsonObject.size() == 4 && jsonObject.has("type"))) &&
                jsonObject.has("left") &&
                jsonObject.has("right") &&
                jsonObject.has("op");
    }

    private static Boolean isLazyList(JsonObject jsonObject) {
        return jsonObject.size() == 2 &&
               jsonObject.has("values") &&
               jsonObject.has("type");
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

