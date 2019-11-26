/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.expressions.BinaryExpression;
import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.parsing.expressions.ListExpression;
import com.yahoo.bullet.parsing.expressions.NAryExpression;
import com.yahoo.bullet.parsing.expressions.UnaryExpression;
import com.yahoo.bullet.parsing.expressions.ValueExpression;

public class Parser {
    private static final FieldTypeAdapterFactory<PostAggregation> POST_AGGREGATION_FACTORY =
            FieldTypeAdapterFactory.of(PostAggregation.class)
                                   .registerSubType(OrderBy.class, Parser::isOrderBy)
                                   .registerSubType(Computation.class, Parser::isComputation);
    private static final FieldTypeAdapterFactory<Expression> EXPRESSION_FACTORY =
            FieldTypeAdapterFactory.of(Expression.class)
                                   .registerSubType(ValueExpression.class, Parser::isValue)
                                   .registerSubType(FieldExpression.class, Parser::isField)
                                   .registerSubType(UnaryExpression.class, Parser::isUnary)
                                   .registerSubType(BinaryExpression.class, Parser::isBinary)
                                   .registerSubType(NAryExpression.class, Parser::isNAry)
                                   .registerSubType(ListExpression.class, Parser::isList);
    private static final Gson GSON = new GsonBuilder().registerTypeAdapterFactory(POST_AGGREGATION_FACTORY)
                                                      .registerTypeAdapterFactory(EXPRESSION_FACTORY)
                                                      .excludeFieldsWithoutExposeAnnotation()
                                                      .create();

    private static Boolean isOrderBy(JsonObject jsonObject) {
        JsonElement jsonElement = jsonObject.get(PostAggregation.TYPE_FIELD);
        return jsonElement != null && jsonElement.getAsString().equals(PostAggregation.ORDER_BY_SERIALIZED_NAME);
    }

    private static Boolean isComputation(JsonObject jsonObject) {
        JsonElement jsonElement = jsonObject.get(PostAggregation.TYPE_FIELD);
        return jsonElement != null && jsonElement.getAsString().equals(PostAggregation.COMPUTATION_SERIALIZED_NAME);
    }

    private static Boolean isValue(JsonObject jsonObject) {
        switch (jsonObject.size()) {
            case 0:
                return true;
            case 1:
                return jsonObject.has("type");
            case 2:
                return jsonObject.has("value") && jsonObject.has("type");
        }
        return false;
    }

    private static Boolean isField(JsonObject jsonObject) {
        switch (jsonObject.size()) {
            case 1:
                return jsonObject.has("field");
            case 2:
                return jsonObject.has("field") && jsonObject.has("type");
        }
        return false;
    }

    private static Boolean isUnary(JsonObject jsonObject) {
        switch (jsonObject.size()) {
            case 2:
                return jsonObject.has("operand") && jsonObject.has("op");
            case 3:
                return jsonObject.has("operand") && jsonObject.has("op") && jsonObject.has("type");
        }
        return false;
    }

    private static Boolean isBinary(JsonObject jsonObject) {
        switch (jsonObject.size()) {
            case 3:
                return jsonObject.has("left") && jsonObject.has("right") && jsonObject.has("op");
            case 4:
                return jsonObject.has("left") && jsonObject.has("right") && jsonObject.has("op") && jsonObject.has("type");
        }
        return false;
    }

    private static Boolean isNAry(JsonObject jsonObject) {
        switch (jsonObject.size()) {
            case 2:
                return jsonObject.has("operands") && jsonObject.has("op");
            case 3:
                return jsonObject.has("operands") && jsonObject.has("op") && jsonObject.has("type");
        }
        return false;
    }

    private static Boolean isList(JsonObject jsonObject) {
        return jsonObject.size() == 2 && jsonObject.has("values") && jsonObject.has("type");
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

