/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yahoo.bullet.operations.FilterOperations.FilterType;

import java.util.Map;

public class Parser {
    private static final FieldTypeAdapterFactory<Clause> CLAUSE_FACTORY =
            FieldTypeAdapterFactory.of(Clause.class, t -> t.getAsJsonObject().get(Clause.OPERATION_FIELD).getAsString())
                                   .registerSubType(FilterClause.class, FilterType.RELATIONALS)
                                   .registerSubType(LogicalClause.class, FilterType.LOGICALS);
    private static final Gson GSON = new GsonBuilder()
                                         .registerTypeAdapterFactory(CLAUSE_FACTORY)
                                         .excludeFieldsWithoutExposeAnnotation()
                                         .create();

    /**
     * Parses a Specification out of the query string.
     *
     * @param queryString The String version of the query.
     * @param configuration Additional configuration for the specification.
     *
     * @return The parsed, configured Specification.
     */
    public static Specification parse(String queryString, Map configuration) {
        Specification specification = GSON.fromJson(queryString, Specification.class);
        specification.configure(configuration);
        return specification;
    }

}

