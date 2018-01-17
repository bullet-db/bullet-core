/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.querying.FilterOperations.FilterType;
import com.yahoo.bullet.typesystem.Type;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

public class FilterUtils {
    public static FilterClause getFieldFilter(FilterType operation, String... values) {
        return (FilterClause) makeClause("field", values == null ? null : asList(values), operation);
    }

    public static FilterClause getFieldFilter(String field, FilterType operation, String... values) {
        return (FilterClause) makeClause(field, values == null ? null : asList(values), operation);
    }

    public static Clause makeClause(FilterType operation, Clause... clauses) {
        LogicalClause clause = new LogicalClause();
        clause.setOperation(operation);
        if (clauses != null) {
            clause.setClauses(asList(clauses));
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
}
