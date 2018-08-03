/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.typesystem.Type;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class FilterUtils {
    public static FilterClause getFieldFilter(Clause.Operation operation, Object... values) {
        return getFieldFilter("field", operation, values);
    }

    public static FilterClause getFieldFilter(String field, Clause.Operation operation, Object... values) {
        List<Object> clauses = values == null ? null : asList(values).stream()
                                                                     .map(s -> s instanceof String ? new FilterClause.Value(FilterClause.Value.Type.VALUE, (String) s) : s)
                                                                     .collect(Collectors.toList());
        return (FilterClause) makeClause(field, clauses, operation);
    }

    public static Clause makeClause(Clause.Operation operation, Clause... clauses) {
        LogicalClause clause = new LogicalClause();
        clause.setOperation(operation);
        if (clauses != null) {
            clause.setClauses(asList(clauses));
        }
        clause.configure(new BulletConfig());
        clause.initialize();
        return clause;
    }

    public static Clause makeClause(String field, List<Object> values, Clause.Operation operation) {
        FilterClause clause = new FilterClause();
        clause.setField(field);
        clause.setValues(values == null ? Collections.singletonList(new FilterClause.Value(FilterClause.Value.Type.VALUE, Type.NULL_EXPRESSION)) : values);
        clause.setOperation(operation);
        clause.configure(new BulletConfig());
        clause.initialize();
        return clause;
    }
}
