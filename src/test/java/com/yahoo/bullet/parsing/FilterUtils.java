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

import static java.util.Arrays.asList;

public class FilterUtils {
    public static FilterClause getFieldFilter(Clause.Operation operation, String... values) {
        return getFieldFilter("field", operation, values);
    }

    public static FilterClause getFieldFilter(String field, Clause.Operation operation, String... values) {
        return (FilterClause) makeClause(field, values == null ? null : asList(values), operation);
    }

    public static FilterClause getObjectFieldFilter(Clause.Operation operation, Value... values) {
        return getObjectFieldFilter("field", operation, values);
    }

    public static FilterClause getObjectFieldFilter(String field, Clause.Operation operation, Value... values) {
        return (FilterClause) makeObjectClause(field, values == null ? null : asList(values), operation);
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

    public static Clause makeClause(String field, List<String> values, Clause.Operation operation) {
        FilterClause clause = new StringFilterClause();
        clause.setField(field);
        clause.setValues(values == null ? Collections.singletonList(Type.NULL_EXPRESSION) : values);
        clause.setOperation(operation);
        clause.configure(new BulletConfig());
        clause.initialize();
        return clause;
    }

    public static Clause makeObjectClause(String field, List<Value> values, Clause.Operation operation) {
        FilterClause clause = new ObjectFilterClause();
        clause.setField(field);
        clause.setValues(values == null ? Collections.singletonList(new Value(Value.Kind.VALUE, Type.NULL_EXPRESSION)) : values);
        clause.setOperation(operation);
        clause.configure(new BulletConfig());
        clause.initialize();
        return clause;
    }
}
