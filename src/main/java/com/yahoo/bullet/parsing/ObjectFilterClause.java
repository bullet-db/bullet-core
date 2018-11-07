/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.typesystem.Type;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ObjectFilterClause extends FilterClause<Value> {
    /**
     * Default Constructor. GSON recommended.
     */
    public ObjectFilterClause() {
        super();
    }

    /**
     *  Constructor takes a {@link StringFilterClause} object to construct from.
     *
     * @param stringFilterClause The {@link StringFilterClause} object tor construct from.
     */
    public ObjectFilterClause(StringFilterClause stringFilterClause) {
        this.operation = stringFilterClause.operation;
        this.field = stringFilterClause.field;
        this.patterns = stringFilterClause.patterns;
        List<String> stringValues = stringFilterClause.getValues();
        if (stringValues != null) {
            values = stringValues.stream().map(s -> s == null ? null : new Value(Value.Kind.VALUE, s)).collect(Collectors.toList());
        }
    }

    @Override
    public String getValue(Value value) {
        return value.getValue();
    }

    @Override
    public boolean hasNull(Value value) {
        return (value.getType() == null || value.getType() == Type.NULL) && Type.isNullExpression(getValue(value));
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        Optional<List<BulletError>> errors = super.initialize();
        if (errors.isPresent()) {
            return errors;
        }

        if (values != null) {
            for (Value value : values) {
                errors = value.initialize();
                if (errors.isPresent()) {
                    return errors;
                }
            }
        }

        return Optional.empty();
    }
}
