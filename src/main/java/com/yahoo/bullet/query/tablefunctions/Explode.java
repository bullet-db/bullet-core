/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.tablefunctions;

import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.querying.tablefunctors.ExplodeFunctor;
import com.yahoo.bullet.querying.tablefunctors.TableFunctor;
import lombok.Getter;

import java.util.Objects;

/**
 * A table function that requires either a list operand and a key alias or a map operand and both key and value aliases.
 */
@Getter
public class Explode extends OuterableTableFunction {
    private static final long serialVersionUID = 6058738006416405818L;

    private final Expression field;
    private final String keyAlias;
    private final String valueAlias;

    /**
     * Constructor that creates an EXPLODE table function.
     *
     * @param field The non-null field to explode.
     * @param keyAlias The non-null alias for the key column of the exploded field.
     * @param valueAlias The alias for the value column of the exploded field.
     * @param outer The outer option.
     */
    public Explode(Expression field, String keyAlias, String valueAlias, boolean outer) {
        super(outer, TableFunctionType.EXPLODE);
        this.field = Objects.requireNonNull(field);
        this.keyAlias = Objects.requireNonNull(keyAlias);
        this.valueAlias = valueAlias;
    }

    @Override
    public TableFunctor getTableFunctor() {
        return new ExplodeFunctor(this);
    }

    @Override
    public String toString() {
        return "{outer: " + outer + ", type: " + type + ", field: " + field + ", keyAlias: " + keyAlias + ", valueAlias: " + valueAlias + "}";
    }
}
