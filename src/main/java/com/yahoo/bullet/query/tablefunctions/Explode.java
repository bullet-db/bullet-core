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

@Getter
public class Explode extends TableFunction {
    private static final long serialVersionUID = 6058738006416405818L;

    private final Expression field;
    private final String keyAlias;
    private final String valueAlias;

    public Explode(Expression field, String keyAlias, String valueAlias, boolean outer) {
        super(outer, TableFunctionType.EXPLODE);
        this.field = Objects.requireNonNull(field);
        this.keyAlias = Objects.requireNonNull(keyAlias);
        this.valueAlias = valueAlias;
    }

    public Explode(Expression field, String keyAlias, boolean outer) {
        this(field, keyAlias, null, outer);
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
