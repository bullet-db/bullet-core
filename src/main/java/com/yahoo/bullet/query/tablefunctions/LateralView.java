/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.tablefunctions;

import com.yahoo.bullet.querying.tablefunctors.LateralViewFunctor;
import com.yahoo.bullet.querying.tablefunctors.TableFunctor;
import lombok.Getter;

import java.util.Objects;

@Getter
public class LateralView extends TableFunction {
    private static final long serialVersionUID = -8238108616312386350L;

    private final TableFunction tableFunction;

    public LateralView(TableFunction tableFunction, boolean outer) {
        super(outer, TableFunctionType.LATERAL_VIEW);
        this.tableFunction = Objects.requireNonNull(tableFunction);
    }

    @Override
    public TableFunctor getTableFunctor() {
        return new LateralViewFunctor(this);
    }

    @Override
    public String toString() {
        return "{outer: " + outer + ", type: " + type + ", tableFunction: " + tableFunction + "}";
    }
}
