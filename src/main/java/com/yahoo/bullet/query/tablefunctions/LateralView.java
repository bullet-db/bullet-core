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

/**
 * A table function that requires another table function.
 */
@Getter
public class LateralView extends TableFunction {
    private static final long serialVersionUID = -8238108616312386350L;

    private final TableFunction tableFunction;

    /**
     * Constructor that creates a LATERAL VIEW table function.
     *
     * @param tableFunction The non-null table function to take a lateral view of.
     */
    public LateralView(TableFunction tableFunction) {
        super(TableFunctionType.LATERAL_VIEW);
        this.tableFunction = Objects.requireNonNull(tableFunction);
    }

    @Override
    public TableFunctor getTableFunctor() {
        return new LateralViewFunctor(this);
    }

    @Override
    public String toString() {
        return "{type: " + type + ", tableFunction: " + tableFunction + "}";
    }
}
