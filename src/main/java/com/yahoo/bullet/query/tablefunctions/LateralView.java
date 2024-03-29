/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.tablefunctions;

import com.yahoo.bullet.querying.tablefunctors.LateralViewFunctor;
import com.yahoo.bullet.querying.tablefunctors.TableFunctor;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A table function that requires another table function.
 */
@Getter
public class LateralView extends TableFunction {
    private static final long serialVersionUID = -8238108616312386350L;

    private final List<TableFunction> tableFunctions;

    /**
     * Constructor that creates a LATERAL VIEW from a {@link List} of {@link TableFunction}.
     *
     * @param tableFunctions The non-null list of table functions to chain lateral views.
     */
    public LateralView(List<TableFunction> tableFunctions) {
        super(TableFunctionType.LATERAL_VIEW);
        this.tableFunctions = Objects.requireNonNull(tableFunctions);
    }

    /**
     * Constructor that creates a LATERAL VIEW from a {@link TableFunction}.
     *
     * @param tableFunction The non-null table function to apply a lateral view.
     */
    public LateralView(TableFunction tableFunction) {
        this(Collections.singletonList(Objects.requireNonNull(tableFunction)));
    }

    @Override
    public TableFunctor getTableFunctor() {
        return new LateralViewFunctor(this);
    }

    @Override
    public String toString() {
        return "{type: " + type + ", tableFunctions: " + tableFunctions + "}";
    }
}
