/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.tablefunctions;

import com.yahoo.bullet.querying.tablefunctors.TableFunctor;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

@Getter @AllArgsConstructor
public abstract class TableFunction implements Serializable {
    private static final long serialVersionUID = 4126801547249854808L;

    // If true, joins input-output.
    protected final boolean lateralView;
    // If true, the function returns null if the input is empty or null. If false, the function returns nothing.
    protected final boolean outer;
    protected final TableFunctionType type;

    /**
     * Gets a new instance of a functor for this table function.
     *
     * @return A newly-constructed functor for this table function.
     */
    public abstract TableFunctor getTableFunctor();

    @Override
    public String toString() {
        return "{lateralView: " + lateralView + ", outer: " + outer + ", type: " + type + "}";
    }
}
