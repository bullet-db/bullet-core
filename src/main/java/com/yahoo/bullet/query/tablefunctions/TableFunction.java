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

/**
 * Table functions are used in Bullet queries to generate virtual records from incoming Bullet records. The generated
 * records are then fed to the rest of the query (filter, projection, aggregation, etc.)
 *
 * Table functions have a lateral view and an outer option. When lateral view is specified, the generated record(s) is
 * joined with the original. When outer is specified, a table function that generates no records from a Bullet record
 * will generate an empty record instead. This can be useful when combined with lateral view since the query will then
 * still see the original record.
 *
 * Currently, the only supported table function type is Explode.
 *
 * Look at {@link TableFunctor} to see how table functions are applied in the {@link com.yahoo.bullet.querying.Querier}.
 */
@Getter @AllArgsConstructor
public abstract class TableFunction implements Serializable {
    private static final long serialVersionUID = 4126801547249854808L;

    protected final boolean lateralView;
    protected final boolean outer;
    protected final TableFunctionType type;

    /**
     * Gets a new instance of a table functor for this table function.
     *
     * @return A newly-constructed table functor for this table function.
     */
    public abstract TableFunctor getTableFunctor();
}
