/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.tablefunctions;

import lombok.Getter;

/**
 * The OuterableTableFunction class adds the outer option. When outer is specified, a table function that generates no
 * records from a Bullet record will generate an empty record instead.
 */
@Getter
public abstract class OuterableTableFunction extends TableFunction {
    private static final long serialVersionUID = 3175147159913255802L;

    protected final boolean outer;

    public OuterableTableFunction(boolean outer, TableFunctionType type) {
        super(type);
        this.outer = outer;
    }
}
