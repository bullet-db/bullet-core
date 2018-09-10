/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class LeafExpression extends Expression {
    @Expose
    private Value value;

    /**
     * Default Constructor. GSON recommended.
     */
    public LeafExpression() {
        super();
        value = null;
    }

    @Override
    public String toString() {
        return "{" + super.toString() + ", " + "value: " + value + "}";
    }
}
