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
public class BinaryExpression extends Expression {
    @Expose
    private Expression leftExpression;
    @Expose
    private Expression rightExpression;

    /**
     * Default Constructor. GSON recommended.
     */
    public BinaryExpression() {
        super();
        leftExpression = null;
        rightExpression = null;
    }

    @Override
    public String toString() {
        return "{" + super.toString() + ", " + "leftExpression: " + leftExpression + ", " + "rightExpression: " + rightExpression + "}";
    }
}

