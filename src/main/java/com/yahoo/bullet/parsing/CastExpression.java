/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class CastExpression extends Expression {
    public enum CastType {
        @SerializedName("INTEGER")
        INTEGER,
        @SerializedName("LONG")
        LONG,
        @SerializedName("FLOAT")
        FLOAT,
        @SerializedName("DOUBLE")
        DOUBLE,
        @SerializedName("BOOLEAN")
        BOOLEAN,
        @SerializedName("STRING")
        STRING
    }

    @Expose
    private Expression expression;
    @Expose
    private CastType type;

    /**
     * Default Constructor. GSON recommended.
     */
    public CastExpression() {
        super();
        expression = null;
        type = null;
    }

    @Override
    public String toString() {
        return "{" + super.toString() + ", expression" + ": " + expression + ", " + "type: " + type + "}";
    }
}
