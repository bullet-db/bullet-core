/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter @AllArgsConstructor
public class Value {
    public enum Kind {
        @SerializedName("VALUE")
        VALUE,
        @SerializedName("FIELD")
        FIELD,
        @SerializedName("CAST")
        CAST
    }
    @Expose
    private Kind kind;
    @Expose
    private String value;

    @Override
    public String toString() {
        return "{kind: " + kind + ", " + "value: " + value + "}";
    }
}
