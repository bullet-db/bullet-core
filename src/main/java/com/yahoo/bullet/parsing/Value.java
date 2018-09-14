/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.typesystem.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter @AllArgsConstructor
public class Value {
    public enum Kind {
        @SerializedName("VALUE")
        VALUE,
        @SerializedName("FIELD")
        FIELD
    }
    @Expose
    private Kind kind;
    @Expose
    private String value;
    // The type field is only used when Kind is VALUE.
    @Expose
    private Type type;

    /**
     * Constructor takes a {@link Kind} and a value.
     *
     * @param kind The {@link Kind} of the Object.
     * @param value The value string of the Object.
     */
    public Value(Kind kind, String value) {
        this(kind, value, null);
    }

    @Override
    public String toString() {
        return "{kind: " + kind + ", value: " + value + ", type: " + type + "}";
    }
}
