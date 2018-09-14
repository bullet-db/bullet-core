/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.common.Initializable;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

import static java.util.Arrays.asList;

@Getter @Setter
public abstract class Expression implements Initializable {
    /** The type of the operation in the expression. */
    public enum Operation {
        @SerializedName("+")
        ADD,
        @SerializedName("-")
        SUB,
        @SerializedName("*")
        MUL,
        @SerializedName("/")
        DIV;

        public static final List<String> BINARY_OPERATION = asList("+", "-", "*", "/");
    }

    @Expose @SerializedName(OPERATION_FIELD)
    protected Operation operation;

    public static final String OPERATION_FIELD = "operation";

    @Override
    public String toString() {
        return OPERATION_FIELD + ": " + operation;
    }
}
